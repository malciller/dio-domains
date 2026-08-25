(** WebSocket L1/L2 market data feed for Alpaca. Serves ring buffer quote snapshots. *)

open Lwt.Infix
open Dio_exchange.Exchange_intf.Types

let section = "alpaca_orderbook"

(** The top-of-book comes primarily from the WebSocket quote stream:
    WS "q" messages on the session-appropriate feed (regular v2 by day,
    v1beta1/overnight at night). When the active WS feed has no quotes (e.g.
    during pre-market/after-hours on the free IEX feed when IEX is closed),
    [get_best_bid_ask] falls back to the account position mark from
    [Alpaca_balances.get_position_price]. Trade prints are recorded for
    analytics only and never fabricate quotes. *)

type quote =
  { bid_price : float
  ; bid_size : float
  ; ask_price : float
  ; ask_size : float
  ; timestamp : float
  }

type trade =
  { price : float
  ; size : float
  ; timestamp : float
  ; side : string
  }

module SymbolStore = struct
  (** Single-writer atomic TOB cache : the WS writer publishes a fresh
      immutable record on every push, and readers do a single [Atomic.get]
      with no mutex. Position and best-bid/ask travel together so a reader
      can never observe a torn (mixed-generation) snapshot. *)
  type tob_cache =
    { pos : int
    ; bid_px : float
    ; bid_sz : float
    ; ask_px : float
    ; ask_sz : float
    ; valid : bool
    }

  type t =
    { symbol : string
    ; buffer : quote array
    ; trades_buffer : trade array
    ; capacity : int
    ; trades_capacity : int
    ; mutable write_pos : int
    ; mutable trades_write_pos : int
    ; tob : tob_cache Atomic.t
      (** Serializes full ring-buffer reads only (read_events / iter_events /
          get_recent_trades). The hot-path TOB/position reads are lock-free
          via [tob]. *)
    ; mutex : Mutex.t
    }

  let create symbol capacity =
    { symbol
    ; buffer =
        Array.make
          capacity
          { bid_price = 0.0
          ; bid_size = 0.0
          ; ask_price = 0.0
          ; ask_size = 0.0
          ; timestamp = 0.0
          }
    ; trades_buffer =
        Array.make 100 { price = 0.0; size = 0.0; timestamp = 0.0; side = "trade" }
    ; capacity
    ; trades_capacity = 100
    ; write_pos = 0
    ; trades_write_pos = 0
    ; tob =
        Atomic.make
          { pos = 0
          ; bid_px = 0.0
          ; bid_sz = 0.0
          ; ask_px = 0.0
          ; ask_sz = 0.0
          ; valid = false
          }
    ; mutex = Mutex.create ()
    }
  ;;

  let push t (q : quote) =
    let tob_cache =
      Mutex.lock t.mutex;
      let idx = t.write_pos mod t.capacity in
      t.buffer.(idx) <- q;
      t.write_pos <- t.write_pos + 1;
      let cache =
        { pos = t.write_pos
        ; bid_px = q.bid_price
        ; bid_sz = q.bid_size
        ; ask_px = q.ask_price
        ; ask_sz = q.ask_size
        ; valid = q.bid_price > 0.0 || q.ask_price > 0.0
        }
      in
      Mutex.unlock t.mutex;
      cache
    in
    Atomic.set t.tob tob_cache;
    (* wake only the domain trading this symbol. signal_all here woke
       every configured asset per tick (O(N) futex wakes + N wasted cycles
       on every quote). *)
    Concurrency.Exchange_wakeup.signal ~symbol:t.symbol
  ;;

  let push_trade t (tr : trade) =
    Mutex.lock t.mutex;
    let idx = t.trades_write_pos mod t.trades_capacity in
    t.trades_buffer.(idx) <- tr;
    t.trades_write_pos <- t.trades_write_pos + 1;
    Mutex.unlock t.mutex;
    Concurrency.Exchange_wakeup.signal ~symbol:t.symbol
  ;;

  let get_recent_trades t count =
    Mutex.lock t.mutex;
    let current_pos = t.trades_write_pos in
    let start_idx = max 0 (current_pos - count) in
    let trades = ref [] in
    for i = current_pos - 1 downto start_idx do
      let idx = i mod t.trades_capacity in
      trades := t.trades_buffer.(idx) :: !trades
    done;
    Mutex.unlock t.mutex;
    !trades
  ;;

  let get_best_bid_ask t =
    let c = Atomic.get t.tob in
    if c.valid
    then Some (c.bid_px, c.bid_sz, c.ask_px, c.ask_sz)
    else (
      match Alpaca_balances.get_position_price t.symbol with
      | Some p when p > 0.0 -> Some (p, 0.0, p, 0.0)
      | _ -> None)
  ;;

  let get_current_position t = (Atomic.get t.tob).pos

  let read_events t start_pos =
    Mutex.lock t.mutex;
    let current_pos = t.write_pos in
    let start_idx = max start_pos (current_pos - t.capacity) in
    let events = ref [] in
    for i = current_pos - 1 downto start_idx do
      let idx = i mod t.capacity in
      let q = t.buffer.(idx) in
      let ob_event =
        { bids = [| q.bid_price, q.bid_size |]
        ; asks = [| q.ask_price, q.ask_size |]
        ; timestamp = q.timestamp
        }
      in
      events := ob_event :: !events
    done;
    Mutex.unlock t.mutex;
    !events
  ;;

  let iter_events t start_pos f =
    Mutex.lock t.mutex;
    let current_pos = t.write_pos in
    let start_idx = max start_pos (current_pos - t.capacity) in
    for i = start_idx to current_pos - 1 do
      let idx = i mod t.capacity in
      let q = t.buffer.(idx) in
      let ob_event =
        { bids = [| q.bid_price, q.bid_size |]
        ; asks = [| q.ask_price, q.ask_size |]
        ; timestamp = q.timestamp
        }
      in
      f ob_event
    done;
    Mutex.unlock t.mutex;
    current_pos
  ;;
end

let stores : (string, SymbolStore.t) Hashtbl.t = Hashtbl.create 16
let stores_mutex = Mutex.create ()

let get_or_create_store symbol =
  Mutex.lock stores_mutex;
  let store =
    match Hashtbl.find_opt stores symbol with
    | Some s -> s
    | None ->
      let s = SymbolStore.create symbol 1024 in
      Hashtbl.replace stores symbol s;
      s
  in
  Mutex.unlock stores_mutex;
  store
;;

let active_subscriptions : string list ref = ref []
let active_conn : Websocket_lwt_unix.conn option ref = ref None

(* Ping/pong liveness tracking.
   The supervisor monitor loop calls [send_ping] on a 15s cadence and expects
   a [bool]; a Pong frame arriving in the read loop broadcasts [pong_condition]
   and stamps [last_pong_time] so the waiter can resolve. [last_pong_time] also
   closes the race where the Pong lands before [send_ping] starts waiting. *)
let last_pong_time = ref 0.0
let pong_condition = Lwt_condition.create ()

(** Timestamp of the last data frame, for the ws_feed inter-message gap
    measurement (recorded under the "alpaca" venue in [Network_latency]). *)
let last_frame_time = ref 0.0

let get_best_bid_ask symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> SymbolStore.get_best_bid_ask store
  | None ->
    (match Alpaca_balances.get_position_price symbol with
     | Some p when p > 0.0 -> Some (p, 0.0, p, 0.0)
     | _ -> None)
;;

let get_best_bid_ask_fast symbol =
  let store = get_or_create_store symbol in
  fun () -> SymbolStore.get_best_bid_ask store
;;

let get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> SymbolStore.get_current_position store
  | None -> 0
;;

let get_current_position_fast symbol =
  let store = get_or_create_store symbol in
  fun () -> SymbolStore.get_current_position store
;;

let read_orderbook_events symbol start_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> SymbolStore.read_events store start_pos
  | None -> []
;;

let get_recent_trades symbol count =
  match Hashtbl.find_opt stores symbol with
  | Some store -> SymbolStore.get_recent_trades store count
  | None -> []
;;

let iter_orderbook_events symbol start_pos f =
  match Hashtbl.find_opt stores symbol with
  | Some store -> SymbolStore.iter_events store start_pos f
  | None -> start_pos
;;

let json_to_float = function
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

let parse_timestamp _str = Unix.gettimeofday ()

let handle_message_str ?on_auth_success ?on_auth_error content =
  let trimmed = String.trim content in
  if trimmed <> ""
  then (
    try
      let json = Yojson.Safe.from_string trimmed in
      let items =
        match json with
        | `List l -> l
        | _ -> [ json ]
      in
      List.iter
        (fun j ->
           let open Yojson.Safe.Util in
           let msg_type =
             j |> member "T" |> to_string_option |> Option.value ~default:""
           in
           match msg_type with
           | "q" ->
             let symbol =
               j |> member "S" |> to_string_option |> Option.value ~default:""
             in
             let bp = j |> member "bp" |> json_to_float in
             let bs = j |> member "bs" |> json_to_float in
             let ap = j |> member "ap" |> json_to_float in
             let as_val = j |> member "as" |> json_to_float in
             let ts_str =
               j |> member "t" |> to_string_option |> Option.value ~default:""
             in
             let ts = parse_timestamp ts_str in
             if symbol <> ""
             then (
               let store = get_or_create_store symbol in
               let final_bp, final_bs, final_ap, final_as =
                 match SymbolStore.get_best_bid_ask store with
                 | Some (prev_bp, prev_bs, prev_ap, prev_as) ->
                   let b_price = if bp > 0.0 then bp else prev_bp in
                   let b_sz = if bp > 0.0 then bs else prev_bs in
                   let a_price = if ap > 0.0 then ap else prev_ap in
                   let a_sz = if ap > 0.0 then as_val else prev_as in
                   b_price, b_sz, a_price, a_sz
                 | None -> bp, bs, ap, as_val
               in
               if final_bp > 0.0 || final_ap > 0.0
               then (
                 SymbolStore.push
                   store
                   { bid_price = final_bp
                   ; bid_size = final_bs
                   ; ask_price = final_ap
                   ; ask_size = final_as
                   ; timestamp = ts
                   };
                 Logging.debug_f
                   ~section
                   "[%s] Quote update: bid %.2f (sz %.2f), ask %.2f (sz %.2f)"
                   symbol
                   final_bp
                   final_bs
                   final_ap
                   final_as))
           | "t" ->
             let symbol =
               j |> member "S" |> to_string_option |> Option.value ~default:""
             in
             let price = j |> member "p" |> json_to_float in
             let size = j |> member "s" |> json_to_float in
             let ts_str =
               j |> member "t" |> to_string_option |> Option.value ~default:""
             in
             let ts = parse_timestamp ts_str in
             if symbol <> "" && price > 0.0
             then (
               let store = get_or_create_store symbol in
               let side_str =
                 match SymbolStore.get_best_bid_ask store with
                 | Some (bp, _, ap, _) ->
                   if ap > 0.0 && price >= ap
                   then "buy"
                   else if bp > 0.0 && price <= bp
                   then "sell"
                   else "trade"
                 | None -> "trade"
               in
               (* Trades are recorded for analytics ONLY. A print is evidence
                    of price, never a two-sided quote: it must never fabricate
                    a bid/ask (the old stale-quote fallback published
                    bid = ask = last trade, which showed raw print volatility
                    that is not a real market). The top-of-book comes from the
                    WS quote stream only, so a quote gap simply holds the last
                    real quote until the feed resumes. *)
               SymbolStore.push_trade
                 store
                 { price; size; timestamp = ts; side = side_str };
               Logging.debug_f
                 ~section
                 "[%s] Live trade update: price %.2f (sz %.2f)"
                 symbol
                 price
                 size)
           | "b" ->
             let symbol =
               j |> member "S" |> to_string_option |> Option.value ~default:""
             in
             let close_p = j |> member "c" |> json_to_float in
             Logging.debug_f ~section "[%s] Bar close: %.2f" symbol close_p
           | "heartbeat" ->
             Logging.debug ~section "Alpaca Market Data WS heartbeat"
           | "success" ->
             let msg =
               j |> member "msg" |> to_string_option |> Option.value ~default:""
             in
             Logging.info_f ~section "Alpaca Market Data WS status: %s" msg;
             if msg = "authenticated"
             then (
               match on_auth_success with
               | Some f -> f ()
               | None -> ())
           | "subscription" ->
             let quotes =
               match j |> member "quotes" with
               | `List l -> List.filter_map to_string_option l
               | _ -> []
             in
             let trades =
               match j |> member "trades" with
               | `List l -> List.filter_map to_string_option l
               | _ -> []
             in
             Logging.debug_f
               ~section
               "Alpaca Market Data WS subscription confirmed - quotes: [%s], trades: [%s]"
               (String.concat ", " quotes)
               (String.concat ", " trades)
           | "error" ->
             let code = j |> member "code" |> to_int_option |> Option.value ~default:0 in
             let msg =
               j |> member "msg" |> to_string_option |> Option.value ~default:""
             in
             Logging.error_f ~section "Alpaca Market Data WS error (%d): %s" code msg;
             (match on_auth_error with
              | Some f -> f code msg
              | None -> ())
           | other ->
             Logging.debug_f
               ~section
               "Alpaca Market Data WS received msg (T=%s): %s"
               other
               (Yojson.Safe.to_string j))
        items
    with
    | exn ->
      Logging.error_f
        ~section
        "Failed to parse WS data frame: %s (content: %s)"
        (Printexc.to_string exn)
        content)
;;

let send_subscription symbols =
  match !active_conn with
  | Some conn ->
    let symbols = List.sort_uniq String.compare symbols in
    let json =
      `Assoc
        [ "action", `String "subscribe"
        ; "quotes", `List (List.map (fun s -> `String s) symbols)
        ; "trades", `List (List.map (fun s -> `String s) symbols)
        ]
    in
    let payload = Yojson.Safe.to_string json in
    Logging.debug_f
      ~section
      "Sending Alpaca Market Data WS subscription for symbols: %s"
      (String.concat ", " symbols);
    let frame = Websocket.Frame.create ~content:payload () in
    Websocket_lwt_unix.write conn frame
  | None -> Lwt.return_unit
;;

let subscribe_symbols symbols =
  (* WS-only data feed: subscribing triggers the stream to send the current
     quote per symbol, then continuous "q" updates (same as every other
     exchange here - Kraken/HL/Lighter stream the book, Alpaca streams L1
     quotes). No REST seed or snapshot poll. *)
  let new_syms = List.filter (fun s -> not (List.mem s !active_subscriptions)) symbols in
  if new_syms <> []
  then (
    active_subscriptions := !active_subscriptions @ new_syms;
    send_subscription new_syms)
  else Lwt.return_unit
;;

(** Session-appropriate market-data feed URL: the derived Alpaca overnight
    feed during overnight hours (8:00 PM - 4:00 AM ET, when the regular v2
    stream carries nothing), the configured iex/sip stream otherwise. *)
let session_data_ws_url () =
  if Alpaca_market_hours.is_overnight_hours ()
  then Alpaca_types.Config.overnight_ws_url ()
  else Alpaca_types.Config.data_ws_url ()
;;

(** Monotone incarnation counter for data connections: each [connect_and_monitor]
    invocation captures the current value and bumps it, so a session-watcher
    spawned by a superseded incarnation stops itself instead of acting on a
    connection that is no longer current. *)
let conn_generation = ref 0

(** How often the data connection re-evaluates which session feed it should
    be on (5s: keeps the pre-market/after-hours <-> overnight switch within a
    few seconds of the boundary). During the switch the store simply holds
    the last real quote until the other feed streams fresh "q" messages. *)
let session_watch_seconds = 5.0

let rec connect_and_monitor ~on_failure ~on_connected ~on_heartbeat =
  let url_str = session_data_ws_url () in
  let is_overnight_feed = Alpaca_market_hours.is_overnight_hours () in
  let my_gen = !conn_generation + 1 in
  conn_generation := my_gen;
  let uri = Uri.of_string url_str in
  let host = Uri.host uri |> Option.value ~default:"stream.data.alpaca.markets" in
  let port = Uri.port uri |> Option.value ~default:443 in
  (* Set by the session watcher when the required feed changes; the catch
     handler then reconnects internally instead of reporting a failure. *)
  let session_switch = ref false in
  Lwt.catch
    (fun () ->
       Lwt_unix.getaddrinfo host (string_of_int port) [ Unix.AI_FAMILY Unix.PF_INET ]
       >>= fun addresses ->
       let ip =
         match addresses with
         | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ ->
           Ipaddr_unix.of_inet_addr addr
         | _ -> failwith ("Failed to resolve host " ^ host)
       in
       let client = `TLS (`Hostname host, `IP ip, `Port port) in
       let ctx = Lazy.force Conduit_lwt_unix.default_ctx in
       Websocket_lwt_unix.connect ~ctx client uri
       >>= fun conn ->
       active_conn := Some conn;
       Logging.info_f
         ~section
         "Connected to Alpaca Market Data WS at %s (%s feed)"
         url_str
         (if is_overnight_feed then "overnight" else "regular");
       (* Send the WebSocket authentication request. *)
       let auth_msg =
         `Assoc
           [ "action", `String "auth"
           ; "key", `String (Alpaca_types.Config.api_key ())
           ; "secret", `String (Alpaca_types.Config.api_secret ())
           ]
         |> Yojson.Safe.to_string
       in
       Logging.debug ~section "Sending Alpaca Market Data WS authentication...";
       Websocket_lwt_unix.write conn (Websocket.Frame.create ~content:auth_msg ())
       >>= fun () ->
       let authenticated = ref false in
       let auth_failure_reason = ref None in
       let on_auth_success () =
         if not !authenticated
         then (
           authenticated := true;
           on_connected ();
           Lwt.async (fun () -> send_subscription !active_subscriptions))
       in
       let on_auth_error code msg =
         let reason = Printf.sprintf "Alpaca Market Data WS error (%d): %s" code msg in
         auth_failure_reason := Some reason;
         Lwt.async (fun () ->
           Lwt.catch
             (fun () -> Websocket_lwt_unix.close_transport conn)
             (fun _exn -> Lwt.return_unit))
       in
       let rec read_loop () =
         Websocket_lwt_unix.read conn
         >>= fun frame ->
         on_heartbeat ();
         (match !auth_failure_reason with
          | Some reason -> Lwt.fail (Failure reason)
          | None ->
            (match frame.Websocket.Frame.opcode with
             | Websocket.Frame.Opcode.Ping ->
               let pong_frame =
                 Websocket.Frame.create
                   ~opcode:Websocket.Frame.Opcode.Pong
                   ~content:frame.Websocket.Frame.content
                   ()
               in
               Websocket_lwt_unix.write conn pong_frame
             | Websocket.Frame.Opcode.Pong ->
               (* Reply to our active [send_ping]; resolves any pending waiter. *)
               last_pong_time := Unix.gettimeofday ();
               Lwt_condition.broadcast pong_condition ();
               Lwt.return_unit
             | Websocket.Frame.Opcode.Close ->
               Lwt.fail (Failure "Alpaca Market Data WS received Close frame from server")
             | _ ->
               let content = String.trim frame.Websocket.Frame.content in
               if content <> ""
               then (
                 (* Feed cadence: gap since the previous data frame on this venue. *)
                 let now = Unix.gettimeofday () in
                 if !last_frame_time > 0.0
                 then Network_latency.record_feed_s "alpaca" (now -. !last_frame_time);
                 last_frame_time := now;
                 handle_message_str ~on_auth_success ~on_auth_error content);
               (match !auth_failure_reason with
                | Some reason -> Lwt.fail (Failure reason)
                | None -> Lwt.return_unit)))
         >>= fun () -> read_loop ()
       in
       (* Seamless session switching: while this incarnation is current, watch
           for the market session boundary. When the required feed changes
           (regular/pre-market/after-hours <-> overnight) close the socket so
           this read loop unwinds and the catch handler reconnects on the other
           feed - the store holds the last real quote during the sub-second
           gap, then fresh "q" messages resume streaming. *)
       let rec session_watcher () =
         Lwt_unix.sleep session_watch_seconds
         >>= fun () ->
         if !conn_generation <> my_gen
         then Lwt.return_unit
         else (
           match !active_conn with
           | Some c when c == conn ->
             let want_overnight = Alpaca_market_hours.is_overnight_hours () in
             if want_overnight <> is_overnight_feed
             then (
               session_switch := true;
               Logging.info_f
                 ~section
                 "Alpaca market session changed: switching to the %s feed"
                 (if want_overnight
                  then "overnight (v1beta1/overnight)"
                  else "regular (v2/" ^ !Alpaca_types.Config.data_feed ^ ")");
               Lwt.catch
                 (fun () -> Websocket_lwt_unix.close_transport conn)
                 (fun _exn -> Lwt.return_unit))
             else session_watcher ()
           | _ -> Lwt.return_unit)
       in
       Lwt.async session_watcher;
       read_loop ())
    (fun exn ->
       active_conn := None;
       if !session_switch
       then (
         (* Controlled switch: reconnect immediately on the other feed. *)
         Logging.info_f
           ~section
           "Alpaca data WS reconnecting to the %s feed"
           (if Alpaca_market_hours.is_overnight_hours () then "overnight" else "regular");
         connect_and_monitor ~on_failure ~on_connected ~on_heartbeat)
       else (
         let err = Printexc.to_string exn in
         Logging.error_f ~section "Alpaca data WS disconnected: %s" err;
         on_failure err;
         Lwt.return_unit))
;;

(** Sends a protocol-level WebSocket Ping frame and waits for the matching
    Pong within [timeout_ms]. Returns [true] when the Pong arrived, [false]
    on timeout or send failure. Records the round trip in the "alpaca" venue
    profiler (the dashboard's ws_ping column). *)
let send_ping ~req_id ~timeout_ms : bool Lwt.t =
  match !active_conn with
  | None -> Lwt.return false
  | Some conn ->
    let send_time = Unix.gettimeofday () in
    Lwt.catch
      (fun () ->
         let payload = Printf.sprintf "dio:%d:%.6f" req_id send_time in
         Websocket_lwt_unix.write
           conn
           (Websocket.Frame.create
              ~opcode:Websocket.Frame.Opcode.Ping
              ~content:payload
              ())
         >>= fun () ->
         let timeout = float_of_int timeout_ms /. 1000.0 in
         Lwt.pick
           [ (Lwt_condition.wait pong_condition
              >>= fun () ->
              (* Guard against a stale Pong from an earlier ping. *)
              if !last_pong_time >= send_time
              then (
                Network_latency.record_ping_s "alpaca" (Unix.gettimeofday () -. send_time);
                Lwt.return true)
              else Lwt.return false)
           ; (Lwt_unix.sleep timeout
              >>= fun () ->
              (* The Pong may have landed just before the timeout fired. *)
              if !last_pong_time >= send_time
              then (
                Network_latency.record_ping_s "alpaca" (Unix.gettimeofday () -. send_time);
                Lwt.return true)
              else (
                Logging.warn_f
                  ~section
                  "Alpaca data WS ping timed out (req_id: %d)"
                  req_id;
                Lwt.return false))
           ])
      (fun exn ->
         Logging.warn_f
           ~section
           "Alpaca data WS ping send failed: %s"
           (Printexc.to_string exn);
         Lwt.return false)
;;
