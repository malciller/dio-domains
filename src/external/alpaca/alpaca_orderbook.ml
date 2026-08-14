(** WebSocket L1/L2 market data feed for Alpaca. Serves ring buffer quote snapshots. *)

open Lwt.Infix
open Dio_exchange.Exchange_intf.Types

let section = "alpaca_orderbook"

(** A real quote older than this (arrival wall-clock) is considered stale:
    the trade handler then falls back to single-price trade prints (clearly
    labeled) until a fresh quote resumes. Matches the REST snapshot poll's
    idle threshold. *)
let stale_quote_seconds = 5.0

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
  (** Single-writer atomic TOB cache (H2): the WS writer publishes a fresh
      immutable record on every push, and readers do a single [Atomic.get]
      with no mutex. Position, best-bid/ask, and update-timestamp travel
      together so a reader can never observe a torn (mixed-generation)
      snapshot. *)
  type tob_cache =
    { pos : int
    ; bid_px : float
    ; bid_sz : float
    ; ask_px : float
    ; ask_sz : float
    ; ts : float
      (** Arrival wall-clock of the last update from ANY source (quote,
            trade, nudge, fallback). Drives the REST snapshot poll's idle
            detection. *)
    ; quote_event_ts : float
      (** Exchange event timestamp of the last REAL quote ("q" message), in
            epoch seconds; [0.0] when no real quote has arrived or its
            timestamp was unparseable. Used to reject out-of-order trades (a
            stale print from a previous session must never move a fresher
            quote). *)
    ; quote_wall_ts : float
      (** Arrival wall-clock of the last REAL quote ("q" message or REST
            snapshot). When this ages past [stale_quote_seconds] the trade
            handler switches to its single-price fallback mode. *)
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
    ; mutable fallback_engaged : bool
      (** True while the trade-price fallback is publishing bid/ask from
            trade prints (no fresh real quote for [stale_quote_seconds]).
            Cleared on the next real quote. Guarded by [mutex]. *)
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
          ; ts = 0.0
          ; quote_event_ts = 0.0
          ; quote_wall_ts = 0.0
          ; valid = false
          }
    ; fallback_engaged = false
    ; mutex = Mutex.create ()
    }
  ;;

  let push_internal t (q : quote) ~is_quote ~event_ts ~fallback =
    let tob_cache =
      Mutex.lock t.mutex;
      let idx = t.write_pos mod t.capacity in
      t.buffer.(idx) <- q;
      t.write_pos <- t.write_pos + 1;
      let now = Unix.gettimeofday () in
      let prev = Atomic.get t.tob in
      let cache =
        { pos = t.write_pos
        ; bid_px = q.bid_price
        ; bid_sz = q.bid_size
        ; ask_px = q.ask_price
        ; ask_sz = q.ask_size
        ; ts = now
        ; quote_event_ts =
            (if is_quote && event_ts > 0.0 then event_ts else prev.quote_event_ts)
        ; quote_wall_ts = (if is_quote then now else prev.quote_wall_ts)
        ; valid = q.bid_price > 0.0 || q.ask_price > 0.0
        }
      in
      if is_quote
      then t.fallback_engaged <- false
      else if fallback
      then t.fallback_engaged <- true;
      Mutex.unlock t.mutex;
      cache
    in
    Atomic.set t.tob tob_cache;
    Concurrency.Exchange_wakeup.signal_all ()
  ;;

  (** Plain push from a non-quote source (a single-side trade nudge or a
      seed): does NOT advance the real-quote bookkeeping, so it can never
      mask a stale quote. *)
  let push t q = push_internal t q ~is_quote:false ~event_ts:0.0 ~fallback:false

  (** Push a REAL quote (WS "q" message or REST snapshot): records the
      exchange event time and marks the quote feed fresh. *)
  let push_quote t q ~event_ts =
    push_internal t q ~is_quote:true ~event_ts ~fallback:false
  ;;

  (** Push the trade-price stale fallback (bid = ask = last trade): engages
      the fallback flag so the next real quote can log the recovery. *)
  let push_fallback t q = push_internal t q ~is_quote:false ~event_ts:0.0 ~fallback:true

  let push_trade t (tr : trade) =
    let now =
      Mutex.lock t.mutex;
      let idx = t.trades_write_pos mod t.trades_capacity in
      t.trades_buffer.(idx) <- tr;
      t.trades_write_pos <- t.trades_write_pos + 1;
      let now = Unix.gettimeofday () in
      Mutex.unlock t.mutex;
      now
    in
    (* Trade updates bump the update timestamp without changing TOB prices. *)
    let prev = Atomic.get t.tob in
    Atomic.set t.tob { prev with ts = now };
    Concurrency.Exchange_wakeup.signal_all ()
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

  let get_last_update_ts t = (Atomic.get t.tob).ts

  (** Arrival wall-clock of the last REAL quote; [0.0] before any quote. *)
  let get_last_quote_wall t = (Atomic.get t.tob).quote_wall_ts

  (** Exchange event time of the last REAL quote; [0.0] when unknown. *)
  let get_last_quote_event_ts t = (Atomic.get t.tob).quote_event_ts

  (** True while the trade-price stale fallback is the active TOB source. *)
  let get_fallback_engaged t =
    Mutex.lock t.mutex;
    let f = t.fallback_engaged in
    Mutex.unlock t.mutex;
    f
  ;;

  let get_best_bid_ask t =
    let c = Atomic.get t.tob in
    if c.valid then Some (c.bid_px, c.bid_sz, c.ask_px, c.ask_sz) else None
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

(** Pure book-update rule for a trade print against the current top-of-book.
    A trade is evidence of price, never a two-sided quote, so it must not
    fabricate a bid/ask from a fill. Returns [Some (b, b_s, a, a_s)] when the
    book should be re-published and [None] when the trade leaves it
    untouched:
    - no book yet: seed a single price so the domain has something to size on
      (a real quote replaces it as soon as one arrives);
    - [quote_stale] (no real quote for [stale_quote_seconds]): the trade price
      serves as a single-price fallback until the quote feed resumes;
    - the trade crossed the ask (price > ask): lift the ask to the trade
      price, leaving the bid untouched;
    - the trade crossed the bid (price < bid): drop the bid to the trade
      price, leaving the ask untouched;
    - otherwise (print inside the current spread): the book is unchanged.
    [trade_newer] rejects prints whose exchange event time is older than the
    current quote's - an out-of-order/stale print from a previous session
    must never move a fresher quote (this is what killed the book during the
    pre-market -> regular transition). *)
let update_tob_from_trade
      ~quote_stale
      ~trade_newer
      (prev : (float * float * float * float) option)
      ~(price : float)
      ~(size : float)
  : (float * float * float * float) option
  =
  match prev with
  | None -> Some (price, size, price, size)
  | Some (prev_bp, prev_bs, prev_ap, prev_as) when trade_newer ->
    if quote_stale
    then Some (price, size, price, size)
    else if prev_ap > 0.0 && price > prev_ap
    then Some (prev_bp, prev_bs, price, size)
    else if prev_bp > 0.0 && price < prev_bp
    then Some (price, size, prev_ap, prev_as)
    else None
  | Some _ -> None
;;

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
  | None -> None
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

let handle_message_str content =
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
                 let was_fallback = SymbolStore.get_fallback_engaged store in
                 SymbolStore.push_quote
                   store
                   ~event_ts:(Alpaca_rest.parse_iso_timestamp ts_str)
                   { bid_price = final_bp
                   ; bid_size = final_bs
                   ; ask_price = final_ap
                   ; ask_size = final_as
                   ; timestamp = ts
                   };
                 if was_fallback
                 then
                   Logging.info_f
                     ~section
                     "[%s] Real quote resumed after stale-price fallback: bid %.2f, ask \
                      %.2f"
                     symbol
                     final_bp
                     final_ap;
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
               SymbolStore.push_trade
                 store
                 { price; size; timestamp = ts; side = side_str };
               (* The book update is session-agnostic and event-ordered: a
                   real quote is always the truth; a trade only ever nudges a
                   single crossed side, and only when it is not older than the
                   current quote (out-of-order prints from a previous session
                   are ignored). With no fresh quote for
                   [stale_quote_seconds], trade prices stand in as a clearly
                   labeled single-price fallback. *)
               let quote_stale =
                 Unix.gettimeofday () -. SymbolStore.get_last_quote_wall store
                 >= stale_quote_seconds
               in
               let trade_event_ts = Alpaca_rest.parse_iso_timestamp ts_str in
               let quote_event_ts = SymbolStore.get_last_quote_event_ts store in
               let trade_newer =
                 trade_event_ts = 0.0
                 || quote_event_ts = 0.0
                 || trade_event_ts >= quote_event_ts
               in
               let book_update =
                 update_tob_from_trade
                   ~quote_stale
                   ~trade_newer
                   (SymbolStore.get_best_bid_ask store)
                   ~price
                   ~size
               in
               (match book_update with
                | Some (b_p, b_s, a_p, a_s) when quote_stale ->
                  if not (SymbolStore.get_fallback_engaged store)
                  then
                    Logging.info_f
                      ~section
                      "[%s] No fresh quote in %.0fs - using trade prices as a \
                       single-price fallback until the quote feed resumes"
                      symbol
                      stale_quote_seconds;
                  SymbolStore.push_fallback
                    store
                    { bid_price = b_p
                    ; bid_size = b_s
                    ; ask_price = a_p
                    ; ask_size = a_s
                    ; timestamp = ts
                    }
                | Some (b_p, b_s, a_p, a_s) ->
                  SymbolStore.push
                    store
                    { bid_price = b_p
                    ; bid_size = b_s
                    ; ask_price = a_p
                    ; ask_size = a_s
                    ; timestamp = ts
                    }
                | None -> ());
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
             (* Alpaca sends application-level heartbeats when no other data
                 is flowing (e.g. quiet market). They keep the connection warm
                 and are counted as feed frames, so the ws_feed gap metric
                 stays honest during idle periods. *)
             Logging.debug ~section "Alpaca Market Data WS heartbeat"
           | "success" ->
             let msg =
               j |> member "msg" |> to_string_option |> Option.value ~default:""
             in
             Logging.debug_f ~section "Alpaca Market Data WS status: %s" msg
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
             Logging.error_f ~section "Alpaca Market Data WS error (%d): %s" code msg
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

let polling_started = ref false

let rec poll_snapshots_loop () =
  Lwt_unix.sleep 2.0
  >>= fun () ->
  let now = Unix.gettimeofday () in
  let syms = !active_subscriptions in
  Lwt_list.iter_p
    (fun sym ->
       let store = get_or_create_store sym in
       (* Gate on the last REAL quote, not any update: a stream of trade
          nudges/fallbacks must not mask a missing quote feed - the REST
          snapshot is a genuine quote source that should take over. *)
       let last_quote_ts = SymbolStore.get_last_quote_wall store in
       if now -. last_quote_ts >= stale_quote_seconds
       then (
         Alpaca_rest.get_snapshot ~symbol:sym ()
         >>= function
         | Ok (bp, bs, ap, as_val) ->
           if bp > 0.0 || ap > 0.0
           then (
             let was_fallback = SymbolStore.get_fallback_engaged store in
             SymbolStore.push_quote
               store
               ~event_ts:0.0
               { bid_price = bp
               ; bid_size = bs
               ; ask_price = ap
               ; ask_size = as_val
               ; timestamp = now
               };
             if was_fallback
             then
               Logging.info_f
                 ~section
                 "[%s] Real quote resumed after stale-price fallback (REST snapshot): \
                  bid %.2f, ask %.2f"
                 sym
                 bp
                 ap;
             Logging.debug_f
               ~section
               "[%s] Refreshed price via REST snapshot (idle %.1fs): bid %.2f, ask %.2f"
               sym
               (now -. last_quote_ts)
               bp
               ap);
           Lwt.return_unit
         | Error e ->
           Logging.debug_f ~section "[%s] Background snapshot poll error: %s" sym e;
           Lwt.return_unit)
       else Lwt.return_unit)
    syms
  >>= fun () -> poll_snapshots_loop ()
;;

let ensure_polling_started () =
  if not !polling_started
  then (
    polling_started := true;
    Lwt.async poll_snapshots_loop)
;;

let subscribe_symbols symbols =
  ensure_polling_started ();
  let new_syms = List.filter (fun s -> not (List.mem s !active_subscriptions)) symbols in
  if new_syms <> []
  then (
    active_subscriptions := !active_subscriptions @ new_syms;
    Lwt.async (fun () ->
      Lwt_list.iter_p
        (fun sym ->
           Alpaca_rest.get_snapshot ~symbol:sym ()
           >|= function
           | Ok (bp, bs, ap, as_val) ->
             let store = get_or_create_store sym in
             SymbolStore.push_quote
               store
               ~event_ts:0.0
               { bid_price = bp
               ; bid_size = bs
               ; ask_price = ap
               ; ask_size = as_val
               ; timestamp = Unix.time ()
               };
             Logging.debug_f
               ~section
               "[%s] Seeded live price from REST snapshot: bid %.2f, ask %.2f"
               sym
               bp
               ap
           | Error e ->
             Logging.warn_f ~section "[%s] Failed to seed REST snapshot: %s" sym e)
        new_syms);
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
    few seconds of the boundary; during the gap the REST snapshot poll holds
    the price). *)
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
       on_connected ();
       (* Send any subscriptions requested before the connection was established. *)
       send_subscription !active_subscriptions
       >>= fun () ->
       let rec read_loop () =
         Websocket_lwt_unix.read conn
         >>= fun frame ->
         on_heartbeat ();
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
              handle_message_str content);
            Lwt.return_unit)
         >>= fun () -> read_loop ()
       in
       (* Seamless session switching: while this incarnation is current, watch
          for the market session boundary. When the required feed changes
          (regular/pre-market/after-hours <-> overnight) close the socket so
          this read loop unwinds and the catch handler reconnects on the other
          feed - the REST snapshot poll and the stale trade fallback hold the
          price during the sub-second gap, so the domain never sees a hole. *)
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
               Websocket_lwt_unix.close_transport conn >>= fun () -> Lwt.return_unit)
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
