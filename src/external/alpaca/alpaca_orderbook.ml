(** WebSocket L1/L2 market data feed for Alpaca. Serves ring buffer quote snapshots. *)

open Lwt.Infix
open Dio_exchange.Exchange_intf.Types

let section = "alpaca_orderbook"

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
  type t =
    { symbol : string
    ; buffer : quote array
    ; trades_buffer : trade array
    ; capacity : int
    ; trades_capacity : int
    ; mutable write_pos : int
    ; mutable trades_write_pos : int
    ; mutable best_bid_ask : (float * float * float * float) option
    ; mutable last_update_ts : float
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
    ; best_bid_ask = None
    ; last_update_ts = 0.0
    ; mutex = Mutex.create ()
    }
  ;;

  let push t (q : quote) =
    Mutex.lock t.mutex;
    let idx = t.write_pos mod t.capacity in
    t.buffer.(idx) <- q;
    t.write_pos <- t.write_pos + 1;
    t.best_bid_ask <- Some (q.bid_price, q.bid_size, q.ask_price, q.ask_size);
    t.last_update_ts <- Unix.gettimeofday ();
    Mutex.unlock t.mutex;
    Concurrency.Exchange_wakeup.signal_all ()
  ;;

  let push_trade t (tr : trade) =
    Mutex.lock t.mutex;
    let idx = t.trades_write_pos mod t.trades_capacity in
    t.trades_buffer.(idx) <- tr;
    t.trades_write_pos <- t.trades_write_pos + 1;
    t.last_update_ts <- Unix.gettimeofday ();
    Mutex.unlock t.mutex;
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

  let get_last_update_ts t =
    Mutex.lock t.mutex;
    let ts = t.last_update_ts in
    Mutex.unlock t.mutex;
    ts
  ;;

  let get_best_bid_ask t =
    Mutex.lock t.mutex;
    let res = t.best_bid_ask in
    Mutex.unlock t.mutex;
    res
  ;;

  let get_current_position t =
    Mutex.lock t.mutex;
    let pos = t.write_pos in
    Mutex.unlock t.mutex;
    pos
  ;;

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
               SymbolStore.push_trade
                 store
                 { price; size; timestamp = ts; side = side_str };
               let is_non_regular = not (Alpaca_market_hours.is_regular_market_open ()) in
               let b_p, b_s, a_p, a_s =
                 match SymbolStore.get_best_bid_ask store with
                 | Some (prev_bp, prev_bs, prev_ap, prev_as) ->
                   if is_non_regular
                   then
                     if
                       prev_bp > 0.0
                       && prev_ap > 0.0
                       && prev_bp <= price
                       && price <= prev_ap
                     then prev_bp, prev_bs, prev_ap, prev_as
                     else price, size, price, size
                   else if
                     abs_float (prev_bp -. price) > price *. 0.005
                     || prev_bp <= 0.0
                     || prev_ap <= 0.0
                   then price, size, price, size
                   else prev_bp, prev_bs, prev_ap, prev_as
                 | None -> price, size, price, size
               in
               SymbolStore.push
                 store
                 { bid_price = b_p
                 ; bid_size = b_s
                 ; ask_price = a_p
                 ; ask_size = a_s
                 ; timestamp = ts
                 };
               Logging.debug_f
                 ~section
                 "[%s] Live trade update: price %.2f (sz %.2f)%s"
                 symbol
                 price
                 size
                 (if is_non_regular then " [after_hours]" else ""))
           | "b" ->
             let symbol =
               j |> member "S" |> to_string_option |> Option.value ~default:""
             in
             let close_p = j |> member "c" |> json_to_float in
             Logging.debug_f ~section "[%s] Bar close: %.2f" symbol close_p
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
       let last_ts = SymbolStore.get_last_update_ts store in
       if now -. last_ts >= 5.0
       then (
         Alpaca_rest.get_snapshot ~symbol:sym ()
         >>= function
         | Ok (bp, bs, ap, as_val) ->
           if bp > 0.0 || ap > 0.0
           then (
             SymbolStore.push
               store
               { bid_price = bp
               ; bid_size = bs
               ; ask_price = ap
               ; ask_size = as_val
               ; timestamp = now
               };
             Logging.debug_f
               ~section
               "[%s] Refreshed after-hours price via REST snapshot (idle %.1fs): bid \
                %.2f, ask %.2f"
               sym
               (now -. last_ts)
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
             SymbolStore.push
               store
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

let connect_and_monitor ~on_failure ~on_connected ~on_heartbeat =
  let url_str = Alpaca_types.Config.data_ws_url () in
  let uri = Uri.of_string url_str in
  let host = Uri.host uri |> Option.value ~default:"stream.data.alpaca.markets" in
  let port = Uri.port uri |> Option.value ~default:443 in
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
       Logging.debug_f ~section "Connected to Alpaca Market Data WS at %s" url_str;
       (* Authenticate *)
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
       (* Send pending subscriptions *)
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
          | Websocket.Frame.Opcode.Close ->
            Lwt.fail (Failure "Alpaca Market Data WS received Close frame from server")
          | _ ->
            let content = String.trim frame.Websocket.Frame.content in
            if content <> "" then handle_message_str content;
            Lwt.return_unit)
         >>= fun () -> read_loop ()
       in
       read_loop ())
    (fun exn ->
       active_conn := None;
       let err = Printexc.to_string exn in
       Logging.error_f ~section "Alpaca data WS disconnected: %s" err;
       on_failure err;
       Lwt.return_unit)
;;
