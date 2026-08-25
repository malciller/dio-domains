(** Resolves symbols to IBKR contracts via reqContractDetails, with a
    mutex-guarded cache so repeated symbol lookups are O(1).
    ETFs are modeled as secType STK on SMART in USD. *)

open Lwt.Infix

let section = "ibkr_contracts"

(** Resolved contracts keyed by symbol; guarded by [cache_mutex]. *)
let cache : (string, Ibkr_types.contract) Hashtbl.t = Hashtbl.create 32

let cache_mutex = Mutex.create ()

(** Request id counter for reqContractDetails. *)
let next_req_id = Atomic.make 9000

(** Sends reqContractDetails for [symbol] and waits (up to 10s) for the
    response. Caches the result on success. The returned contract
    includes conId, minTick, and trading attributes. *)
let resolve conn ~symbol =
  (* Check cache before hitting the wire. *)
  Mutex.lock cache_mutex;
  let cached = Hashtbl.find_opt cache symbol in
  Mutex.unlock cache_mutex;
  match cached with
  | Some contract ->
    Logging.debug_f ~section "Contract cache hit for %s (conId=%d)" symbol contract.con_id;
    Lwt.return contract
  | None ->
    let req_id = Atomic.fetch_and_add next_req_id 1 in
    Logging.info_f ~section "Resolving contract for %s (reqId=%d)" symbol req_id;
    let result = ref None in
    (* Register a reqId-correlated handler for the response. *)
    let condition =
      Ibkr_dispatcher.register_req_handler
        ~req_id
        ~on_data:(fun fields ->
          (* Server version >= 176 omits the leading version field.
             Field order: reqId, symbol, secType, lastTradeDate, strike,
             right, exchange, currency, localSymbol, marketName,
             tradingClass, conId, minTick, multiplier. *)
          let _req_id, fields = Ibkr_codec.read_int fields in
          let symbol_resp, fields = Ibkr_codec.read_string fields in
          let sec_type, fields = Ibkr_codec.read_string fields in
          let _last_trade_date, fields = Ibkr_codec.read_string fields in
          let _strike, fields = Ibkr_codec.read_float fields in
          let _right, fields = Ibkr_codec.read_string fields in
          let exchange, fields = Ibkr_codec.read_string fields in
          let currency, fields = Ibkr_codec.read_string fields in
          let local_symbol, fields = Ibkr_codec.read_string fields in
          let _market_name, fields = Ibkr_codec.read_string fields in
          let trading_class, fields = Ibkr_codec.read_string fields in
          let con_id, fields = Ibkr_codec.read_int fields in
          let min_tick, fields = Ibkr_codec.read_float fields in
          let multiplier, _fields = Ibkr_codec.read_string fields in
          let contract =
            { Ibkr_types.con_id
            ; symbol = symbol_resp
            ; sec_type
            ; exchange
            ; currency
            ; local_symbol
            ; trading_class
            ; min_tick
            ; multiplier
            }
          in
          result := Some contract;
          Logging.info_f
            ~section
            "Resolved %s: conId=%d minTick=%.4f exchange=%s"
            symbol
            con_id
            min_tick
            exchange)
        ~on_end:(fun () ->
          Logging.debug_f ~section "Contract details end for reqId=%d" req_id)
    in
    (* reqContractDetails fields: msgId 9, version 8, reqId, contract,
       includeExpired, secIdType, secId, issuerId (required for server
       version >= 176). *)
    let lookup_contract = Ibkr_types.make_stk_contract ~symbol in
    let msg_fields =
      [ string_of_int Ibkr_types.msg_req_contract_details
      ; "8" (* message version *)
      ; string_of_int req_id
      ]
      @ Ibkr_codec.encode_contract lookup_contract
      @ [ ""; (* secIdType *) ""; (* secId *) "" ]
      (* issuerId *)
    in
    Ibkr_connection.send conn msg_fields
    >>= fun () ->
    (* Wait for the response condition, with a 10s timeout. *)
    Lwt.pick
      [ (Lwt_condition.wait condition >|= fun () -> `Done)
      ; (Lwt_unix.sleep 10.0 >|= fun () -> `Timeout)
      ]
    >|= fun status ->
    Ibkr_dispatcher.remove_req_handler ~req_id;
    (match status, !result with
     | `Done, Some contract ->
       Mutex.lock cache_mutex;
       Hashtbl.replace cache symbol contract;
       Mutex.unlock cache_mutex;
       contract
     | `Timeout, _ ->
       Logging.error_f ~section "Contract resolution timed out for %s" symbol;
       failwith (Printf.sprintf "Contract resolution timed out: %s" symbol)
     | _, None ->
       Logging.error_f ~section "No contract data received for %s" symbol;
       failwith (Printf.sprintf "No contract data received: %s" symbol))
;;

(** Cached contract for [symbol], or [None]. *)
let get_cached ~symbol =
  Mutex.lock cache_mutex;
  let r = Hashtbl.find_opt cache symbol in
  Mutex.unlock cache_mutex;
  r
;;

(** (price_decimals, qty_decimals) for [symbol]; price decimals derive
    from minTick. *)
let get_precision ~symbol =
  match get_cached ~symbol with
  | Some c ->
    let price_dec =
      if c.min_tick >= 1.0
      then 0
      else if c.min_tick >= 0.1
      then 1
      else if c.min_tick >= 0.01
      then 2
      else if c.min_tick >= 0.001
      then 3
      else 4
    in
    Some (price_dec, 0)
    (* Quantity precision is zero: fractional shares are unsupported
       for the ETF contracts this module routes. *)
  | None -> None
;;
