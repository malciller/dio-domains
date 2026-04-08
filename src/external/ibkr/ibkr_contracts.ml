(** Symbol → contract resolution via reqContractDetails.

    Caches resolved contracts so repeated lookups for the same symbol
    are O(1). ETFs use [secType = "STK"], [exchange = "SMART"],
    [currency = "USD"]. *)

open Lwt.Infix

let section = "ibkr_contracts"

(** Resolved contract cache: symbol → full contract. *)
let cache : (string, Ibkr_types.contract) Hashtbl.t = Hashtbl.create 32
let cache_mutex = Mutex.create ()

(** Next request ID for contract lookups. *)
let next_req_id = Atomic.make 9000

(** [resolve conn symbol] sends reqContractDetails for [symbol] and
    waits for the response. Caches the result for future calls.
    Returns the full contract with conId, minTick, etc. *)
let resolve conn ~symbol =
  (* Check cache first *)
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

      (* Accumulator for contract data *)
      let result = ref None in

      (* Register reqId-correlated handler *)
      let condition = Ibkr_dispatcher.register_req_handler
        ~req_id
        ~on_data:(fun fields ->
          (* For serverVersion >= 164 (ours is 176), no version field.
             Fields: [reqId, symbol, secType, lastTradeDate, strike, right,
                      exchange, currency, localSymbol, marketName,
                      tradingClass, conId, minTick, multiplier, ...] *)
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

          let contract = {
            Ibkr_types.con_id;
            symbol = symbol_resp;
            sec_type;
            exchange;
            currency;
            local_symbol;
            trading_class;
            min_tick;
            multiplier;
          } in
          result := Some contract;
          Logging.info_f ~section "Resolved %s: conId=%d minTick=%.4f exchange=%s"
            symbol con_id min_tick exchange
        )
        ~on_end:(fun () ->
          Logging.debug_f ~section "Contract details end for reqId=%d" req_id
        )
      in

      (* Send reqContractDetails
         Wire format: [msgId=9, version=8, reqId, contract..., includeExpired,
                       secIdType, secId, issuerId(v176+)] *)
      let lookup_contract = Ibkr_types.make_stk_contract ~symbol in
      let msg_fields =
        [string_of_int Ibkr_types.msg_req_contract_details;
         "8";    (* version *)
         string_of_int req_id]
        @ Ibkr_codec.encode_contract lookup_contract
        @ ["";     (* secIdType *)
           "";     (* secId *)
           ""]     (* issuerId - required for serverVersion >= 176 *)
      in
      Ibkr_connection.send conn msg_fields >>= fun () ->

      (* Wait for response with timeout *)
      Lwt.pick [
        (Lwt_condition.wait condition >|= fun () -> `Done);
        (Lwt_unix.sleep 10.0 >|= fun () -> `Timeout);
      ] >|= fun status ->
      Ibkr_dispatcher.remove_req_handler ~req_id;
      match status, !result with
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
          failwith (Printf.sprintf "No contract data received: %s" symbol)

(** [get_cached symbol] returns the cached contract, or None. *)
let get_cached ~symbol =
  Mutex.lock cache_mutex;
  let r = Hashtbl.find_opt cache symbol in
  Mutex.unlock cache_mutex;
  r

(** Return (price_decimals, qty_decimals) from cached contract.
    Price decimals derived from min_tick. *)
let get_precision ~symbol =
  match get_cached ~symbol with
  | Some c ->
      let price_dec =
        if c.min_tick >= 1.0 then 0
        else if c.min_tick >= 0.1 then 1
        else if c.min_tick >= 0.01 then 2
        else if c.min_tick >= 0.001 then 3
        else 4
      in
      Some (price_dec, 0)  (* ETFs trade in whole shares *)
  | None -> None
