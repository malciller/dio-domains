(** Implements financial instrument symbol to complete trading contract resolution utilizing the reqContractDetails message vector.

    Maintains a thread safe cache of resolved Interactive Brokers contracts ensuring that subsequent identical symbol queries execute with O(1) time complexity. Exchange traded funds are specifically modeled utilizing security type STK, the SMART routing exchange, and USD currency settings. *)

open Lwt.Infix

let section = "ibkr_contracts"

(** Hash map storing resolved symbol strings mapped to their comprehensive Ibkr_types.contract structures. *)
let cache : (string, Ibkr_types.contract) Hashtbl.t = Hashtbl.create 32
let cache_mutex = Mutex.create ()

(** Atomic integer utilized for generating unique request identifiers during reqContractDetails network dispatches. *)
let next_req_id = Atomic.make 9000

(** Transmits a reqContractDetails invocation for the specified symbol across the given active connection and blocks asynchronously pending the server response. The resultant contract structure is persisted into memory cache for expediting future identical requests. Provides the fully initialized contract payload inclusive of conId and trading attributes such as minTick. *)
let resolve conn ~symbol =
  (* Inspect memory cache prior to network transmission *)
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

      (* Mutable reference accumulating inbound parsed contract metadata *)
      let result = ref None in

      (* Establish a callback router index correlated to the generated request identifier *)
      let condition = Ibkr_dispatcher.register_req_handler
        ~req_id
        ~on_data:(fun fields ->
          (* Parsing logic predicated on server version 176 and greater omitting the initial version specifier.
             Sequential field decoding maps to reqId, symbol, secType, lastTradeDate, strike, right, exchange, currency, localSymbol, marketName, tradingClass, conId, minTick, multiplier respectively. *)
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

      (* Formulate and stream out the reqContractDetails wire format array.
         Protocol message sequence corresponds to msgId 9, version 8, reqId, trailing contract definition, includeExpired, secIdType, secId, concluding with issuerId mandated for protocol versions 176 and above. *)
      let lookup_contract = Ibkr_types.make_stk_contract ~symbol in
      let msg_fields =
        [string_of_int Ibkr_types.msg_req_contract_details;
         "8";    (* Hardcoded protocol payload version *)
         string_of_int req_id]
        @ Ibkr_codec.encode_contract lookup_contract
        @ ["";     (* Blank secIdType specification *)
           "";     (* Blank secId attribute *)
           ""]     (* Blank issuerId attribute strictly required for server version 176 and higher *)
      in
      Ibkr_connection.send conn msg_fields >>= fun () ->

      (* Asynchronously yield execution context awaiting server callback activation paired with an absolute duration timeout *)
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

(** Synchronously retrieves an associated contract from the memory cache mapping substituting None if unpopulated. *)
let get_cached ~symbol =
  Mutex.lock cache_mutex;
  let r = Hashtbl.find_opt cache symbol in
  Mutex.unlock cache_mutex;
  r

(** Computes and extracts a tuple encapsulating the price and quantity decimal point precision requirements sourced directly from the resolved cached contract. The price precision bounds are mathematically extrapolated utilizing the contract min_tick attribute. *)
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
      Some (price_dec, 0)  (* Zero decimal precision constraint bounded by fractional share inability for configured ETF processing *)
  | None -> None
