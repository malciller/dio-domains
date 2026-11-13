(** Kraken trading action utilities. Provides helper functions for order parameter
    formatting, precision truncation, and payload construction. *)
open Lwt.Infix

open Yojson.Safe

(** Prepends a (key, value) pair to a JSON association list. *)
let add_to_assoc (json : Yojson.Safe.t) (key_value_pair : string * Yojson.Safe.t)
  : Yojson.Safe.t
  =
  match json with
  | `Assoc assoc_list ->
    let key, value = key_value_pair in
    `Assoc ((key, value) :: assoc_list)
  | _ -> failwith "add_to_assoc: expected JSON association object"
;;

let section = "kraken_actions"

(** Per-attempt response timeout for place/amend/cancel. *)
let order_request_timeout_ms = 10000

(** Worst case for one order operation. Amend/cancel retry at this timeout; place is
    single-attempt (a retried placement could double-fill), so this is the wider of the
    two. *)
let order_budget_s =
  Error_handling.retry_budget_s
    ~request_timeout_s:(float_of_int order_request_timeout_ms /. 1000.0)
    Error_handling.default_retry_config
;;

(** Rounds a price to the instrument's configured decimal precision. *)
let truncate_price_to_precision price symbol =
  match Kraken_instruments_feed.get_precision_info symbol with
  | Some (price_precision, qty_precision) ->
    Logging.debug_f
      ~section
      "Truncating price %.8f for %s with precision %d (qty_precision: %d)"
      price
      symbol
      price_precision
      qty_precision;
    (* Round to the absolute precision first, preventing truncation errors. *)
    let multiplier = 10.0 ** float_of_int price_precision in
    let truncated_price = Float.round (price *. multiplier) /. multiplier in
    (* Reparses to eliminate floating point artifacts. *)
    let formatted_str = Printf.sprintf "%.*f" price_precision truncated_price in
    let clean_price = Float.of_string formatted_str in
    Logging.debug_f
      ~section
      "Rounded price %.8f -> %s -> %.8f for %s"
      price
      formatted_str
      clean_price
      symbol;
    clean_price
  | None ->
    Logging.warn_f ~section "No price precision info for %s, using original price" symbol;
    price
;;

(** Returns true if the string contains the given substring. Delegates to the centralized
    [Error_handling.string_contains]. *)
let string_contains = Error_handling.string_contains

(** JSON string literal, escaped exactly as {!json_to_string_precise} escapes it. *)
let json_escape_into buf s =
  Buffer.add_char buf '"';
  String.iter
    (function
      | '"' -> Buffer.add_string buf "\\\""
      | '\\' -> Buffer.add_string buf "\\\\"
      | '\b' -> Buffer.add_string buf "\\b"
      | '\012' -> Buffer.add_string buf "\\f"
      | '\n' -> Buffer.add_string buf "\\n"
      | '\r' -> Buffer.add_string buf "\\r"
      | '\t' -> Buffer.add_string buf "\\t"
      | c -> Buffer.add_char buf c)
    s;
  Buffer.add_char buf '"'
;;

(** Number formatted with the instrument's precision for [field], matching
    {!json_to_string_precise}'s rule: a field whose name contains "price" uses the price
    precision, everything else the quantity precision. *)
let precise_float_into buf symbol ~field f =
  let precision =
    match symbol with
    | Some sym ->
      (match Kraken_instruments_feed.get_precision_info sym with
       | Some (price_precision, qty_precision) ->
         if string_contains field "price" then price_precision else qty_precision
       | None -> 8)
    | None -> 8
  in
  Buffer.add_string buf (Printf.sprintf "%.*f" precision f)
;;

(** Serializes JSON to a string, applying instrument-specific float precision bounds. *)
let rec json_to_string_precise ?field_name symbol (json : Yojson.Safe.t) : string =
  match json with
  | `Null -> "null"
  | `Bool b -> if b then "true" else "false"
  | `Int i -> string_of_int i
  | `Float f ->
    (* Select the precision bounds based on the field name context. *)
    let precision =
      match symbol with
      | Some sym ->
        (match Kraken_instruments_feed.get_precision_info sym with
         | Some (price_precision, qty_precision) ->
           (* Use price precision for price fields and quantity precision otherwise. *)
           (match field_name with
            | Some name when string_contains name "price" -> price_precision
            | _ -> qty_precision)
         | None -> 8)
      | None -> 8
    in
    Printf.sprintf "%.*f" precision f
  | `String s ->
    (* Escapes JSON string literal values. *)
    "\""
    ^ String.concat
        ""
        (List.map
           (function
             | '"' -> "\\\""
             | '\\' -> "\\\\"
             | '\b' -> "\\b"
             | '\012' -> "\\f"
             | '\n' -> "\\n"
             | '\r' -> "\\r"
             | '\t' -> "\\t"
             | c -> String.make 1 c)
           (List.init (String.length s) (String.get s)))
    ^ "\""
  | `List items ->
    "["
    ^ String.concat "," (List.map (json_to_string_precise ?field_name symbol) items)
    ^ "]"
  | `Assoc pairs ->
    "{"
    ^ String.concat
        ","
        (List.map
           (fun (k, v) ->
             "\"" ^ k ^ "\":" ^ json_to_string_precise ~field_name:k symbol v)
           pairs)
    ^ "}"
  | `Intlit s -> s
  | `Tuple _ -> failwith "json_to_string_precise: Tuple not supported"
  | `Variant _ -> failwith "json_to_string_precise: Variant not supported"
;;

(** Generates a sequential request identifier. *)
let next_req_id =
  let counter = ref 0 in
  let ping_start_id = 1000000 in
  (* Denotes the reserved ping ID boundary. *)
  fun () ->
    incr counter;
    let id = !counter in
    if id >= ping_start_id - 1000
    then
      Logging.warn_f
        ~section
        "Trading request ID %d approaching ping ID range (ping IDs start at %d) - \
         potential collision risk"
        id
        ping_start_id;
    id
;;

(** Retry configuration, re-exported from the centralized [Error_handling] module. *)
type retry_config = Error_handling.retry_config =
  { max_attempts : int
  ; base_delay_ms : float
  ; max_delay_ms : float
  ; backoff_factor : float
  }

let default_retry_config = Error_handling.default_retry_config

(** Returns true for errors that indicate transient network conditions. Delegates to the
    centralized [Error_handling.is_retriable_error]. *)
let is_retriable_error = Error_handling.is_retriable_error

(* Reused body buffer, domain-local so a second scheduler cannot share it. *)
[@@@alert "-unsafe_multidomain"]

let order_body_buf_key = Domain.DLS.new_key (fun () -> Buffer.create 1024)
let order_body_buf () = Domain.DLS.get order_body_buf_key

(* Restore the alert beyond the two bindings above. *)
[@@@alert "+unsafe_multidomain"]

(* Write the add_order request body straight into [buf], in the exact shape
    {!json_to_string_precise} produces for the equivalent Yojson tree. The order path is the
    one place where building a whole JSON AST per order is measurable, and the websocket
   only needs the bytes. *)
let add_order_body_into
      buf
      ~symbol
      ~req_id
      ~order_type
      ~side
      ~order_qty
      ~token
      ?limit_price
      ?time_in_force
      ?post_only
      ?margin
      ?reduce_only
      ?order_userref
      ?cl_ord_id
      ?trigger_price
      ?trigger_price_type
      ?display_qty
      ?fee_preference
      ?validate
      ()
  =
  let started = ref false in
  let field name =
    if !started then Buffer.add_char buf ',';
    started := true;
    Buffer.add_char buf '"';
    Buffer.add_string buf name;
    Buffer.add_string buf "\":"
  in
  let num name f =
    field name;
    precise_float_into buf (Some symbol) ~field:name f
  in
  let str name v =
    field name;
    json_escape_into buf v
  in
  let bool name v =
    field name;
    Buffer.add_string buf (if v then "true" else "false")
  in
  Buffer.add_string buf "{\"method\":\"add_order\",\"params\":{";
  str "order_type" order_type;
  str "side" side;
  num "order_qty" order_qty;
  str "symbol" symbol;
  str "token" token;
  (match limit_price with
   | Some price -> num "limit_price" (truncate_price_to_precision price symbol)
   | None -> ());
  (match time_in_force with
   | Some tif -> str "time_in_force" tif
   | None -> ());
  (match post_only with
   | Some v -> bool "post_only" v
   | None -> ());
  (match margin with
   | Some v -> bool "margin" v
   | None -> ());
  (match reduce_only with
   | Some v -> bool "reduce_only" v
   | None -> ());
  (match order_userref with
   | Some v ->
     field "order_userref";
     Buffer.add_string buf (string_of_int v)
   | None -> ());
  (match cl_ord_id with
   | Some v -> str "cl_ord_id" v
   | None -> ());
  (match trigger_price with
   | Some price ->
     field "triggers";
     Buffer.add_string buf "{\"price\":";
     precise_float_into buf (Some symbol) ~field:"price"
       (truncate_price_to_precision price symbol);
     (match trigger_price_type with
      | Some tpt ->
        Buffer.add_string buf ",\"price_type\":";
        json_escape_into buf tpt
      | None -> ());
     Buffer.add_char buf '}'
   | None -> ());
  (match display_qty with
   | Some v -> num "display_qty" v
   | None -> ());
  (match fee_preference with
   | Some v -> str "fee_preference" v
   | None -> ());
  (match validate with
   | Some v -> bool "validate" v
   | None -> ());
  Buffer.add_string buf "},\"req_id\":";
  Buffer.add_string buf (string_of_int req_id);
  Buffer.add_char buf '}'
;;

(** Places an order, retrying via [retry_config]. *)
let place_order
  ~token
  ~order_type
  ~side
  ~order_qty
  ~symbol
  ?limit_price
  ?time_in_force
  ?post_only
  ?margin
  ?reduce_only
  ?order_userref
  ?cl_ord_id
  ?trigger_price
  ?trigger_price_type
  ?display_qty
  ?fee_preference
  ?validate
  ?retry_config
  ()
  : (Kraken_common_types.add_order_result, string) result Lwt.t
  =
  let config =
    (* Placements must NOT retry on transport errors (Timeout/Connection): the request may
       have reached the matching engine before the 10s WS response timeout, so a retry can
       silently double-place (Kraken v2 has no idempotency token for userref-tagged grid
       orders). We force a single attempt: an ambiguous failure surfaces to the strategy
       via the Failed event, its in-flight/dedup guards clear, and the ladder re-places on
       its next cycle - the engine's own reconciliation is the recovery path. Amend/cancel
       keep their converge-safe retries. *)
    match retry_config with
    | Some c -> { c with max_attempts = 1 }
    | None -> { default_retry_config with max_attempts = 1 }
  in
  (* Ensure cl_ord_id is stable across retries for idempotency. If the caller didn't
     provide one and order_userref is not set, generate a unique ID before entering the
     retry loop. Note: Kraken API errors if both cl_ord_id and order_userref are sent. *)
  let cl_ord_id =
    match cl_ord_id, order_userref with
    | Some _, _ -> cl_ord_id
    | None, Some _ -> None
    | None, None ->
      Some
        (Printf.sprintf
           "dio_%d_%d"
           (next_req_id ())
           (int_of_float (Unix.gettimeofday () *. 1000.0)))
  in
  let place_order_once () =
    let req_id = next_req_id () in
    Logging.debug_f
      ~section
      "Placing %s order: %s %s %f @ %s"
      side
      symbol
      order_type
      order_qty
      (match limit_price with
       | Some p -> Printf.sprintf "%.6f" (truncate_price_to_precision p symbol)
       | None -> "market");
    let buf = order_body_buf () in
    Buffer.clear buf;
    add_order_body_into
      buf
      ~symbol
      ~req_id
      ~order_type
      ~side
      ~order_qty
      ~token
      ?limit_price
      ?time_in_force
      ?post_only
      ?margin
      ?reduce_only
      ?order_userref
      ?cl_ord_id
      ?trigger_price
      ?trigger_price_type
      ?display_qty
      ?fee_preference
      ?validate
      ();
    let body = Buffer.contents buf in
    if Logging.will_log Logging.DEBUG section
    then Logging.debug_f ~section "Order body: %s" body;
    Kraken_trading_client.send_request_body
      ~body
      ~req_id
      ~expected_method:"add_order"
      ~timeout_ms:order_request_timeout_ms
    >>= fun response ->
    if response.success
    then (
      match response.result with
      | Some result_json ->
        (try
           let order_id = Util.(member "order_id" result_json |> to_string) in
           let cl_ord_id = Util.(member "cl_ord_id" result_json |> to_string_option) in
           let order_userref =
             Util.(member "order_userref" result_json |> to_int_option)
           in
           Lwt.return (Ok { Kraken_common_types.order_id; cl_ord_id; order_userref })
         with
         | exn ->
           let err =
             Printf.sprintf "Failed to parse order response: %s" (Printexc.to_string exn)
           in
           Logging.error_f ~section "%s" err;
           Lwt.return (Error err))
      | None ->
        let err = "No result in successful response" in
        Logging.error ~section err;
        Lwt.return (Error err))
    else (
      let err =
        match response.error with
        | Some e -> e
        | None -> "Unknown error"
      in
      Logging.debug_f ~section "Order placement failed: %s" err;
      Lwt.return (Error err))
  in
  Error_handling.retry_with_backoff ~section ~config ~f:place_order_once ()
;;

(** Amends an order, retrying via [retry_config]. *)
let amend_order
  ~token
  ~order_id
  ?cl_ord_id
  ?order_qty
  ?limit_price
  ?limit_price_type
  ?post_only
  ?trigger_price
  ?trigger_price_type
  ?display_qty
  ?deadline
  ?validate
  ?symbol
  ?retry_config
  ()
  : (Kraken_common_types.amend_order_result, string) result Lwt.t
  =
  let config =
    match retry_config with
    | Some c -> c
    | None -> default_retry_config
  in
  let amend_order_once () =
    let req_id = next_req_id () in
    let symbol_str = Option.value symbol ~default:"" in
    if symbol_str = "" then failwith "Symbol required for price precision in amend_order";
    (* Fail if no symbol was supplied; price precision truncation requires it. *)
    let params = `Assoc [ "order_id", `String order_id; "token", `String token ] in
    let params =
      match order_qty with
      | Some qty -> add_to_assoc params ("order_qty", `Float qty)
      | None -> params
    in
    let params =
      match cl_ord_id with
      | Some id -> add_to_assoc params ("cl_ord_id", `String id)
      | None -> params
    in
    let params =
      match limit_price with
      | Some price ->
        let truncated_price = truncate_price_to_precision price symbol_str in
        add_to_assoc params ("limit_price", `Float truncated_price)
      | None -> params
    in
    let params =
      match limit_price_type with
      | Some lpt -> add_to_assoc params ("limit_price_type", `String lpt)
      | None -> params
    in
    let params =
      match post_only with
      | Some po -> add_to_assoc params ("post_only", `Bool po)
      | None -> params
    in
    let params =
      match trigger_price with
      | Some price ->
        let truncated_price = truncate_price_to_precision price symbol_str in
        add_to_assoc params ("trigger_price", `Float truncated_price)
      | None -> params
    in
    let params =
      match trigger_price_type with
      | Some tpt -> add_to_assoc params ("trigger_price_type", `String tpt)
      | None -> params
    in
    let params =
      match display_qty with
      | Some qty -> add_to_assoc params ("display_qty", `Float qty)
      | None -> params
    in
    let params =
      match deadline with
      | Some deadline -> add_to_assoc params ("deadline", `String deadline)
      | None -> params
    in
    let params =
      match validate with
      | Some v -> add_to_assoc params ("validate", `Bool v)
      | None -> params
    in
    let params =
      match symbol with
      | Some s -> add_to_assoc params ("symbol", `String s)
      | None -> params
    in
    Logging.debug_f
      ~section
      "Amending order %s: %s%s%s"
      order_id
      (match order_qty with
       | Some qty -> Printf.sprintf "qty=%.8f" qty
       | None -> "")
      (match limit_price with
       | Some p ->
         Printf.sprintf ", price=%.6f" (truncate_price_to_precision p symbol_str)
       | None -> "")
      (match deadline with
       | Some d -> Printf.sprintf ", deadline=%s" d
       | None -> "");
    Kraken_trading_client.send_request
      ~symbol:(Some symbol_str)
      ~method_:"amend_order"
      ~params
      ~req_id
      ~timeout_ms:order_request_timeout_ms
    >>= fun response ->
    if response.success
    then (
      match response.result with
      | Some result_json ->
        (try
           let amend_id = Util.(member "amend_id" result_json |> to_string) in
           let order_id = Util.(member "order_id" result_json |> to_string) in
           let cl_ord_id = Util.(member "cl_ord_id" result_json |> to_string_option) in
           Lwt.return (Ok { Kraken_common_types.amend_id; order_id; cl_ord_id })
         with
         | exn ->
           let err =
             Printf.sprintf "Failed to parse amend response: %s" (Printexc.to_string exn)
           in
           Logging.error_f ~section "%s" err;
           Lwt.return (Error err))
      | None ->
        let err = "No result in successful amend response" in
        Logging.error ~section err;
        Lwt.return (Error err))
    else (
      let err =
        match response.error with
        | Some e -> e
        | None -> "Unknown error"
      in
      if string_contains err "EOrder:No amendable parameters specified"
      then (
        Logging.debug_f
          ~section
          "Order amendment returned no changes for %s, treating as skipped"
          order_id;
        Lwt.return
          (Ok { Kraken_common_types.amend_id = "skipped_no_change"; order_id; cl_ord_id }))
      else (
        Logging.error_f ~section "Order amendment failed: %s" err;
        Lwt.return (Error err)))
  in
  Error_handling.retry_with_backoff ~section ~config ~f:amend_order_once ()
;;

(** Cancels orders, one request per order, retried via [retry_config]. Requests are sent
    in parallel, each carrying its own req_id so responses correlate per order. A single
    shared req_id would desync multi-frame responses, since [resolve_response] removes the
    waiter on the first frame, and Kraken does not offer a true batch cancel. *)
let cancel_orders ~token ?order_ids ?cl_ord_ids ?order_userrefs ?retry_config ()
  : (Kraken_common_types.cancel_order_result list, string) result Lwt.t
  =
  let config =
    match retry_config with
    | Some c -> c
    | None -> default_retry_config
  in
  let individual_cancels =
    let from_order_ids =
      match order_ids with
      | Some ids -> List.map (fun id -> "order_id", `List [ `String id ]) ids
      | None -> []
    in
    let from_cl_ord_ids =
      match cl_ord_ids with
      | Some ids -> List.map (fun id -> "cl_ord_id", `List [ `String id ]) ids
      | None -> []
    in
    let from_userrefs =
      match order_userrefs with
      | Some refs -> List.map (fun r -> "order_userref", `List [ `Int r ]) refs
      | None -> []
    in
    from_order_ids @ from_cl_ord_ids @ from_userrefs
  in
  (* Execute all cancels in parallel: each cancel carries its own req_id, so response
     correlation is per-order; the shared-req_id desync concern only applies to a true
     batch cancel, which Kraken does not offer. The old sequential pipeline turned N
     cancels into N times the RTT on the domain's critical path, while parallel requests
     collapse it to roughly one RTT. Each failure is logged and the remaining cancels
     still complete. *)
  let cancel_single (key, value) =
    let cancel_once () =
      let req_id = next_req_id () in
      let params = `Assoc [ "token", `String token; key, value ] in
      Logging.debug_f ~section "Cancelling order: %s=%s" key (Yojson.Safe.to_string value);
      Kraken_trading_client.send_request
        ~symbol:None
        ~method_:"cancel_order"
        ~params
        ~req_id
        ~timeout_ms:order_request_timeout_ms
      >>= fun response ->
      if response.success
      then (
        match response.result with
        | Some result_json ->
          (try
             let order_id = Util.(member "order_id" result_json |> to_string) in
             let cl_ord_id = Util.(member "cl_ord_id" result_json |> to_string_option) in
             let result = { Kraken_common_types.order_id; cl_ord_id } in
             Lwt.return (Ok result)
           with
           | exn ->
             let err =
               Printf.sprintf
                 "Failed to parse cancel response: %s"
                 (Printexc.to_string exn)
             in
             Logging.error_f ~section "%s" err;
             Lwt.return (Error err))
        | None ->
          let err = "No result in successful cancel response" in
          Logging.error ~section err;
          Lwt.return (Error err))
      else (
        let err =
          match response.error with
          | Some e -> e
          | None -> "Unknown error"
        in
        Logging.error_f ~section "Order cancellation failed: %s" err;
        Lwt.return (Error err))
    in
    Error_handling.retry_with_backoff ~section ~config ~f:cancel_once ()
  in
  Lwt_list.map_p cancel_single individual_cancels
  >|= fun results ->
  let results =
    List.filter_map
      (function
        | Ok r -> Some r
        | Error _ -> None)
      results
  in
  if results <> [] then Ok results else Error "Failed to cancel orders"
;;
