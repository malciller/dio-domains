open Lwt.Infix
open Yojson.Safe

(** Helper function to add key-value pair to JSON association object *)
let add_to_assoc (json : Yojson.Safe.t) (key_value_pair : string * Yojson.Safe.t) : Yojson.Safe.t =
  match json with
  | `Assoc assoc_list ->
      let (key, value) = key_value_pair in
      `Assoc ((key, value) :: assoc_list)
  | _ -> failwith "add_to_assoc: expected JSON association object"

let section = "kraken_actions"

(** Truncate price to exact decimal precision and return as properly-rounded float *)
let truncate_price_to_precision price symbol =
  match Kraken_instruments_feed.get_precision_info symbol with
  | Some (price_precision, qty_precision) ->
      Logging.debug_f ~section "Truncating price %.8f for %s with precision %d (qty_precision: %d)" price symbol price_precision qty_precision;
      (* Truncate mathematically to exact decimal precision *)
      let multiplier = 10.0 ** float_of_int price_precision in
      let truncated_price = floor (price *. multiplier) /. multiplier in
      (* Force re-parsing through string to eliminate FP artifacts *)
      let formatted_str = Printf.sprintf "%.*f" price_precision truncated_price in
      let clean_price = Float.of_string formatted_str in
      Logging.debug_f ~section "Truncated price %.8f -> %s -> %.8f for %s" price formatted_str clean_price symbol;
      clean_price
  | None ->
      Logging.warn_f ~section "No price precision info for %s, using original price" symbol;
      price

(** Helper function to check if string contains substring *)
let string_contains (str : string) (substr : string) : bool =
  let str_len = String.length str in
  let substr_len = String.length substr in
  if substr_len > str_len then false
  else
    let rec loop i =
      if i + substr_len > str_len then false
      else if String.sub str i substr_len = substr then true
      else loop (i + 1)
    in
    loop 0

(** Custom JSON encoder that uses symbol-specific precision for floats based on field type *)
let rec json_to_string_precise ?field_name symbol (json : Yojson.Safe.t) : string =
  match json with
  | `Null -> "null"
  | `Bool b -> if b then "true" else "false"
  | `Int i -> string_of_int i
  | `Float f ->
      (* Use appropriate precision based on field name and symbol *)
      let precision = match symbol with
        | Some sym ->
            (match Kraken_instruments_feed.get_precision_info sym with
             | Some (price_precision, qty_precision) ->
                 (* Use price precision for price-related fields, qty precision for others *)
                 (match field_name with
                  | Some name when string_contains name "price" -> price_precision
                  | _ -> qty_precision)
             | None -> 8)
        | None -> 8
      in
      Printf.sprintf "%.*f" precision f
  | `String s ->
      (* Properly escape JSON strings *)
      "\"" ^ (String.concat "" (List.map (function
        | '"' -> "\\\""
        | '\\' -> "\\\\"
        | '\b' -> "\\b"
        | '\012' -> "\\f"
        | '\n' -> "\\n"
        | '\r' -> "\\r"
        | '\t' -> "\\t"
        | c -> String.make 1 c
      ) (List.init (String.length s) (String.get s)))) ^ "\""
  | `List items ->
      "[" ^ (String.concat "," (List.map (json_to_string_precise ?field_name symbol) items)) ^ "]"
  | `Assoc pairs ->
      "{" ^ (String.concat "," (List.map (fun (k, v) ->
        "\"" ^ k ^ "\":" ^ json_to_string_precise ~field_name:k symbol v
      ) pairs)) ^ "}"
  | `Intlit s -> s
  | `Tuple _ -> failwith "json_to_string_precise: Tuple not supported"
  | `Variant _ -> failwith "json_to_string_precise: Variant not supported"

(** Generate unique request ID *)
let next_req_id =
  let counter = ref 0 in
  let ping_start_id = 1000000 in  (* Ping IDs start at this value *)
  fun () ->
    incr counter;
    let id = !counter in
    (* Warn if approaching ping ID range to detect potential collisions *)
    if id >= ping_start_id - 1000 then
      Logging.warn_f ~section "Trading request ID %d approaching ping ID range (ping IDs start at %d) - potential collision risk" id ping_start_id;
    id

(** Retry configuration *)
type retry_config = {
  max_attempts: int;
  base_delay_ms: float;
  max_delay_ms: float;
  backoff_factor: float;
}

let default_retry_config = {
  max_attempts = 3;
  base_delay_ms = 1000.0;
  max_delay_ms = 30000.0;
  backoff_factor = 2.0;
}

(** Sleep for the specified milliseconds *)
let sleep_ms ms = Lwt_unix.sleep (ms /. 1000.0)

(** Retry a function with exponential backoff *)
let retry_with_backoff ~config ~f ~is_retriable =
  let rec attempt attempt_num =
    if attempt_num > config.max_attempts then
      Lwt.fail_with (Printf.sprintf "Max retry attempts (%d) exceeded" config.max_attempts)
    else begin
      f () >>= fun result ->
      match result with
      | Ok _ as success -> Lwt.return success
      | Error err ->
          if attempt_num >= config.max_attempts || not (is_retriable err) then
            Lwt.return (Error err)
          else begin
            let delay = min config.max_delay_ms (config.base_delay_ms *. (config.backoff_factor ** float_of_int (attempt_num - 1))) in
            Logging.warn_f ~section "Attempt %d failed: %s. Retrying in %.0fms..." attempt_num err delay;
            sleep_ms delay >>= fun () ->
            attempt (attempt_num + 1)
          end
    end
  in
  attempt 1


(** Check if an error is retriable (network issues, temporary server errors) *)
let is_retriable_error err =
  let err_lower = String.lowercase_ascii err in
  (* Network and connectivity errors *)
  string_contains err_lower "timeout" ||
  string_contains err_lower "connection" ||
  string_contains err_lower "network" ||
  string_contains err_lower "reset" ||
  string_contains err_lower "broken pipe" ||
  (* Server errors that might be temporary *)
  string_contains err_lower "500" ||
  string_contains err_lower "502" ||
  string_contains err_lower "503" ||
  string_contains err_lower "504" ||
  (* WebSocket connection issues *)
  string_contains err_lower "websocket" ||
  string_contains err_lower "socket" ||
  (* Rate limiting (though Kraken might use different codes) *)
  string_contains err_lower "rate limit" ||
  string_contains err_lower "too many requests"

(** Place a new order on Kraken with retry logic *)
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
    () : (Kraken_common_types.add_order_result, string) result Lwt.t =

  let config = match retry_config with Some c -> c | None -> default_retry_config in

  let place_order_once () =
    (* Use REST API instead of WebSocket to avoid concurrency issues *)
    let req_id = next_req_id () in

    (* Build order parameters *)
    let params = `Assoc [
      ("order_type", `String order_type);
      ("side", `String side);
      ("order_qty", `Float order_qty);
      ("symbol", `String symbol);
      ("token", `String token);
    ] in

    (* Add optional parameters *)
    let params = match limit_price with
      | Some price -> 
          let truncated_price = truncate_price_to_precision price symbol in
          add_to_assoc params ("limit_price", `Float truncated_price)
      | None -> params
    in
    let params = match time_in_force with
      | Some tif -> add_to_assoc params ("time_in_force", `String tif)
      | None -> params
    in
    let params = match post_only with
      | Some po -> add_to_assoc params ("post_only", `Bool po)
      | None -> params
    in
    let params = match margin with
      | Some m -> add_to_assoc params ("margin", `Bool m)
      | None -> params
    in
    let params = match reduce_only with
      | Some ro -> add_to_assoc params ("reduce_only", `Bool ro)
      | None -> params
    in
    let params = match order_userref with
      | Some userref -> add_to_assoc params ("order_userref", `Int userref)
      | None -> params
    in
    let params = match cl_ord_id with
      | Some id -> add_to_assoc params ("cl_ord_id", `String id)
      | None -> params
    in

    (* Add trigger parameters for conditional orders *)
    let params = match trigger_price with
      | Some price ->
          let truncated_price = truncate_price_to_precision price symbol in
          let triggers = `Assoc [("price", `Float truncated_price)] in
          let triggers = match trigger_price_type with
            | Some tpt -> add_to_assoc triggers ("price_type", `String tpt)
            | None -> triggers
          in
          add_to_assoc params ("triggers", triggers)
      | None -> params
    in

    (* Add display quantity for iceberg orders *)
    let params = match display_qty with
      | Some qty -> add_to_assoc params ("display_qty", `Float qty)
      | None -> params
    in

    (* Add fee preference *)
    let params = match fee_preference with
      | Some pref -> add_to_assoc params ("fee_preference", `String pref)
      | None -> params
    in

    (* Add validate parameter for testing without execution *)
    let params = match validate with
      | Some v -> add_to_assoc params ("validate", `Bool v)
      | None -> params
    in

    Logging.info_f ~section "Placing %s order: %s %s %f @ %s" side symbol order_type order_qty
      (match limit_price with Some p -> Printf.sprintf "%.6f" (truncate_price_to_precision p symbol) | None -> "market");

    (* Log the order parameters being sent *)
    let json_str = json_to_string_precise (Some symbol) params in
    Logging.debug_f ~section "Order parameters: %s" json_str;

    Kraken_trading_client.send_request
      ~symbol:(Some symbol)
      ~method_:"add_order"
      ~params
      ~req_id
      ~timeout_ms:10000 >>= fun response ->

    if response.success then begin
      match response.result with
      | Some result_json ->
          begin try
            let order_id = Util.(member "order_id" result_json |> to_string) in
            let cl_ord_id = Util.(member "cl_ord_id" result_json |> to_string_option) in
            let order_userref = Util.(member "order_userref" result_json |> to_int_option) in
            Lwt.return (Ok {
              Kraken_common_types.order_id;
              cl_ord_id;
              order_userref;
            })
          with exn ->
            let err = Printf.sprintf "Failed to parse order response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s" err;
            Lwt.return (Error err)
          end
      | None ->
          let err = "No result in successful response" in
          Logging.error ~section err;
          Lwt.return (Error err)
    end else begin
      let err = match response.error with
        | Some e -> e
        | None -> "Unknown error"
      in
      Logging.error_f ~section "Order placement failed: %s" err;
      Lwt.return (Error err)
    end
  in

  retry_with_backoff ~config ~f:place_order_once ~is_retriable:is_retriable_error

(** Amend an existing order on Kraken with retry logic *)
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
    () : (Kraken_common_types.amend_order_result, string) result Lwt.t =

  let config = match retry_config with Some c -> c | None -> default_retry_config in

  let amend_order_once () =
    (* Trading client should already be initialized by order executor *)
    let req_id = next_req_id () in

    let symbol_str = Option.value symbol ~default:"" in
    if symbol_str = "" then failwith "Symbol required for price precision in amend_order";  (* Enforce it *)

    (* Build amend parameters *)
    let params = `Assoc [
      ("order_id", `String order_id);
      ("token", `String token);
    ] in

    (* Add order_qty only if provided *)
    let params = match order_qty with
      | Some qty -> add_to_assoc params ("order_qty", `Float qty)
      | None -> params
    in

    (* Add optional parameters *)
    let params = match cl_ord_id with
      | Some id -> add_to_assoc params ("cl_ord_id", `String id)
      | None -> params
    in
    let params = match limit_price with
      | Some price -> 
          let truncated_price = truncate_price_to_precision price symbol_str in
          add_to_assoc params ("limit_price", `Float truncated_price)
      | None -> params
    in
    let params = match limit_price_type with
      | Some lpt -> add_to_assoc params ("limit_price_type", `String lpt)
      | None -> params
    in
    let params = match post_only with
      | Some po -> add_to_assoc params ("post_only", `Bool po)
      | None -> params
    in
    let params = match trigger_price with
      | Some price -> 
          let truncated_price = truncate_price_to_precision price symbol_str in
          add_to_assoc params ("trigger_price", `Float truncated_price)
      | None -> params
    in
    let params = match trigger_price_type with
      | Some tpt -> add_to_assoc params ("trigger_price_type", `String tpt)
      | None -> params
    in
    let params = match display_qty with
      | Some qty -> add_to_assoc params ("display_qty", `Float qty)
      | None -> params
    in

    (* Add deadline parameter for latency-sensitive orders *)
    let params = match deadline with
      | Some deadline -> add_to_assoc params ("deadline", `String deadline)
      | None -> params
    in

    (* Add validate parameter for testing without execution *)
    let params = match validate with
      | Some v -> add_to_assoc params ("validate", `Bool v)
      | None -> params
    in

    (* Add symbol parameter for non-crypto pairs *)
    let params = match symbol with
      | Some s -> add_to_assoc params ("symbol", `String s)
      | None -> params
    in

    Logging.info_f ~section "Amending order %s: %s%s%s" order_id
      (match order_qty with Some qty -> Printf.sprintf "qty=%.8f" qty | None -> "")
      (match limit_price with Some p -> Printf.sprintf ", price=%.6f" (truncate_price_to_precision p symbol_str) | None -> "")
      (match deadline with Some d -> Printf.sprintf ", deadline=%s" d | None -> "");


    Kraken_trading_client.send_request
      ~symbol:(Some symbol_str)
      ~method_:"amend_order"
      ~params
      ~req_id
      ~timeout_ms:10000 >>= fun response ->

    if response.success then begin
      match response.result with
      | Some result_json ->
          begin try
            let amend_id = Util.(member "amend_id" result_json |> to_string) in
            let order_id = Util.(member "order_id" result_json |> to_string) in
            let cl_ord_id = Util.(member "cl_ord_id" result_json |> to_string_option) in
            Lwt.return (Ok {
              Kraken_common_types.amend_id;
              order_id;
              cl_ord_id;
            })
          with exn ->
            let err = Printf.sprintf "Failed to parse amend response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s" err;
            Lwt.return (Error err)
          end
      | None ->
          let err = "No result in successful amend response" in
          Logging.error ~section err;
          Lwt.return (Error err)
    end else begin
      let err = match response.error with
        | Some e -> e
        | None -> "Unknown error"
      in
      Logging.error_f ~section "Order amendment failed: %s" err;
      Lwt.return (Error err)
    end
  in

  retry_with_backoff ~config ~f:amend_order_once ~is_retriable:is_retriable_error

(** Cancel orders on Kraken with retry logic *)
let cancel_orders
    ~token
    ?order_ids
    ?cl_ord_ids
    ?order_userrefs
    ?retry_config
    () : ((Kraken_common_types.cancel_order_result list, string) result) Lwt.t =

  let config = match retry_config with Some c -> c | None -> default_retry_config in

  let cancel_orders_once () =
    (* Trading client should already be initialized by order executor *)
    let req_id = next_req_id () in

    (* Build cancel parameters *)
    let params = `Assoc [("token", `String token)] in

    (* Add order identifiers *)
    let params = match order_ids with
      | Some ids -> add_to_assoc params ("order_id", `List (List.map (fun id -> `String id) ids))
      | None -> params
    in
    let params = match cl_ord_ids with
      | Some ids -> add_to_assoc params ("cl_ord_id", `List (List.map (fun id -> `String id) ids))
      | None -> params
    in
    let params = match order_userrefs with
      | Some refs -> add_to_assoc params ("order_userref", `List (List.map (fun ref -> `Int ref) refs))
      | None -> params
    in

    Logging.info_f ~section "Cancelling orders: %s%s%s"
      (match order_ids with Some ids -> Printf.sprintf "order_ids=[%s]" (String.concat "," ids) | None -> "")
      (match cl_ord_ids with Some ids -> Printf.sprintf "cl_ord_ids=[%s]" (String.concat "," ids) | None -> "")
      (match order_userrefs with Some refs -> Printf.sprintf "userrefs=[%s]" (String.concat "," (List.map string_of_int refs)) | None -> "");

    Kraken_trading_client.send_request
      ~symbol:None
      ~method_:"cancel_order"
      ~params
      ~req_id
      ~timeout_ms:10000 >>= fun response ->

    (* For cancel operations, Kraken returns individual responses for each cancelled order *)
    (* The current response contains one result, but we need to handle the fact that
       multiple cancel operations might be processed as separate responses *)

    if response.success then begin
      match response.result with
      | Some result_json ->
          begin try
            let order_id = Util.(member "order_id" result_json |> to_string) in
            let cl_ord_id = Util.(member "cl_ord_id" result_json |> to_string_option) in
            let result = {
              Kraken_common_types.order_id;
              cl_ord_id;
            } in
            (* For now, return a list with single result. In a more sophisticated implementation,
               we might need to handle streaming responses differently *)
            Lwt.return (Ok [result])
          with exn ->
            let err = Printf.sprintf "Failed to parse cancel response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s" err;
            Lwt.return (Error err)
          end
      | None ->
          (* This shouldn't happen for successful cancel responses *)
          let err = "No result in successful cancel response" in
          Logging.error ~section err;
          Lwt.return (Error err)
    end else begin
      let err = match response.error with
        | Some e -> e
        | None -> "Unknown error"
      in
      Logging.error_f ~section "Order cancellation failed: %s" err;
      Lwt.return (Error err)
    end
  in

  retry_with_backoff ~config ~f:cancel_orders_once ~is_retriable:is_retriable_error