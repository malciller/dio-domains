(**
   Kraken trading action utilities.
   Provides helper functions for order parameter formatting, precision truncation, and payload construction.
*)
open Lwt.Infix
open Yojson.Safe

(** Appends key value tuple to JSON association structure. *)
let add_to_assoc (json : Yojson.Safe.t) (key_value_pair : string * Yojson.Safe.t) : Yojson.Safe.t =
  match json with
  | `Assoc assoc_list ->
      let (key, value) = key_value_pair in
      `Assoc ((key, value) :: assoc_list)
  | _ -> failwith "add_to_assoc: expected JSON association object"

let section = "kraken_actions"

(** Clamps float value to instrument decimal precision limitations. *)
let truncate_price_to_precision price symbol =
  match Kraken_instruments_feed.get_precision_info symbol with
  | Some (price_precision, qty_precision) ->
      Logging.debug_f ~section "Truncating price %.8f for %s with precision %d (qty_precision: %d)" price symbol price_precision qty_precision;
      (* Rounds to absolute precision preventing truncation. *)
      let multiplier = 10.0 ** float_of_int price_precision in
      let truncated_price = Float.round (price *. multiplier) /. multiplier in
      (* Reparses to eliminate floating point artifacts. *)
      let formatted_str = Printf.sprintf "%.*f" price_precision truncated_price in
      let clean_price = Float.of_string formatted_str in
      Logging.debug_f ~section "Rounded price %.8f -> %s -> %.8f for %s" price formatted_str clean_price symbol;
      clean_price
  | None ->
      Logging.warn_f ~section "No price precision info for %s, using original price" symbol;
      price

(** Evaluates string for substring presence.
    Delegates to centralized [Error_handling.string_contains]. *)
let string_contains = Error_handling.string_contains

(** Serializes JSON applying instrument specific float precision bounds. *)
let rec json_to_string_precise ?field_name symbol (json : Yojson.Safe.t) : string =
  match json with
  | `Null -> "null"
  | `Bool b -> if b then "true" else "false"
  | `Int i -> string_of_int i
  | `Float f ->
      (* Selects precision bounds using field context. *)
      let precision = match symbol with
        | Some sym ->
            (match Kraken_instruments_feed.get_precision_info sym with
             | Some (price_precision, qty_precision) ->
                 (* Routes price limits to price parameters and quantity limits otherwise. *)
                 (match field_name with
                  | Some name when string_contains name "price" -> price_precision
                  | _ -> qty_precision)
             | None -> 8)
        | None -> 8
      in
      Printf.sprintf "%.*f" precision f
  | `String s ->
      (* Escapes JSON string literal values. *)
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

(** Generates sequential request identifier. *)
let next_req_id =
  let counter = ref 0 in
  let ping_start_id = 1000000 in  (* Denotes ping ID reserved boundary. *)
  fun () ->
    incr counter;
    let id = !counter in
    (* Logs threshold warning approaching ping ID collision boundary. *)
    if id >= ping_start_id - 1000 then
      Logging.warn_f ~section "Trading request ID %d approaching ping ID range (ping IDs start at %d) - potential collision risk" id ping_start_id;
    id

(** Retry configuration — re-exported from centralized [Error_handling]. *)
type retry_config = Error_handling.retry_config = {
  max_attempts: int;
  base_delay_ms: float;
  max_delay_ms: float;
  backoff_factor: float;
}

let default_retry_config = Error_handling.default_retry_config

(** Classifies error responses targeting transient network conditions.
    Delegates to centralized [Error_handling.is_retriable_error]. *)
let is_retriable_error = Error_handling.is_retriable_error

(** Transmits order placement payload utilizing retry handler. *)
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
    (* Submits via REST enforcing sequence isolation constraints. *)
    let req_id = next_req_id () in

    (* Appends core parameters. *)
    let params = `Assoc [
      ("order_type", `String order_type);
      ("side", `String side);
      ("order_qty", `Float order_qty);
      ("symbol", `String symbol);
      ("token", `String token);
    ] in

    (* Appends optional parameters. *)
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

    (* Appends conditional trigger parameters. *)
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

    (* Appends execution display limits. *)
    let params = match display_qty with
      | Some qty -> add_to_assoc params ("display_qty", `Float qty)
      | None -> params
    in

    (* Appends fee structure preference. *)
    let params = match fee_preference with
      | Some pref -> add_to_assoc params ("fee_preference", `String pref)
      | None -> params
    in

    (* Appends execution validation flag. *)
    let params = match validate with
      | Some v -> add_to_assoc params ("validate", `Bool v)
      | None -> params
    in

    Logging.debug_f ~section "Placing %s order: %s %s %f @ %s" side symbol order_type order_qty
      (match limit_price with Some p -> Printf.sprintf "%.6f" (truncate_price_to_precision p symbol) | None -> "market");

    (* Logs serialized order payload. *)
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

  Error_handling.retry_with_backoff ~section ~config ~f:place_order_once ()

(** Transmits order modification payload utilizing retry handler. *)
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
    (* Assumes initialized singleton dependency. *)
    let req_id = next_req_id () in

    let symbol_str = Option.value symbol ~default:"" in
    if symbol_str = "" then failwith "Symbol required for price precision in amend_order";  (* Validates symbol configuration. *)

    (* Appends immutable parameters. *)
    let params = `Assoc [
      ("order_id", `String order_id);
      ("token", `String token);
    ] in

    (* Appends dynamic quantity parameter. *)
    let params = match order_qty with
      | Some qty -> add_to_assoc params ("order_qty", `Float qty)
      | None -> params
    in

    (* Appends optional parameters. *)
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

    (* Appends expiration deadline parameter. *)
    let params = match deadline with
      | Some deadline -> add_to_assoc params ("deadline", `String deadline)
      | None -> params
    in

    (* Appends execution validation flag. *)
    let params = match validate with
      | Some v -> add_to_assoc params ("validate", `Bool v)
      | None -> params
    in

    (* Appends asset symbol mapping. *)
    let params = match symbol with
      | Some s -> add_to_assoc params ("symbol", `String s)
      | None -> params
    in

    Logging.debug_f ~section "Amending order %s: %s%s%s" order_id
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
      if string_contains err "EOrder:No amendable parameters specified" then begin
        Logging.debug_f ~section "Order amendment returned no changes for %s, treating as skipped" order_id;
        Lwt.return (Ok {
          Kraken_common_types.amend_id = "skipped_no_change";
          order_id = order_id;
          cl_ord_id = cl_ord_id;
        })
      end else begin
        Logging.error_f ~section "Order amendment failed: %s" err;
        Lwt.return (Error err)
      end
    end
  in

  Error_handling.retry_with_backoff ~section ~config ~f:amend_order_once ()

(** Transmits order cancellation payload utilizing retry handler. *)
let cancel_orders
    ~token
    ?order_ids
    ?cl_ord_ids
    ?order_userrefs
    ?retry_config
    () : ((Kraken_common_types.cancel_order_result list, string) result) Lwt.t =

  let config = match retry_config with Some c -> c | None -> default_retry_config in

  let cancel_orders_once () =
    (* Assumes initialized singleton dependency. *)
    let req_id = next_req_id () in

    (* Initializes base cancel parameters. *)
    let params = `Assoc [("token", `String token)] in

    (* Appends target reference identifiers. *)
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

    Logging.debug_f ~section "Cancelling orders: %s%s%s"
      (match order_ids with Some ids -> Printf.sprintf "order_ids=[%s]" (String.concat "," ids) | None -> "")
      (match cl_ord_ids with Some ids -> Printf.sprintf "cl_ord_ids=[%s]" (String.concat "," ids) | None -> "")
      (match order_userrefs with Some refs -> Printf.sprintf "userrefs=[%s]" (String.concat "," (List.map string_of_int refs)) | None -> "");

    Kraken_trading_client.send_request
      ~symbol:None
      ~method_:"cancel_order"
      ~params
      ~req_id
      ~timeout_ms:10000 >>= fun response ->

    (* Processes individual sequential cancel responses. *)
    (* Modifies multi order payload processing behavior routing sequence limits. *)

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
            (* Yields singleton list bypassing streaming payload structures. *)
            Lwt.return (Ok [result])
          with exn ->
            let err = Printf.sprintf "Failed to parse cancel response: %s" (Printexc.to_string exn) in
            Logging.error_f ~section "%s" err;
            Lwt.return (Error err)
          end
      | None ->
          (* Validates successful response body allocation. *)
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

  Error_handling.retry_with_backoff ~section ~config ~f:cancel_orders_once ()