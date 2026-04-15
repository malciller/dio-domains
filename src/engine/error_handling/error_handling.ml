(** Centralized error handling for the Dio Domains trading system.

    Provides unified error classification, exponential backoff retry logic,
    Lwt exception wrappers, and string utilities. Replaces the duplicated
    error handling patterns previously scattered across kraken_actions,
    hyperliquid_actions, lighter_actions, and order_executor. *)

open Lwt.Infix

let section = "error_handling"

(* ---- String utilities ---- *)

(** Returns [true] if [needle] occurs anywhere within [haystack].
    Case-sensitive linear scan. *)
let string_contains (haystack : string) (needle : string) : bool =
  let haystack_len = String.length haystack in
  let needle_len = String.length needle in
  if needle_len > haystack_len then false
  else
    let rec loop i =
      if i + needle_len > haystack_len then false
      else if String.sub haystack i needle_len = needle then true
      else loop (i + 1)
    in
    loop 0

(* ---- Error classification ---- *)

(** Semantic categories for errors encountered during trading operations.
    Used to determine retry eligibility and logging severity. *)
type error_kind =
  | Connection     (** Transport-level: closed socket, TLS, EOF *)
  | Timeout        (** Request or operation timed out *)
  | RateLimit      (** Exchange rate limit hit *)
  | ServerError    (** HTTP 5xx or exchange-side transient failure *)
  | InvalidRequest (** Validation failure, bad parameters *)
  | OrderRejected  (** Exchange rejected the order *)
  | ParseError     (** Failed to parse response *)
  | Unknown        (** Unclassified error *)

(** Classifies an error message string into an [error_kind].
    Subsumes all per-module [is_retriable_error], [is_connection_error],
    and ad-hoc substring checks. *)
let classify (err : string) : error_kind =
  let e = String.lowercase_ascii err in
  (* Connection / transport errors *)
  if string_contains e "closed socket"
     || string_contains e "channel_closed"
     || string_contains e "tls:"
     || string_contains e "end_of_file"
     || string_contains e "connection"
     || string_contains e "network"
     || string_contains e "reset"
     || string_contains e "broken pipe"
     || string_contains e "websocket"
     || string_contains e "socket"
  then Connection
  (* Timeout *)
  else if string_contains e "timeout"
  then Timeout
  (* Rate limiting *)
  else if string_contains e "rate limit"
          || string_contains e "too many requests"
          || string_contains e "too many cumulative requests"
  then RateLimit
  (* HTTP 5xx server errors *)
  else if string_contains e "500"
          || string_contains e "502"
          || string_contains e "503"
          || string_contains e "504"
  then ServerError
  (* Parse errors *)
  else if string_contains e "failed to parse"
  then ParseError
  (* Order rejections *)
  else if string_contains e "rejected"
  then OrderRejected
  (* Fallback *)
  else Unknown

(** Returns [true] for error kinds that are worth retrying:
    transient transport failures, timeouts, rate limits, and server errors. *)
let is_retriable (kind : error_kind) : bool =
  match kind with
  | Connection | Timeout | RateLimit | ServerError -> true
  | InvalidRequest | OrderRejected | ParseError | Unknown -> false

(** Convenience: classifies an error string and checks retriability. *)
let is_retriable_error (err : string) : bool =
  is_retriable (classify err)

(* ---- Retry configuration ---- *)

(** Parameters controlling exponential backoff retry behavior.
    Canonical definition — re-exported by [Exchange_intf.Types]. *)
type retry_config = {
  max_attempts: int;     (** Maximum number of attempts (including the initial). *)
  base_delay_ms: float;  (** Initial delay between retries, in milliseconds. *)
  max_delay_ms: float;   (** Upper bound on delay between retries, in milliseconds. *)
  backoff_factor: float; (** Multiplicative factor applied to the delay after each attempt. *)
}

let default_retry_config = {
  max_attempts = 3;
  base_delay_ms = 1000.0;
  max_delay_ms = 30000.0;
  backoff_factor = 2.0;
}

(** Lwt sleep for the given duration in milliseconds. *)
let sleep_ms ms = Lwt_unix.sleep (ms /. 1000.0)

(** Retries [f] with exponential backoff according to [config].
    Stops retrying when [max_attempts] is reached or the error is not retriable.

    @param section   Logging section name for retry warnings.
    @param config    Backoff parameters.
    @param f         The operation to attempt.
    @param is_retriable_override
      Optional override for retriability classification. When provided,
      this function is used instead of the default [is_retriable_error].
      Exchange-specific callers can extend the default logic here. *)
let retry_with_backoff ~section:(log_section : string) ~config ~f
    ?(is_retriable_override : (string -> bool) option) () =
  let check_retriable = match is_retriable_override with
    | Some fn -> fn
    | None -> is_retriable_error
  in
  let rec attempt attempt_num =
    if attempt_num > config.max_attempts then
      Lwt.fail_with (Printf.sprintf "Max retry attempts (%d) exceeded" config.max_attempts)
    else begin
      f () >>= fun result ->
      match result with
      | Ok _ as success -> Lwt.return success
      | Error err ->
          if attempt_num >= config.max_attempts || not (check_retriable err) then
            Lwt.return (Error err)
          else begin
            let delay = min config.max_delay_ms
              (config.base_delay_ms *. (config.backoff_factor ** float_of_int (attempt_num - 1))) in
            Logging.warn_f ~section:log_section
              "Attempt %d failed: %s. Retrying in %.0fms..." attempt_num err delay;
            sleep_ms delay >>= fun () ->
            attempt (attempt_num + 1)
          end
    end
  in
  attempt 1

(* ---- Lwt exception wrappers ---- *)

(** Wraps [f ()] in [Lwt.catch], logs exceptions at ERROR level with
    [section] and [operation] context, and returns [Error msg].
    Eliminates the repeated [Lwt.catch + Printexc.to_string + Logging.error_f]
    boilerplate found across ~30 call sites. *)
let catch_and_log ~section:(log_section : string) ~operation f =
  Lwt.catch
    (fun () -> f () >>= fun result -> Lwt.return (Ok result))
    (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.error_f ~section:log_section "[%s] %s failed: %s" log_section operation error_msg;
      Lwt.return (Error error_msg))

(** Like [catch_and_log] but returns [unit], swallowing the error after
    logging. For fire-and-forget operations like reconnection attempts
    and background cleanup tasks. *)
let catch_and_return_unit ~section:(log_section : string) ~operation f =
  Lwt.catch f
    (fun exn ->
      Logging.error_f ~section:log_section "[%s] %s failed: %s"
        log_section operation (Printexc.to_string exn);
      Lwt.return_unit)

(** Catches exceptions, logs them, then runs [recovery].
    Replaces the repeated reconnect-on-failure pattern where the catch
    block logs an error and immediately schedules a recovery action. *)
let catch_with_recovery ~section:(log_section : string) ~operation ~recovery f =
  Lwt.catch f
    (fun exn ->
      let error_msg = Printexc.to_string exn in
      Logging.error_f ~section:log_section "[%s] %s failed: %s"
        log_section operation error_msg;
      recovery exn)
