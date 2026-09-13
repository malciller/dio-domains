(** Centralized error handling for the Dio Domains trading system.

    Provides unified error classification, exponential backoff retry, Lwt
    exception wrappers, and string utilities. *)

open Lwt.Infix

let section = "error_handling"

(* ---- String utilities ---- *)

(** [true] if [needle] occurs in [haystack]. Case-sensitive linear scan. *)
let string_contains (haystack : string) (needle : string) : bool =
  let haystack_len = String.length haystack in
  let needle_len = String.length needle in
  if needle_len > haystack_len
  then false
  else (
    let rec loop i =
      if i + needle_len > haystack_len
      then false
      else if String.sub haystack i needle_len = needle
      then true
      else loop (i + 1)
    in
    loop 0)
;;

(* ---- Error classification ---- *)

(** Semantic error categories, used to determine retry eligibility and logging
    severity. *)
type error_kind =
  | Connection (** Transport-level: closed socket, TLS, EOF *)
  | Timeout (** Request or operation timed out *)
  | RateLimit (** Exchange rate limit hit *)
  | ServerError (** HTTP 5xx or exchange-side transient failure *)
  | InvalidRequest (** Validation failure, bad parameters *)
  | OrderRejected (** Exchange rejected the order *)
  | ParseError (** Failed to parse response *)
  | Unknown (** Unclassified error *)

(** Classify an error message string into an [error_kind]. *)
let classify (err : string) : error_kind =
  let e = String.lowercase_ascii err in
  (* Connection / transport errors *)
  if
    string_contains e "closed socket"
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
  else if string_contains e "timeout"
  then Timeout
  else if
    string_contains e "rate limit"
    || string_contains e "too many requests"
    || string_contains e "too many cumulative requests"
  then RateLimit
  else if
    string_contains e "500"
    || string_contains e "502"
    || string_contains e "503"
    || string_contains e "504"
  then ServerError
  else if string_contains e "failed to parse"
  then ParseError
  else if string_contains e "rejected"
  then OrderRejected
  (* Fallback *)
  else Unknown
;;

(** [true] for retryable kinds: transient transport failures, timeouts, rate
    limits, and server errors. *)
let is_retriable (kind : error_kind) : bool =
  match kind with
  | Connection | Timeout | RateLimit | ServerError -> true
  | InvalidRequest | OrderRejected | ParseError | Unknown -> false
;;

(** Convenience: classifies an error string and checks retriability. *)
let is_retriable_error (err : string) : bool = is_retriable (classify err)

(* ---- Retry configuration ---- *)

(** Exponential backoff retry parameters. Canonical definition, re-exported by
    [Exchange_intf.Types]. *)
type retry_config =
  { max_attempts : int (** Maximum number of attempts (including the initial). *)
  ; base_delay_ms : float (** Initial delay between retries, in milliseconds. *)
  ; max_delay_ms : float (** Upper bound on delay between retries, in milliseconds. *)
  ; backoff_factor : float
    (** Multiplicative factor applied to the delay after each attempt. *)
  }

let default_retry_config =
  { max_attempts = 3
  ; base_delay_ms = 1000.0
  ; max_delay_ms = 30000.0
  ; backoff_factor = 2.0
  }
;;

(** Lwt sleep for the given duration in milliseconds. *)
let sleep_ms ms = Lwt_unix.sleep (ms /. 1000.0)

(** Retry [f] with exponential backoff per [config], stopping when
    [max_attempts] is reached or the error is not retriable.

    @param section   Logging section for retry warnings.
    @param config    Backoff parameters.
    @param f         Operation to attempt.
    @param is_retriable_override Optional retriability classifier; defaults to
      [is_retriable_error]. Callers can extend the default logic here. *)
let retry_with_backoff
      ~section:(log_section : string)
      ~config
      ~f
      ?(is_retriable_override : (string -> bool) option)
      ()
  =
  let check_retriable =
    match is_retriable_override with
    | Some fn -> fn
    | None -> is_retriable_error
  in
  let rec attempt attempt_num =
    if attempt_num > config.max_attempts
    then
      Lwt.fail_with
        (Printf.sprintf "Max retry attempts (%d) exceeded" config.max_attempts)
    else
      f ()
      >>= fun result ->
      match result with
      | Ok _ as success -> Lwt.return success
      | Error err ->
        if attempt_num >= config.max_attempts || not (check_retriable err)
        then Lwt.return (Error err)
        else (
          let delay =
            min
              config.max_delay_ms
              (config.base_delay_ms
               *. (config.backoff_factor ** float_of_int (attempt_num - 1)))
          in
          Logging.warn_f
            ~section:log_section
            "Attempt %d failed: %s. Retrying in %.0fms..."
            attempt_num
            err
            delay;
          sleep_ms delay >>= fun () -> attempt (attempt_num + 1))
  in
  attempt 1
;;

(* ---- Lwt exception wrappers ---- *)

(** Wrap [f ()] in [Lwt.catch], log exceptions at ERROR with [section] and
    [operation] context, and return [Error msg]. *)
let catch_and_log ~section:(log_section : string) ~operation f =
  Lwt.catch
    (fun () -> f () >>= fun result -> Lwt.return (Ok result))
    (fun exn ->
       let error_msg = Printexc.to_string exn in
       Logging.error_f
         ~section:log_section
         "[%s] %s failed: %s"
         log_section
         operation
         error_msg;
       Lwt.return (Error error_msg))
;;

(** Like [catch_and_log] but returns [unit], swallowing the error after logging.
    For fire-and-forget operations such as reconnection attempts and background
    cleanup. *)
let catch_and_return_unit ~section:(log_section : string) ~operation f =
  Lwt.catch f (fun exn ->
    Logging.error_f
      ~section:log_section
      "[%s] %s failed: %s"
      log_section
      operation
      (Printexc.to_string exn);
    Lwt.return_unit)
;;

(** Catch exceptions, log them, then run [recovery]. For reconnect-on-failure
    and similar recovery patterns. *)
let catch_with_recovery ~section:(log_section : string) ~operation ~recovery f =
  Lwt.catch f (fun exn ->
    let error_msg = Printexc.to_string exn in
    Logging.error_f
      ~section:log_section
      "[%s] %s failed: %s"
      log_section
      operation
      error_msg;
    recovery exn)
;;
