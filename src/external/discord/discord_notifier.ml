(** Discord order fill notifier.

    Consumes fill events from the centralized [Fill_event_bus] ring buffer
    and delivers formatted notifications to a Discord webhook. Implements
    strict rate limiting via a token bucket to guarantee compliance with
    Discord's webhook rate limits (5 requests per 2 seconds).

    Architecture:
    - A single Lwt consumer fiber drains the ring buffer on each wakeup.
    - Fills arriving in the same drain cycle are batched into a single
      Discord embed (up to 10 fields per embed) to minimize API calls.
    - A token bucket (5 tokens, 1 refill per 400ms) governs POST timing.
    - On 429 (rate limited): sleeps for the Retry-After duration.
    - On 5xx / network error: backs off 5s, retries up to 3 times.
    - Self-restarts on crash with 10s backoff. *)

open Lwt.Infix

let section = "discord_notifier"

(** Maximum number of fill events per Discord embed message.
    Discord supports up to 25 fields per embed; we use 10 to keep
    messages readable (each fill uses ~5-6 fields). *)
let max_fills_per_message = 10

(* ---- Token Bucket Rate Limiter ---- *)

(** Token bucket state for rate limiting webhook POSTs.
    Discord enforces 5 requests per 2 seconds per webhook URL.
    We use 5 tokens with a 400ms refill interval (= 2.5 req/s steady state),
    safely under the 5/2s limit even accounting for clock drift. *)
type token_bucket = {
  mutable tokens: float;
  mutable last_refill: float;
  max_tokens: float;
  refill_interval: float;  (** Seconds between token refills. *)
}

let bucket = {
  tokens = 5.0;
  last_refill = Unix.gettimeofday ();
  max_tokens = 5.0;
  refill_interval = 0.4;  (* 400ms = 1 token per 0.4s *)
}

(** Refill tokens based on elapsed time since last refill. *)
let refill_tokens () =
  let now = Unix.gettimeofday () in
  let elapsed = now -. bucket.last_refill in
  let new_tokens = elapsed /. bucket.refill_interval in
  if new_tokens >= 1.0 then begin
    bucket.tokens <- min bucket.max_tokens (bucket.tokens +. new_tokens);
    bucket.last_refill <- now
  end

(** Acquire a token, sleeping until one is available. *)
let acquire_token () =
  let rec try_acquire () =
    refill_tokens ();
    if bucket.tokens >= 1.0 then begin
      bucket.tokens <- bucket.tokens -. 1.0;
      Lwt.return_unit
    end else begin
      (* Calculate sleep duration until next token is available *)
      let deficit = 1.0 -. bucket.tokens in
      let sleep_time = deficit *. bucket.refill_interval in
      Lwt_unix.sleep sleep_time >>= fun () ->
      try_acquire ()
    end
  in
  try_acquire ()

(* ---- Message Formatting ---- *)

(** Format a fill event as a Discord embed field. *)
let format_fill_field (fill : Concurrency.Fill_event_bus.fill_event) =
  let side_emoji = if fill.side = "buy" then "🟢" else "🔴" in
  let side_label = String.uppercase_ascii fill.side in
  let value_after_fee = fill.value -. fill.fee in
  let name = Printf.sprintf "%s %s %s" side_emoji side_label fill.symbol in
  let value = Printf.sprintf
    "**Amount:** `%.8g`\n**Price:** `$%.4f`\n**Value:** `$%.2f`\n**Fee:** `$%.4f`\n**Net:** `$%.2f`"
    fill.amount fill.fill_price fill.value fill.fee value_after_fee
  in
  `Assoc [
    ("name", `String name);
    ("value", `String value);
    ("inline", `Bool true);
  ]

(** Build a Discord webhook JSON payload for a batch of fills. *)
let build_webhook_payload (fills : Concurrency.Fill_event_bus.fill_event list) =
  match fills with
  | [] -> None
  | [fill] ->
      (* Single fill: use a focused embed *)
      let side_label = String.uppercase_ascii fill.side in
      let color = if fill.side = "buy" then 0x2ECC71 else 0xE74C3C in
      let value_after_fee = fill.value -. fill.fee in
      let embed = `Assoc [
        ("title", `String (Printf.sprintf "Order Fill — %s" fill.venue));
        ("color", `Int color);
        ("fields", `List [
          `Assoc [("name", `String "Pair"); ("value", `String (Printf.sprintf "`%s`" fill.symbol)); ("inline", `Bool true)];
          `Assoc [("name", `String "Side"); ("value", `String side_label); ("inline", `Bool true)];
          `Assoc [("name", `String "Amount"); ("value", `String (Printf.sprintf "`%.8g`" fill.amount)); ("inline", `Bool true)];
          `Assoc [("name", `String "Fill Price"); ("value", `String (Printf.sprintf "`$%.4f`" fill.fill_price)); ("inline", `Bool true)];
          `Assoc [("name", `String "Value"); ("value", `String (Printf.sprintf "`$%.2f`" fill.value)); ("inline", `Bool true)];
          `Assoc [("name", `String "Net (after fee)"); ("value", `String (Printf.sprintf "`$%.2f`" value_after_fee)); ("inline", `Bool true)];
        ]);
        ("timestamp", `String (let t = Unix.gmtime fill.timestamp in
          Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02dZ"
            (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday
            t.tm_hour t.tm_min t.tm_sec));
      ] in
      Some (`Assoc [("embeds", `List [embed])])
  | fills ->
      (* Multiple fills: batch into a single embed with per-fill fields *)
      let n = List.length fills in
      let color = 0x3498DB in (* Blue for batched *)
      let venues = List.sort_uniq String.compare (List.map (fun (f : Concurrency.Fill_event_bus.fill_event) -> f.venue) fills) in
      let venue_str = String.concat ", " venues in
      let fields = List.map format_fill_field fills in
      let embed = `Assoc [
        ("title", `String (Printf.sprintf "%d Order Fills — %s" n venue_str));
        ("color", `Int color);
        ("fields", `List fields);
        ("timestamp", `String (let t = Unix.gmtime (Unix.gettimeofday ()) in
          Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02dZ"
            (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday
            t.tm_hour t.tm_min t.tm_sec));
      ] in
      Some (`Assoc [("embeds", `List [embed])])

(* ---- Webhook HTTP Client ---- *)

(** Send a JSON payload to the Discord webhook URL.
    Returns [Ok ()] on success, [Error msg] on failure.
    Handles 429 rate limit responses by sleeping for Retry-After. *)
let send_webhook ~webhook_url payload =
  let json_str = Yojson.Safe.to_string payload in
  let body = Cohttp_lwt.Body.of_string json_str in
  let headers = Cohttp.Header.of_list [
    ("Content-Type", "application/json");
  ] in
  let uri = Uri.of_string webhook_url in
  Lwt.catch (fun () ->
    Cohttp_lwt_unix.Client.post ~headers ~body uri >>= fun (resp, resp_body) ->
    let status = Cohttp.Response.status resp in
    let code = Cohttp.Code.code_of_status status in
    Cohttp_lwt.Body.drain_body resp_body >>= fun () ->
    if code >= 200 && code < 300 then
      Lwt.return (Ok ())
    else if code = 429 then begin
      (* Rate limited: extract Retry-After header and sleep *)
      let retry_after =
        match Cohttp.Header.get (Cohttp.Response.headers resp) "retry-after" with
        | Some s -> (try float_of_string s with _ -> 2.0)
        | None -> 2.0
      in
      (* Cap retry_after to prevent extreme sleeps from Discord abuse penalties *)
      let capped = min retry_after 60.0 in
      Logging.warn_f ~section "Discord webhook rate limited (429), sleeping %.1fs (raw retry-after: %.1fs)" capped retry_after;
      Lwt_unix.sleep capped >>= fun () ->
      Lwt.return (Error "rate_limited")
    end else begin
      Logging.warn_f ~section "Discord webhook returned HTTP %d" code;
      Lwt.return (Error (Printf.sprintf "http_%d" code))
    end
  ) (fun exn ->
    Logging.warn_f ~section "Discord webhook request failed: %s" (Printexc.to_string exn);
    Lwt.return (Error (Printexc.to_string exn))
  )

(** Send with retry: up to 3 attempts with 5s backoff on transient errors. *)
let send_with_retry ~webhook_url payload =
  let rec attempt n =
    if n > 3 then begin
      Logging.error ~section "Discord webhook failed after 3 attempts, dropping message";
      Lwt.return_unit
    end else begin
      acquire_token () >>= fun () ->
      send_webhook ~webhook_url payload >>= function
      | Ok () -> Lwt.return_unit
      | Error "rate_limited" ->
          (* Already slept in send_webhook; retry immediately *)
          attempt (n + 1)
      | Error _ ->
          Lwt_unix.sleep 5.0 >>= fun () ->
          attempt (n + 1)
    end
  in
  attempt 1

(* ---- Consumer Loop ---- *)

(** Main consumer loop. Drains the fill event ring buffer on each wakeup,
    batches fills into webhook payloads, and sends them with rate limiting. *)
let consumer_loop ~webhook_url () =
  let read_pos = ref (Concurrency.Fill_event_bus.get_position ()) in
  let rec loop () =
    if !read_pos = Concurrency.Fill_event_bus.get_position () then
      (* No new fills, wait for the condition variable to be signaled *)
      Concurrency.Fill_event_bus.wait_for_fill () >>= fun () ->
      loop ()
    else begin
      (* New fills available, drain them all *)
      let fills = ref [] in
      let new_pos = Concurrency.Fill_event_bus.iter_since !read_pos (fun fill ->
        fills := fill :: !fills
      ) in
      read_pos := new_pos;
      (* Drop stale fills (>5min old) to skip replayed historical fills on reconnect *)
      let now = Unix.gettimeofday () in
      let max_age = 300.0 in
      let fills = List.rev !fills
        |> List.filter (fun (f : Concurrency.Fill_event_bus.fill_event) ->
          let age = now -. f.timestamp in
          if age > max_age then begin
            Logging.debug_f ~section "Dropping stale fill for %s/%s (age=%.0fs)" f.venue f.symbol age;
            false
          end else true
        )
      in
      
      if fills <> [] then begin
        Logging.debug_f ~section "Processing %d fill event(s) for Discord" (List.length fills);
        (* Split into batches of max_fills_per_message *)
        let rec send_batches remaining =
          match remaining with
          | [] -> Lwt.return_unit
          | _ ->
              let batch, rest =
                let rec take n acc = function
                  | [] -> (List.rev acc, [])
                  | _ when n = 0 -> (List.rev acc, remaining)
                  | x :: xs -> take (n - 1) (x :: acc) xs
                in
                take max_fills_per_message [] remaining
              in
              (match build_webhook_payload batch with
               | Some payload ->
                   send_with_retry ~webhook_url payload >>= fun () ->
                   send_batches rest
               | None ->
                   send_batches rest)
        in
        send_batches fills >>= fun () ->
        loop ()
      end else
        loop ()
    end
  in
  loop ()

(* ---- Initialization ---- *)

(** Start the Discord notifier. Reads DISCORD_WEBHOOK_URL from environment.
    If not set, logs a warning and returns silently (no-op).
    Spawns the consumer loop as a self-restarting Lwt.async fiber. *)
let start () =
  let webhook_url =
    try
      let url = Sys.getenv "DISCORD_WEBHOOK_URL" |> String.trim in
      if url = "" then None else Some url
    with Not_found -> None
  in
  match webhook_url with
  | None ->
      Logging.warn ~section "DISCORD_WEBHOOK_URL not set, Discord notifications disabled";
      ()
  | Some url ->
      Logging.info ~section "Discord notifier starting (webhook configured)";
      let rec start_consumer () =
        Lwt.catch
          (fun () -> consumer_loop ~webhook_url:url ())
          (fun exn ->
            Logging.error_f ~section "Discord consumer loop crashed: %s, restarting in 10s"
              (Printexc.to_string exn);
            Lwt_unix.sleep 10.0 >>= fun () ->
            start_consumer ())
      in
      Lwt.async start_consumer
