(** Discord order fill notifier.

    Consumes fill events from the centralized [Fill_event_bus] ring buffer and delivers
    formatted notifications to a Discord webhook. A token bucket enforces compliance with
    Discord's webhook rate limit (5 requests / 2 s).

    Architecture:
    - One Lwt consumer fiber drains the ring buffer per wakeup.
    - Fills in the same drain cycle batch into one embed (up to 10 fields).
    - Token bucket (5 tokens, 1 refill / 400ms) governs POST timing.
    - On 429: sleeps for the Retry-After duration.
    - On 5xx / network error: 5s backoff, up to 3 attempts.
    - Self-restarts on crash with 10s backoff. *)

open Lwt.Infix

let section = "discord_notifier"

(** Maximum fill events per embed. Discord supports up to 25 fields; 10 keeps messages
    readable (one full-width field per fill). *)
let max_fills_per_message = 10

(* Token bucket rate limiter. *)

(** Token bucket state for rate limiting webhook POSTs. Discord enforces 5 requests / 2
    seconds per webhook URL. 5 tokens with a 400ms refill interval = 2.5 req/s steady
    state, below the limit with margin for clock drift. *)
type token_bucket =
  { mutable tokens : float
  ; mutable last_refill : float
  ; max_tokens : float
  ; refill_interval : float (** Seconds between token refills. *)
  }

let bucket =
  { tokens = 5.0
  ; last_refill = Unix.gettimeofday ()
  ; max_tokens = 5.0
  ; refill_interval = 0.4 (* 400ms = 1 token per 0.4s *)
  }
;;

(** Refill tokens based on elapsed time since last refill. *)
let refill_tokens () =
  let now = Unix.gettimeofday () in
  let elapsed = now -. bucket.last_refill in
  let new_tokens = elapsed /. bucket.refill_interval in
  if new_tokens >= 1.0
  then (
    bucket.tokens <- min bucket.max_tokens (bucket.tokens +. new_tokens);
    bucket.last_refill <- now)
;;

(** Acquire a token, sleeping until one is available. *)
let acquire_token () =
  let rec try_acquire () =
    refill_tokens ();
    if bucket.tokens >= 1.0
    then (
      bucket.tokens <- bucket.tokens -. 1.0;
      Lwt.return_unit)
    else (
      (* Sleep until the next token is available. *)
      let deficit = 1.0 -. bucket.tokens in
      let sleep_time = deficit *. bucket.refill_interval in
      Lwt_unix.sleep sleep_time >>= fun () -> try_acquire ())
  in
  try_acquire ()
;;

(* Message formatting. *)

(** Market and strategy labels. *)
let market_label = function
  | "perp" -> "Perp"
  | "spot" -> "Spot"
  | "equity" -> "Equity"
  | m -> m
;;

let strategy_label = function
  | "grid" -> "Grid"
  | "carry" -> "Carry"
  | "hedge" -> "Hedge"
  | "account" -> "Account"
  | s -> s
;;

let venue_label = function
  | "kraken" -> "Kraken"
  | "hyperliquid" -> "Hyperliquid"
  | "lighter" -> "Lighter"
  | "ibkr" -> "IBKR"
  | "alpaca" -> "Alpaca"
  | v -> String.capitalize_ascii v
;;

(* Embed colors. Spot uses green/red for buys/sells; perps use blue/grey for opens/closes
   so the color bar identifies the market at a glance. *)
let color_buy = 0x2ECC71
let color_sell = 0xE74C3C
let color_perp_open = 0x3498DB
let color_perp_close = 0x95A5A6
let color_liquidation = 0x8B0000
let color_batch = 0x5865F2

(** Venue intent -> action label. The action leads so it is the first thing read. *)
let intent_label (fill : Concurrency.Fill_event_bus.fill_event) =
  match fill.intent with
  | "open_short" -> "OPEN SHORT"
  | "close_short" -> "CLOSE SHORT"
  | "open_long" -> "OPEN LONG"
  | "close_long" -> "CLOSE LONG"
  | "buy_spot" -> "BUY SPOT"
  | "sell_spot" -> "SELL SPOT"
  | "buy_equity" -> "BUY EQUITY"
  | "sell_equity" -> "SELL EQUITY"
  | "liquidation" -> "LIQUIDATION"
  | _ -> if fill.side = "buy" then "BUY" else "SELL"
;;

let intent_color (fill : Concurrency.Fill_event_bus.fill_event) =
  match fill.intent with
  | "liquidation" -> color_liquidation
  | "open_short" | "open_long" -> color_perp_open
  | "close_short" | "close_long" -> color_perp_close
  | "sell_spot" | "sell_equity" -> color_sell
  | "buy_spot" | "buy_equity" -> color_buy
  | _ -> if fill.side = "buy" then color_buy else color_sell
;;

(** A batch keeps a single fill's color when every fill agrees, otherwise it falls back to
    a neutral color so the bar never implies a specific action. *)
let batch_color (fills : Concurrency.Fill_event_bus.fill_event list) =
  match List.map intent_color fills with
  | [] -> color_batch
  | c :: rest when List.for_all (fun x -> x = c) rest -> c
  | _ -> color_batch
;;

(** Insert thousands separators into a plain decimal string. *)
let group_thousands s =
  let int_part, fraction =
    match String.index_opt s '.' with
    | Some i -> String.sub s 0 i, String.sub s i (String.length s - i)
    | None -> s, ""
  in
  let n = String.length int_part in
  let buf = Buffer.create (n + (n / 3)) in
  String.iteri
    (fun i c ->
      if i > 0 && (n - i) mod 3 = 0 then Buffer.add_char buf ',';
      Buffer.add_char buf c)
    int_part;
  Buffer.contents buf ^ fraction
;;

(** Trim trailing zeros while keeping two decimal places. *)
let trim_zeros s =
  match String.index_opt s '.' with
  | None -> s
  | Some dot ->
    let min_len = dot + 3 in
    let rec find i =
      if i <= min_len
      then min_len
      else if String.get s (i - 1) = '0'
      then find (i - 1)
      else i
    in
    String.sub s 0 (find (String.length s))
;;

(** Currency with useful precision: cents for large amounts, more digits for sub-dollar
    prices, and no noisy trailing zeros. *)
let format_currency v =
  let magnitude = Float.abs v in
  let decimals =
    if magnitude >= 1000.0
    then 2
    else if magnitude >= 1.0
    then 4
    else if magnitude >= 0.01
    then 6
    else if magnitude >= 0.0001
    then 8
    else 10
  in
  let magnitude_str = Printf.sprintf "%.*f" decimals magnitude |> trim_zeros in
  Printf.sprintf "%s$%s" (if v < 0.0 then "-" else "") (group_thousands magnitude_str)
;;

let pnl_str p =
  let sign = if p >= 0.0 then "+" else "-" in
  sign ^ format_currency (Float.abs p)
;;

let field name value inline =
  `Assoc [ "name", `String name; "value", `String value; "inline", `Bool inline ]
;;

(** One-line summary: action, instrument, and fill price. Used as message content so push
    notifications carry the same information as the channel. *)
let fill_summary (fill : Concurrency.Fill_event_bus.fill_event) =
  let action = intent_label fill in
  if fill.symbol = "ACCOUNT" && String.equal fill.intent "liquidation"
  then "LIQUIDATION — ACCOUNT"
  else if fill.fill_price > 0.0
  then Printf.sprintf "%s — %s @ %s" action fill.symbol (format_currency fill.fill_price)
  else Printf.sprintf "%s — %s" action fill.symbol
;;

(** Compact per-fill field block for batched embeds. *)
let format_fill_field (fill : Concurrency.Fill_event_bus.fill_event) =
  let name =
    if fill.symbol = "ACCOUNT"
    then Printf.sprintf "%s — ACCOUNT" (intent_label fill)
    else if fill.fill_price > 0.0
    then
      Printf.sprintf
        "%s — %s @ %s"
        (intent_label fill)
        fill.symbol
        (format_currency fill.fill_price)
    else Printf.sprintf "%s — %s" (intent_label fill) fill.symbol
  in
  let detail =
    if fill.symbol = "ACCOUNT"
    then
      Printf.sprintf
        "Venue: %s\nLiquidated notional: %s"
        (venue_label fill.venue)
        (format_currency fill.value)
    else
      Printf.sprintf
        "Venue: %s · %s · %s\nAmount: %.8g\nValue: %s\nFee: %s"
        (venue_label fill.venue)
        (market_label fill.market)
        (strategy_label fill.strategy)
        fill.amount
        (format_currency fill.value)
        (format_currency fill.fee)
  in
  let detail =
    match fill.realized_pnl with
    | Some p -> detail ^ Printf.sprintf "\nRealized PnL: %s" (pnl_str p)
    | None -> detail
  in
  let detail =
    match fill.note with
    | Some s when s <> "" -> detail ^ Printf.sprintf "\nNote: %s" s
    | _ -> detail
  in
  field name detail false
;;

(** Full field list for the single-fill focused embed. *)
let fill_fields (fill : Concurrency.Fill_event_bus.fill_event) =
  let base =
    [ field "Venue" (venue_label fill.venue) true
    ; field "Market" (market_label fill.market) true
    ; field "Strategy" (strategy_label fill.strategy) true
    ]
  in
  let size =
    if fill.symbol = "ACCOUNT"
    then [ field "Liquidated notional" (format_currency fill.value) true ]
    else
      [ field "Amount" (Printf.sprintf "%.8g" fill.amount) true
      ; field
          "Price"
          (if fill.fill_price > 0.0 then format_currency fill.fill_price else "n/a")
          true
      ; field "Value" (format_currency fill.value) true
      ]
  in
  let fees =
    if fill.fee > 0.0 then [ field "Fee" (format_currency fill.fee) true ] else []
  in
  let pnl =
    match fill.realized_pnl with
    | Some p -> [ field "Realized PnL" (pnl_str p) true ]
    | None -> []
  in
  let note =
    match fill.note with
    | Some s when s <> "" -> [ field "Note" s false ]
    | _ -> []
  in
  base @ size @ fees @ pnl @ note
;;

let iso8601 timestamp =
  let t = Unix.gmtime timestamp in
  Printf.sprintf
    "%04d-%02d-%02dT%02d:%02d:%02dZ"
    (t.tm_year + 1900)
    (t.tm_mon + 1)
    t.tm_mday
    t.tm_hour
    t.tm_min
    t.tm_sec
;;

let embed_json ~color ~fields ~timestamp =
  `Assoc
    [ "color", `Int color
    ; "fields", `List fields
    ; "timestamp", `String (iso8601 timestamp)
    ]
;;

(** Batch summary: count and action when uniform, the asset when uniform, and venues. *)
let batch_summary (fills : Concurrency.Fill_event_bus.fill_event list) =
  let unique f = List.sort_uniq String.compare (List.map f fills) in
  let actions = unique intent_label in
  let symbols = unique (fun (f : Concurrency.Fill_event_bus.fill_event) -> f.symbol) in
  let venues =
    unique (fun (f : Concurrency.Fill_event_bus.fill_event) -> venue_label f.venue)
  in
  let n = List.length fills in
  let action =
    match actions with
    | [ action ] -> Printf.sprintf "%d× %s" n action
    | _ -> Printf.sprintf "%d FILLS" n
  in
  let subject =
    match symbols with
    | [ symbol ] -> symbol
    | _ -> "MULTIPLE ASSETS"
  in
  Printf.sprintf "%s — %s · %s" action subject (String.concat ", " venues)
;;

(** Build a Discord webhook JSON payload for a batch of fills. *)
let build_webhook_payload (fills : Concurrency.Fill_event_bus.fill_event list) =
  match fills with
  | [] -> None
  | [ fill ] ->
    let embed =
      embed_json
        ~color:(intent_color fill)
        ~fields:(fill_fields fill)
        ~timestamp:fill.timestamp
    in
    Some (`Assoc [ "content", `String (fill_summary fill); "embeds", `List [ embed ] ])
  | fills ->
    (* Multiple fills: one embed with a full-width field per fill. *)
    let n = List.length fills in
    let has_liquidation =
      List.exists
        (fun (f : Concurrency.Fill_event_bus.fill_event) ->
          String.equal f.intent "liquidation")
        fills
    in
    let color = if has_liquidation then color_liquidation else batch_color fills in
    let venues =
      List.sort_uniq
        String.compare
        (List.map
           (fun (f : Concurrency.Fill_event_bus.fill_event) -> venue_label f.venue)
           fills)
    in
    let venue_str = String.concat ", " venues in
    let summary =
      if has_liquidation
      then Printf.sprintf "%d FILLS (LIQUIDATION) — %s" n venue_str
      else batch_summary fills
    in
    let embed =
      embed_json
        ~color
        ~fields:(List.map format_fill_field fills)
        ~timestamp:(Unix.gettimeofday ())
    in
    Some (`Assoc [ "content", `String summary; "embeds", `List [ embed ] ])
;;

(* Webhook HTTP client. *)

(** POST a JSON payload to the Discord webhook URL. Returns [Ok ()] on success,
    [Error msg] on failure. On 429, sleeps for the Retry-After duration. *)
let send_webhook ~webhook_url payload =
  let json_str = Yojson.Safe.to_string payload in
  let body = Cohttp_lwt.Body.of_string json_str in
  let headers = Cohttp.Header.of_list [ "Content-Type", "application/json" ] in
  let uri = Uri.of_string webhook_url in
  Lwt.catch
    (fun () ->
      Cohttp_lwt_unix.Client.post ~headers ~body uri
      >>= fun (resp, resp_body) ->
      let status = Cohttp.Response.status resp in
      let code = Cohttp.Code.code_of_status status in
      Cohttp_lwt.Body.drain_body resp_body
      >>= fun () ->
      if code >= 200 && code < 300
      then Lwt.return (Ok ())
      else if code = 429
      then (
        (* Rate limited: extract Retry-After header and sleep *)
        let retry_after =
          match Cohttp.Header.get (Cohttp.Response.headers resp) "retry-after" with
          | Some s ->
            (try float_of_string s with
             | _ -> 2.0)
          | None -> 2.0
        in
        (* Cap retry_after to prevent extreme sleeps from Discord abuse penalties *)
        let capped = min retry_after 60.0 in
        Logging.warn_f
          ~section
          "Discord webhook rate limited (429), sleeping %.1fs (raw retry-after: %.1fs)"
          capped
          retry_after;
        Lwt_unix.sleep capped >>= fun () -> Lwt.return (Error "rate_limited"))
      else (
        Logging.warn_f ~section "Discord webhook returned HTTP %d" code;
        Lwt.return (Error (Printf.sprintf "http_%d" code))))
    (fun exn ->
      Logging.warn_f
        ~section
        "Discord webhook request failed: %s"
        (Printexc.to_string exn);
      Lwt.return (Error (Printexc.to_string exn)))
;;

(** Send with retry: up to 3 attempts, 5s backoff on transient errors. *)
let send_with_retry ~webhook_url payload =
  let rec attempt n =
    if n > 3
    then (
      Logging.error ~section "Discord webhook failed after 3 attempts, dropping message";
      Lwt.return_unit)
    else
      acquire_token ()
      >>= fun () ->
      send_webhook ~webhook_url payload
      >>= function
      | Ok () -> Lwt.return_unit
      | Error "rate_limited" ->
        (* Already slept in send_webhook; retry immediately *)
        attempt (n + 1)
      | Error _ -> Lwt_unix.sleep 5.0 >>= fun () -> attempt (n + 1)
  in
  attempt 1
;;

(* Consumer loop. *)

(** Main consumer loop. Drains the fill event ring buffer per wakeup, batches fills into
    webhook payloads, and sends with rate limiting. *)
let consumer_loop ~webhook_url () =
  let read_pos = ref (Concurrency.Fill_event_bus.get_position ()) in
  let done_p, done_u = Lwt.wait () in
  let rec loop () =
    Lwt.catch
      (fun () ->
        if !read_pos = Concurrency.Fill_event_bus.get_position ()
        then (
          (* No new fills, wait for the condition variable to be signaled *)
          Concurrency.Fill_event_bus.wait_for_fill ()
          >>= fun () ->
          Lwt.async loop;
          Lwt.return_unit)
        else (
          (* New fills available, drain them all *)
          let fills = ref [] in
          let new_pos =
            Concurrency.Fill_event_bus.iter_since !read_pos (fun fill ->
              fills := fill :: !fills)
          in
          read_pos := new_pos;
          (* Drop stale fills (>5min old) to skip replayed historical fills on reconnect *)
          let now = Unix.gettimeofday () in
          let max_age = 300.0 in
          let fills =
            List.rev !fills
            |> List.filter (fun (f : Concurrency.Fill_event_bus.fill_event) ->
              let age = now -. f.timestamp in
              if age > max_age
              then (
                Logging.debug_f
                  ~section
                  "Dropping stale fill for %s/%s (age=%.0fs)"
                  f.venue
                  f.symbol
                  age;
                false)
              else true)
          in
          if fills <> []
          then (
            Logging.debug_f
              ~section
              "Processing %d fill event(s) for Discord"
              (List.length fills);
            (* Split into batches of max_fills_per_message *)
            let rec send_batches remaining =
              match remaining with
              | [] -> Lwt.return_unit
              | _ ->
                let batch, rest =
                  let rec take n acc = function
                    | [] -> List.rev acc, []
                    | _ when n = 0 -> List.rev acc, remaining
                    | x :: xs -> take (n - 1) (x :: acc) xs
                  in
                  take max_fills_per_message [] remaining
                in
                (match build_webhook_payload batch with
                 | Some payload ->
                   send_with_retry ~webhook_url payload >>= fun () -> send_batches rest
                 | None -> send_batches rest)
            in
            send_batches fills
            >>= fun () ->
            Lwt.async loop;
            Lwt.return_unit)
          else (
            Lwt.async loop;
            Lwt.return_unit)))
      (fun exn ->
        if Lwt.is_sleeping done_p then Lwt.wakeup_later_exn done_u exn;
        Lwt.return_unit)
  in
  Lwt.async loop;
  done_p
;;

(* Initialization. *)

(** Start the Discord notifier. Reads DISCORD_WEBHOOK_URL from the environment; if unset,
    logs a warning and returns (no-op). Spawns the consumer loop as a self-restarting
    Lwt.async fiber. *)
let start () =
  let webhook_url =
    try
      let url = Sys.getenv "DISCORD_WEBHOOK_URL" |> String.trim in
      if url = "" then None else Some url
    with
    | Not_found -> None
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
          Logging.error_f
            ~section
            "Discord consumer loop crashed: %s, restarting in 10s"
            (Printexc.to_string exn);
          Lwt_unix.sleep 10.0 >>= fun () -> start_consumer ())
    in
    Lwt.async start_consumer
;;
