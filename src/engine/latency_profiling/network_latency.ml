(** Per-venue network latency profilers.

    Measure the four NETWORK-page metrics shown under each domain row (the domain's
    venue):
    - ws_ping: venue WebSocket ping/pong round trip
    - ws_feed: clock-corrected one-way latency of market-data feed messages (server event
      timestamp -> local receive)
    - rest_request: HTTP REST round trip (trading actions and oracle fetches)
    - signer: local signature generation time

    Profilers are keyed by venue name ("hyperliquid", "kraken", "lighter", "alpaca"), so
    every connection or fetch on a venue feeds one histogram. The dashboard merges these
    windows into the per-symbol latency map via each configured symbol's exchange.

    [start_publisher] publishes (snapshot_and_reset) every ~10s, inside the dashboard's
    15s freshness tolerance, so the NETWORK page always has a fresh window per metric; an
    idle venue reads as "idle", not "--". *)

open Lwt.Infix

let section = "network_latency"

(** One venue's four profilers. *)
type t =
  { ping : Latency_profiler.t
  ; feed : Latency_profiler.t
  ; rest : Latency_profiler.t
  ; signer : Latency_profiler.t
  }

let profilers : (string, t) Hashtbl.t = Hashtbl.create 8
let mutex = Mutex.create ()

(** Coarse bucket width and upper bound for the network RTT/latency metrics. 1ms buckets
    with a 1us fine tier give exact sub-millisecond resolution and a 60s range, so even a
    multi-second stall is represented faithfully instead of saturating a low ceiling (the
    old 2s cap collapsed every >=2s sample to a misleading "2.0s"). *)
let network_bucket_us = 1000

let network_max_latency_us = 60_000_000

let create_venue_profilers venue =
  { ping =
      Latency_profiler.create
        ~bucket_us:network_bucket_us
        ~max_latency_us:network_max_latency_us
        (venue ^ ":ws_ping")
  ; feed =
      Latency_profiler.create
        ~bucket_us:network_bucket_us
        ~max_latency_us:network_max_latency_us
        (venue ^ ":ws_feed")
  ; rest =
      Latency_profiler.create
        ~bucket_us:network_bucket_us
        ~max_latency_us:network_max_latency_us
        (venue ^ ":rest_request")
  ; signer =
      Latency_profiler.create ~bucket_us:1 ~max_latency_us:100_000 (venue ^ ":signer")
  }
;;

let hl_profilers = create_venue_profilers "hyperliquid"
let kraken_profilers = create_venue_profilers "kraken"
let lighter_profilers = create_venue_profilers "lighter"
let alpaca_profilers = create_venue_profilers "alpaca"

let () =
  Hashtbl.replace profilers "hyperliquid" hl_profilers;
  Hashtbl.replace profilers "kraken" kraken_profilers;
  Hashtbl.replace profilers "lighter" lighter_profilers;
  Hashtbl.replace profilers "alpaca" alpaca_profilers
;;

let[@inline always] venue_profilers venue =
  match venue with
  | "hyperliquid" -> hl_profilers
  | "kraken" -> kraken_profilers
  | "lighter" -> lighter_profilers
  | "alpaca" -> alpaca_profilers
  | _ ->
    Mutex.lock mutex;
    let p =
      match Hashtbl.find_opt profilers venue with
      | Some p -> p
      | None ->
        let p = create_venue_profilers venue in
        Hashtbl.replace profilers venue p;
        p
    in
    Mutex.unlock mutex;
    p
;;

(** Span of [seconds] (wall-clock delta) for call sites timing with [Unix.gettimeofday].
    Rounds to the nearest nanosecond instead of truncating, so a float-arithmetic result
    just under a bucket edge (e.g. 149.999999ms) is not silently demoted to the bucket
    below. *)
let span_of_seconds s =
  let ns = s *. 1_000_000_000.0 in
  let ns = if ns >= 0.0 then ns +. 0.5 else ns -. 0.5 in
  Mtime.Span.of_uint64_ns (Int64.of_float (max 0.0 ns))
;;

(* ── Feed clock-offset correction ───────────────────────────────────────────

   A feed message's one-way latency is [local_recv - server_event], but that difference
   also contains the constant offset between the host clock and the exchange clock. Ping
   RTT is offset-free, so the best-case one-way transit is [min_rtt / 2]. Taking the
   minimum [local_recv - server_event] over a recent window as [offset + min_transit] lets
   us estimate the clock offset as [min_d - min_rtt/2] and recover a true one-way latency:

   latency = (local_recv - server_event) - (min_d - min_rtt/2) = (d - min_d) + min_rtt/2

   The minimum therefore reads as [min_rtt/2] (the network floor) and every other sample
   as that floor plus its excess over the best case.

   Both minima are tracked with a sliding-window min filter (a monotonic deque evicting
   entries older than [offset_window_s]) rather than a lifetime minimum. A lifetime
   minimum never adapts to host-clock drift or an NTP step, and a single anomalously low
   sample biases every later reading for the life of the process. The window bounds both:
   stale samples age out after five minutes, so the estimate tracks the clocks while
   remaining cheap (O(1) amortized) and memory-bounded. *)

(** Sliding-window minimum ("min filter"). [push ~time ~value] keeps values in
    non-decreasing order from front to back so the front is always the window minimum;
    [evict ~now] drops entries older than [window_s]. *)
module Window_min = struct
  type t =
    { mutable a_t : float array (* sample times, increasing *)
    ; mutable a_v : float array (* values, non-decreasing front -> back *)
    ; mutable head : int
    ; mutable len : int
    ; window_s : float
    ; max_len : int
    }

  let create ~window_s ~max_len =
    { a_t = Array.make 64 0.0
    ; a_v = Array.make 64 0.0
    ; head = 0
    ; len = 0
    ; window_s
    ; max_len
    }
  ;;

  (* Ensure one free slot at [head + len], compacting or growing as needed. *)
  let ensure_space t =
    let cap = Array.length t.a_t in
    if t.head + t.len >= cap
    then
      if t.head > cap / 2
      then (
        Array.blit t.a_t t.head t.a_t 0 t.len;
        Array.blit t.a_v t.head t.a_v 0 t.len;
        t.head <- 0)
      else (
        let cap' = cap * 2 in
        let nt = Array.make cap' 0.0 in
        let nv = Array.make cap' 0.0 in
        Array.blit t.a_t t.head nt 0 t.len;
        Array.blit t.a_v t.head nv 0 t.len;
        t.a_t <- nt;
        t.a_v <- nv;
        t.head <- 0)
  ;;

  let push t ~time ~value =
    while t.len > 0 && t.a_v.(t.head + t.len - 1) >= value do
      t.len <- t.len - 1
    done;
    ensure_space t;
    t.a_t.(t.head + t.len) <- time;
    t.a_v.(t.head + t.len) <- value;
    t.len <- t.len + 1;
    (* Memory backstop: drop the oldest entry if the window grew unbounded. *)
    if t.len > t.max_len
    then (
      t.head <- t.head + 1;
      t.len <- t.len - 1)
  ;;

  let evict t ~now =
    let cutoff = now -. t.window_s in
    while t.len > 0 && t.a_t.(t.head) < cutoff do
      t.head <- t.head + 1;
      t.len <- t.len - 1
    done
  ;;

  let min_opt t = if t.len = 0 then None else Some t.a_v.(t.head)
end

(** Rolling window over which the clock-offset minima are taken. Long enough to observe
    the network floor, short enough to track clock drift/steps. *)
let offset_window_s = 300.0

let offset_max_len = 20_000

(** A feed sample is stale/out-of-order (not transit delay) when its server event time
    trails the newest event seen for the venue by more than this. A live feed's
    cross-symbol jitter is well under a second. *)
let stale_event_tolerance_s = 5.0

(** Feed freshness gate. A live event-driven feed's server event timestamps advance at
    roughly wall-clock rate; a feed replaying stale data (e.g. the previous session's
    quotes outside market hours) keeps emitting the same old timestamp while local time
    advances. Comparing the buffered server-event span against the local-receive span over
    a recent window distinguishes the two, so staleness is reported as "no sample" instead
    of as enormous latency. Without this, a min-filter clock-offset estimate decays toward
    the window length during a stale period and pins the reading at the profiler ceiling. *)
module Feed_freshness = struct
  let cap = 256
  let window_s = 60.0
  let min_span_s = 15.0
  let min_advance_ratio = 0.25

  type t =
    { recv : float array
    ; event : float array
    ; mutable n : int
    ; mutable i : int
    }

  let create () = { recv = Array.make cap 0.0; event = Array.make cap 0.0; n = 0; i = 0 }

  let push t ~recv ~event =
    t.recv.(t.i) <- recv;
    t.event.(t.i) <- event;
    t.i <- (t.i + 1) mod cap;
    if t.n < cap then t.n <- t.n + 1
  ;;

  (** [is_live t ~now] is true unless recent server timestamps clearly failed to advance
      with local time, or there is too little recent data to judge. *)
  let is_live t ~now =
    let rmin = ref infinity
    and rmax = ref neg_infinity
    and emin = ref infinity
    and emax = ref neg_infinity
    and count = ref 0 in
    let cutoff = now -. window_s in
    for k = 0 to t.n - 1 do
      let r = t.recv.(k) in
      if r >= cutoff
      then (
        incr count;
        let e = t.event.(k) in
        if r < !rmin then rmin := r;
        if r > !rmax then rmax := r;
        if e < !emin then emin := e;
        if e > !emax then emax := e)
    done;
    if !count < 2
    then true
    else (
      let dr = !rmax -. !rmin in
      let de = !emax -. !emin in
      dr < min_span_s || de >= min_advance_ratio *. dr)
  ;;
end

type offset_state =
  { d : Window_min.t (* min (recv - event) *)
  ; rtt : Window_min.t (* min ping RTT *)
  ; mutable max_event : float (* newest server event time seen *)
  ; fresh : Feed_freshness.t
  }

let offsets : (string, offset_state) Hashtbl.t = Hashtbl.create 8
let offsets_mutex = Mutex.create ()

let offset_state venue =
  Mutex.lock offsets_mutex;
  let st =
    match Hashtbl.find_opt offsets venue with
    | Some st -> st
    | None ->
      let st =
        { d = Window_min.create ~window_s:offset_window_s ~max_len:offset_max_len
        ; rtt = Window_min.create ~window_s:offset_window_s ~max_len:offset_max_len
        ; max_event = neg_infinity
        ; fresh = Feed_freshness.create ()
        }
      in
      Hashtbl.replace offsets venue st;
      st
  in
  Mutex.unlock offsets_mutex;
  st
;;

(** Records a ping RTT (seconds) as a candidate for the best-case one-way transit floor
    used by [corrected_one_way]. *)
let observe_rtt ?now venue rtt =
  if Float.is_finite rtt && rtt > 0.0
  then (
    let now =
      match now with
      | Some t -> t
      | None -> Unix.gettimeofday ()
    in
    let st = offset_state venue in
    Mutex.lock offsets_mutex;
    Window_min.push st.rtt ~time:now ~value:rtt;
    Window_min.evict st.rtt ~now;
    Mutex.unlock offsets_mutex)
;;

(** [corrected_one_way ?now venue ~recv ~event] returns the clock-offset- corrected
    one-way latency in seconds for a fresh feed message whose server event time is [event]
    (Unix seconds) and which was read locally at [recv] (Unix seconds), or [None] when the
    sample is stale/out-of-order (see [Feed_freshness] and [stale_event_tolerance_s]).
    [now] defaults to [recv] and drives window eviction. Fresh samples are folded into the
    venue's sliding-window minimum. *)
let corrected_one_way ?now venue ~recv ~event =
  let now =
    match now with
    | Some t -> t
    | None -> recv
  in
  let st = offset_state venue in
  Mutex.lock offsets_mutex;
  Feed_freshness.push st.fresh ~recv ~event;
  let live = Feed_freshness.is_live st.fresh ~now in
  let ordered = event >= st.max_event -. stale_event_tolerance_s in
  if event > st.max_event then st.max_event <- event;
  let result =
    if not (live && ordered)
    then None
    else (
      let d = recv -. event in
      Window_min.push st.d ~time:recv ~value:d;
      Window_min.evict st.d ~now;
      let offset =
        match Window_min.min_opt st.d, Window_min.min_opt st.rtt with
        | Some dm, Some rm -> dm -. (rm /. 2.0)
        | Some dm, None -> dm
        | None, _ -> 0.0
      in
      Some (max 0.0 (d -. offset)))
  in
  Mutex.unlock offsets_mutex;
  result
;;

(** Parse an RFC3339 / ISO8601 timestamp (e.g. "2025-01-14T16:05:51.872012Z") into Unix
    epoch seconds. Handles fractional seconds and a trailing "Z" or numeric ±HH:MM offset.
    Returns [None] for unparseable input. *)
let unix_of_rfc3339 s =
  let n = String.length s in
  if n < 19
  then None
  else (
    try
      let y = int_of_string (String.sub s 0 4) in
      let mo = int_of_string (String.sub s 5 2) in
      let d = int_of_string (String.sub s 8 2) in
      let h = int_of_string (String.sub s 11 2) in
      let mi = int_of_string (String.sub s 14 2) in
      let rest = String.sub s 17 (n - 17) in
      (* Numeric seconds end at 'Z' or a ±HH:MM offset. *)
      let stop = ref (String.length rest) in
      String.iteri
        (fun i c ->
          if (c = 'Z' || c = 'z' || c = '+' || c = '-') && i < !stop then stop := i)
        rest;
      let sec = float_of_string (String.sub rest 0 !stop) in
      let tz_offset =
        if !stop < String.length rest && (rest.[!stop] = '+' || rest.[!stop] = '-')
        then (
          let sign = if rest.[!stop] = '+' then 1.0 else -1.0 in
          let hh = int_of_string (String.sub rest (!stop + 1) 2) in
          let mm = int_of_string (String.sub rest (!stop + 4) 2) in
          sign *. ((float_of_int hh *. 3600.0) +. (float_of_int mm *. 60.0)))
        else 0.0
      in
      (* Days since 1970-01-01 for the proleptic Gregorian date (Howard Hinnant's
         days_from_civil). Avoids [Unix.mktime]/timezone entirely. *)
      let yy = if mo <= 2 then y - 1 else y in
      let era = (if yy >= 0 then yy else yy - 399) / 400 in
      let yoe = yy - (era * 400) in
      let doy = (((153 * if mo > 2 then mo - 3 else mo + 9) + 2) / 5) + d - 1 in
      let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
      let days = (era * 146097) + doe - 719468 in
      let epoch =
        (float_of_int days *. 86400.0)
        +. (float_of_int h *. 3600.0)
        +. (float_of_int mi *. 60.0)
        +. sec
        -. tz_offset
      in
      Some epoch
    with
    | _ -> None)
;;

let record_ping venue span =
  observe_rtt venue (Mtime.Span.to_float_ns span /. 1_000_000_000.0);
  Latency_profiler.record (venue_profilers venue).ping span
;;

let record_feed venue span = Latency_profiler.record (venue_profilers venue).feed span
let record_rest venue span = Latency_profiler.record (venue_profilers venue).rest span
let record_signer venue span = Latency_profiler.record (venue_profilers venue).signer span
let record_ping_s venue s = record_ping venue (span_of_seconds s)
let record_feed_s venue s = record_feed venue (span_of_seconds s)
let record_rest_s venue s = record_rest venue (span_of_seconds s)

(** [record_feed_event_s venue ~event ?recv ()] records one fresh feed message's
    clock-corrected one-way latency. [event] is the server event timestamp in Unix
    seconds; [recv] defaults to the current wall time. Stale/out-of-order samples are
    dropped rather than recorded. *)
let record_feed_event_s venue ~event ?recv () =
  let recv =
    match recv with
    | Some r -> r
    | None -> Unix.gettimeofday ()
  in
  match corrected_one_way venue ~recv ~event with
  | Some lat -> record_feed_s venue lat
  | None -> ()
;;

(** [record_feed_event_ms venue ~event_ms ?recv ()] is [record_feed_event_s] for server
    timestamps expressed in milliseconds since epoch. *)
let record_feed_event_ms venue ~event_ms ?recv () =
  let recv =
    match recv with
    | Some r -> r
    | None -> Unix.gettimeofday ()
  in
  match corrected_one_way venue ~recv ~event:(event_ms /. 1000.0) with
  | Some lat -> record_feed_s venue lat
  | None -> ()
;;

(** Most recent published windows for [venue], in the label order the dashboard's NETWORK
    page expects. Empty when the venue has no profilers yet. *)
let snapshots venue =
  match Hashtbl.find_opt profilers venue with
  | None -> []
  | Some p ->
    [ "ws_ping", Latency_profiler.published_snapshot p.ping
    ; "ws_feed", Latency_profiler.published_snapshot p.feed
    ; "rest_request", Latency_profiler.published_snapshot p.rest
    ; "signer", Latency_profiler.published_snapshot p.signer
    ]
;;

(** All venues with measured activity: (venue, label windows). *)
let all_venue_snapshots () =
  Hashtbl.fold (fun venue _ acc -> (venue, snapshots venue) :: acc) profilers []
;;

(** Network window publication cadence, in seconds. Shared by the background loop and the
    spike-log window label so the two cannot drift. *)
let publish_interval_seconds = 10.0

(** Advance every venue profiler's window, publishing an immutable snapshot for the
    dashboard. Safe from any thread; profilers hold an internal mutex for the atomic
    publish. When [log_spikes] is set, each venue emits at most one INFO line naming the
    network metrics with a sample at or above [threshold_us]. Gated by the caller so the
    network tail can be silenced while internal ops are profiled. *)
let publish_all ?(log_spikes = false) ?(threshold_us = 10.0) () =
  Hashtbl.iter
    (fun venue p ->
      let ping =
        Latency_profiler.snapshot_and_reset ~spike_threshold_us:threshold_us p.ping
      in
      let feed =
        Latency_profiler.snapshot_and_reset ~spike_threshold_us:threshold_us p.feed
      in
      let rest =
        Latency_profiler.snapshot_and_reset ~spike_threshold_us:threshold_us p.rest
      in
      let signer =
        Latency_profiler.snapshot_and_reset ~spike_threshold_us:threshold_us p.signer
      in
      if log_spikes
      then (
        match
          Latency_profiler.spike_message
            ~key:venue
            ~window_seconds:publish_interval_seconds
            ~threshold_us
            [ "WS_PING", ping; "WS_FEED", feed; "REST", rest; "SIGNER", signer ]
        with
        | None -> ()
        | Some msg -> Logging.info_f ~section "%s" msg))
    profilers
;;

(** Background publisher: advance all venue windows every [publish_interval_seconds] so
    the dashboard always has a fresh NETWORK page. Runs as an Lwt fiber; call once at
    engine startup. [log_spikes] controls whether windows also emit spike logs (see
    [publish_all]). *)
let start_publisher ?(log_spikes = false) ?(threshold_us = 10.0) () =
  let rec loop () =
    Lwt_unix.sleep publish_interval_seconds
    >>= fun () ->
    publish_all ~log_spikes ~threshold_us ();
    loop ()
  in
  Lwt.async loop
;;
