(** Per-venue network latency profilers.

    Measures the four NETWORK-page metrics the dashboard displays under each
    domain row (a domain's venue):
    - ws_ping:      venue WebSocket ping/pong round trip
    - ws_feed:      gap between consecutive market-data feed messages
    - rest_request: HTTP REST round trip (trading actions and oracle fetches)
    - signer:       local signature generation time

    Profilers are keyed by venue name ("hyperliquid", "kraken", "lighter",
    "alpaca") so every connection/fetch on a venue feeds one histogram. The
    dashboard merges these per-venue windows into the per-symbol latency map
    by looking up each configured symbol's exchange.

    Windows are published (snapshot_and_reset) every ~10s by [start_publisher]
    — well inside the dashboard's 15s freshness tolerance — so the NETWORK
    page always has a fresh window per metric, even when a venue is idle
    (which then reads as "idle", not "--"). *)

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

let venue_profilers venue =
  Mutex.lock mutex;
  let p =
    match Hashtbl.find_opt profilers venue with
    | Some p -> p
    | None ->
      let p =
        { ping =
            Latency_profiler.create
              ~bucket_us:1
              ~max_latency_us:2_000_000
              (venue ^ ":ws_ping")
        ; feed =
            Latency_profiler.create
              ~bucket_us:100
              ~max_latency_us:2_000_000
              (venue ^ ":ws_feed")
        ; rest =
            Latency_profiler.create
              ~bucket_us:100
              ~max_latency_us:2_000_000
              (venue ^ ":rest_request")
        ; signer =
            Latency_profiler.create
              ~bucket_us:1
              ~max_latency_us:100_000
              (venue ^ ":signer")
        }
      in
      Hashtbl.replace profilers venue p;
      p
  in
  Mutex.unlock mutex;
  p
;;

(** A span of [seconds] (wall clock delta), for call sites that time with
    [Unix.gettimeofday]. *)
let span_of_seconds s = Mtime.Span.of_uint64_ns (Int64.of_float (s *. 1_000_000_000.0))

let record_ping venue span = Latency_profiler.record (venue_profilers venue).ping span
let record_feed venue span = Latency_profiler.record (venue_profilers venue).feed span
let record_rest venue span = Latency_profiler.record (venue_profilers venue).rest span
let record_signer venue span = Latency_profiler.record (venue_profilers venue).signer span
let record_ping_s venue s = record_ping venue (span_of_seconds s)
let record_feed_s venue s = record_feed venue (span_of_seconds s)
let record_rest_s venue s = record_rest venue (span_of_seconds s)

(** Most recent published windows for [venue], in the label order the
    dashboard's NETWORK page expects. Empty when the venue has no profilers
    yet (nothing measured). *)
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

(** Advances the window of every venue profiler, publishing an immutable
    snapshot for the dashboard. Safe to call from any thread; profilers use
    the internal mutex for the atomic publish. *)
let publish_all () =
  Hashtbl.iter
    (fun _ p ->
       ignore (Latency_profiler.snapshot_and_reset p.ping);
       ignore (Latency_profiler.snapshot_and_reset p.feed);
       ignore (Latency_profiler.snapshot_and_reset p.rest);
       ignore (Latency_profiler.snapshot_and_reset p.signer))
    profilers
;;

(** Background window publisher: advances all venue windows every 10s so the
    dashboard always has a fresh NETWORK page. Runs as an Lwt fiber; call
    once from engine startup. *)
let start_publisher () =
  let rec loop () =
    Lwt_unix.sleep 10.0
    >>= fun () ->
    publish_all ();
    loop ()
  in
  Lwt.async loop
;;
