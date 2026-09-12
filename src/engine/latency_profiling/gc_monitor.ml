(** GC monitoring utilities for correlating latency spikes with collector activity. *)

type gc_stats =
  { minor_collections : int
  ; major_collections : int
  ; forced_major_collections : int
  ; compactions : int
  ; promoted_words : int
  ; major_words : int
  }

(** Zero-initialized stats, used as the "not sampling" sentinel so a
    per-cycle capture can be skipped without allocating an option. *)
let zero =
  { minor_collections = 0
  ; major_collections = 0
  ; forced_major_collections = 0
  ; compactions = 0
  ; promoted_words = 0
  ; major_words = 0
  }
;;

(** Returns a snapshot of current GC counters. [Gc.quick_stat] is ~0.3us and
    allocates its stat record (~24 words); it is cheap enough to take twice per
    busy cycle for per-cycle GC attribution, but note that these counters are
    per-domain: a domain merely PAUSED by another domain's major collection
    observes no delta of its own. Detecting that cross-domain stop-the-world
    pause is the job of {!Canary}, not of this per-cycle attribution. *)
let[@inline] get_stats () =
  let stat = Gc.quick_stat () in
  { minor_collections = stat.minor_collections
  ; major_collections = stat.major_collections
  ; forced_major_collections = stat.forced_major_collections
  ; compactions = stat.compactions
  ; promoted_words = int_of_float stat.promoted_words
  ; major_words = int_of_float stat.major_words
  }
;;

(** Formats the difference between two stats into a cause string. A major
    collection (or compaction) is always reported with its promotion volume,
    since the promotion scan is the part that follows an application into the
    stop-the-world pause. *)
let diff_to_string start_stats end_stats =
  let minor = end_stats.minor_collections - start_stats.minor_collections in
  let major = end_stats.major_collections - start_stats.major_collections in
  let forced =
    end_stats.forced_major_collections - start_stats.forced_major_collections
  in
  let comp = end_stats.compactions - start_stats.compactions in
  let promoted = end_stats.promoted_words - start_stats.promoted_words in
  let major_words = end_stats.major_words - start_stats.major_words in
  if major > 0 || comp > 0 || forced > 0
  then
    Printf.sprintf
      " (GC: major=%d forced=%d comp=%d prom=%dw majorw=%dw minor=%d)"
      major
      forced
      comp
      promoted
      major_words
      minor
  else if minor > 0
  then Printf.sprintf " (GC: minor=%d)" minor
  else ""
;;
