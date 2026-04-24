(** GC monitoring utilities for correlating latency spikes with collector activity. *)

type gc_stats = {
  minor_collections : int;
  major_collections : int;
  compactions : int;
}

(** Returns a snapshot of current GC collection counts. 
    Uses [Gc.quick_stat] for minimal overhead on the hot path. *)
let[@inline] get_stats () =
  let stat = Gc.quick_stat () in
  {
    minor_collections = stat.minor_collections;
    major_collections = stat.major_collections;
    compactions = stat.compactions;
  }

(** Formats the difference between two stats into a cause string. *)
let diff_to_string start_stats end_stats =
  let minor = end_stats.minor_collections - start_stats.minor_collections in
  let major = end_stats.major_collections - start_stats.major_collections in
  let comp = end_stats.compactions - start_stats.compactions in
  if major > 0 || comp > 0 then
    Printf.sprintf " (GC: major=%d comp=%d minor=%d)" major comp minor
  else if minor > 0 then
    Printf.sprintf " (GC: minor=%d)" minor
  else
    ""
