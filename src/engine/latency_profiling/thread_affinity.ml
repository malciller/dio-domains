(** CPU-affinity placement for the trading domains.

    The engine runs one OCaml domain per asset plus a few background domains. On a hybrid
    (P-core/E-core) host the scheduler is free to migrate a busy domain between core types
    and to preempt it against other runnable threads, which is what turns a fast cycle
    into a multi-hundred-microsecond p999 stall. This module pins each domain to a chosen
    CPU so the operator can keep the P-cores for the trading domains and everything else
    on the E-cores.

    Placement is configured with [DIO_TRADING_CPUS] (a comma-separated CPU list, e.g.
    [0,2,4,6,8,10]); when unset it is auto-detected from the sysfs topology (the CPUs
    whose [cpuinfo_max_freq] is at the package maximum, one logical CPU per physical
    core). With no [DIO_BACKGROUND_CPUS], background work is left where it is unless a
    background range is given. All calls are best-effort: a failure to pin is logged and
    ignored, never fatal. *)

external pin_current_thread : int -> bool = "dio_pin_current_thread"
external pin_current_thread_range : int -> int -> bool = "dio_pin_current_thread_range"
external allowed_cpu_count : unit -> int = "dio_allowed_cpu_count"
external set_current_idle : unit -> bool = "dio_set_current_idle"

let ( let* ) = Option.bind

let read_file path =
  try
    let ic = open_in path in
    Fun.protect
      ~finally:(fun () -> close_in_noerr ic)
      (fun () ->
        try Some (String.trim (input_line ic)) with
        | End_of_file -> None)
  with
  | Sys_error _ -> None
;;

let max_freq_khz cpu =
  match
    read_file
      (Printf.sprintf "/sys/devices/system/cpu/cpu%d/cpufreq/cpuinfo_max_freq" cpu)
  with
  | Some s ->
    (try Some (int_of_string s) with
     | _ -> None)
  | None -> None
;;

let core_id cpu =
  match
    read_file (Printf.sprintf "/sys/devices/system/cpu/cpu%d/topology/core_id" cpu)
  with
  | Some s ->
    (try Some (int_of_string s) with
     | _ -> None)
  | None -> None
;;

let package_id cpu =
  match
    read_file
      (Printf.sprintf "/sys/devices/system/cpu/cpu%d/topology/physical_package_id" cpu)
  with
  | Some s ->
    (try Some (int_of_string s) with
     | _ -> None)
  | None -> None
;;

let online_cpus () =
  let n = max 1 (allowed_cpu_count ()) in
  List.init n Fun.id
;;

(** The CPUs that sit on a P-core: one logical CPU per physical core, chosen as the
    lowest-numbered sibling, restricted to the cores whose max frequency equals the
    package maximum. Returns [[]] when the topology/frequency files are unavailable (e.g.
    a VM), in which case the caller leaves affinity alone. *)
let detect_p_core_cpus () =
  let cpus = online_cpus () in
  let freqs =
    List.filter_map (fun c -> Option.map (fun f -> c, f) (max_freq_khz c)) cpus
  in
  match freqs with
  | [] -> []
  | _ ->
    let top = List.fold_left (fun a (_, f) -> max a f) 0 freqs in
    (* P-cores sit within ~15% of the top turbo bin; E-cores fall well below (3.5 vs 4.7
       GHz on the i7-12650H, which itself reports two P-core bins). An exact match on
       [top] would keep only one bin and drop half the P-cores. *)
    let is_p f = f * 100 >= top * 85 in
    (* Group the P-core CPUs by physical core, keep the lowest sibling of each. *)
    let seen = Hashtbl.create 16 in
    let cores =
      List.filter_map
        (fun (c, f) ->
          if not (is_p f)
          then None
          else (
            match package_id c, core_id c with
            | Some p, Some k ->
              let key = p, k in
              if Hashtbl.mem seen key
              then None
              else (
                Hashtbl.replace seen key ();
                Some c)
            | _ -> Some c))
        (List.sort compare freqs)
    in
    List.sort compare cores
;;

let parse_cpu_list s =
  String.split_on_char ',' s
  |> List.filter_map (fun part ->
    let part = String.trim part in
    if part = ""
    then None
    else (
      try Some (int_of_string part) with
      | _ -> None))
;;

(** The trading CPU list to hand out round-robin: [DIO_TRADING_CPUS] if set, else the
    detected P-cores, else [[]] (no pinning). *)
let trading_cpus () =
  match Sys.getenv_opt "DIO_TRADING_CPUS" with
  | Some s -> parse_cpu_list s
  | None -> detect_p_core_cpus ()
;;

(** Pin the calling domain to [cpu] (best-effort). *)
let pin_self cpu =
  if cpu < 0
  then ()
  else if not (pin_current_thread cpu)
  then
    Logging.debug_f
      ~section:"affinity"
      "could not pin current thread to cpu %d (continuing unpinned)"
      cpu
;;

(** Round-robin allocator over the trading CPU list. [next ()] returns the next CPU, or
    [-1] when no list is configured. *)
let make_allocator () =
  let cpus = Array.of_list (trading_cpus ()) in
  let n = Array.length cpus in
  let i = ref 0 in
  fun () ->
    if n = 0
    then -1
    else (
      let c = cpus.(!i mod n) in
      incr i;
      c)
;;

(** The background CPU list: [DIO_BACKGROUND_CPUS] if set, else every online CPU that is
    not a trading CPU (i.e. the E-cores and the unused P-core siblings). *)
let background_cpus () =
  match Sys.getenv_opt "DIO_BACKGROUND_CPUS" with
  | Some s -> parse_cpu_list s
  | None ->
    let trading = trading_cpus () in
    List.filter (fun c -> not (List.mem c trading)) (online_cpus ())
;;

(** Round-robin allocator over the background CPU list. *)
let make_background_allocator () =
  let cpus = Array.of_list (background_cpus ()) in
  let n = Array.length cpus in
  let i = ref 0 in
  fun () ->
    if n = 0
    then -1
    else (
      let c = cpus.(!i mod n) in
      incr i;
      c)
;;

(** Shared background allocator so distinct background domains/threads land on distinct
    (E-)cores instead of all piling onto the first one. *)
let background_alloc = lazy (make_background_allocator ())

(** Pin the calling thread to a background CPU (best-effort): keeps feed parsing,
    persistence and the supervisor off the trading P-cores so they cannot preempt a
    trading cycle. *)
let pin_self_background () = pin_self (Lazy.force background_alloc ())

(** Pin the calling domain to a fixed background range [lo, hi] (best-effort). Used to
    keep the busy-spin canary and other polling work off the trading cores. *)
let pin_self_to_range lo hi =
  if lo > hi || lo < 0
  then ()
  else if not (pin_current_thread_range lo hi)
  then
    Logging.debug_f
      ~section:"affinity"
      "could not pin current thread to cpus %d-%d (continuing unpinned)"
      lo
      hi
;;

(** Drop the calling thread to SCHED_IDLE (best-effort): it then only runs when nothing
    else is runnable. Intended for pure busy-spin helpers so they can never preempt a
    trading cycle. *)
let set_self_idle () =
  if not (set_current_idle ())
  then
    Logging.debug_f
      ~section:"affinity"
      "could not set SCHED_IDLE on current thread (continuing with default policy)"
;;

let describe () =
  let t = trading_cpus () in
  Printf.sprintf
    "affinity: trading cpus=[%s] (allowed=%d)"
    (String.concat "," (List.map string_of_int t))
    (allowed_cpu_count ())
;;
