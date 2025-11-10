(** Leak Detector - Analyzes allocation patterns to identify memory leaks

    Monitors allocation trends, detects unbounded growth, and identifies
    potential memory leaks in data structures and domains.
*)

(** Helper function to take first n elements from a list *)
let rec take n = function
  | [] -> []
  | x :: xs -> if n <= 0 then [] else x :: take (n - 1) xs

(** Leak detection result *)
type leak_result = {
  structure_type: string;
  allocation_site: Allocation_tracker.allocation_site;
  domain_id: int;
  severity: [`Low | `Medium | `High | `Critical];
  growth_rate: float;  (* bytes/second *)
  current_size: int;
  time_window: float;  (* seconds over which growth was measured *)
  description: string;
  recommendations: string list;
}

(** Structure growth tracking *)
type growth_tracker = {
  structure_type: string;
  allocation_site: Allocation_tracker.allocation_site;
  domain_id: int;
  mutable samples: (float * int) list;  (* (timestamp, size) pairs *)
  mutable last_alert_time: float;
}

(** Global leak detection state *)
let active_allocations : (int, Allocation_tracker.allocation_record) Allocation_tracker.TrackedHashtbl.t = Allocation_tracker.TrackedHashtbl.create 1024
let growth_trackers : (string, growth_tracker) Allocation_tracker.TrackedHashtbl.t = Allocation_tracker.TrackedHashtbl.create 256
let leak_results : leak_result list ref = ref []
let detector_mutex = Mutex.create ()


(** Growth tracker limits to prevent unbounded growth *)
let max_growth_trackers = 500  (* Maximum number of growth trackers to maintain *)
let tracker_stale_seconds = 3600.0  (* Remove trackers not updated in 1 hour *)

(** Generate tracker key *)
let tracker_key structure_type (site : Allocation_tracker.allocation_site) domain_id =
  Printf.sprintf "%s:%s:%d:%d:%s" structure_type site.file domain_id site.line site.function_name

(** Register allocation for leak detection *)
let register_allocation record =
  Mutex.lock detector_mutex;
  Allocation_tracker.TrackedHashtbl.add active_allocations record.Allocation_tracker.id record;
  Mutex.unlock detector_mutex

(** Register deallocation for leak detection *)
let register_deallocation id =
  Mutex.lock detector_mutex;
  Allocation_tracker.TrackedHashtbl.remove active_allocations id;
  Mutex.unlock detector_mutex

(** Update growth tracker with new sample *)
let update_growth_tracker tracker timestamp size =
  let new_samples = (timestamp, size) :: tracker.samples in
  (* Keep only last 20 samples to prevent unbounded growth *)
  let trimmed_samples = if List.length new_samples > 20 then
    take 20 new_samples
  else new_samples in
  tracker.samples <- trimmed_samples

(** Analyze growth pattern and detect leaks *)
let analyze_growth growth_tracker : leak_result option =
  match growth_tracker.samples with
  | [] | [_] -> None  (* Need at least 2 samples *)
  | samples ->
      let sorted = List.sort (fun (t1, _) (t2, _) -> Float.compare t1 t2) samples in
      let earliest = List.hd sorted in
      let latest = List.hd (List.rev sorted) in
      let time_diff = fst latest -. fst earliest in
      let size_diff = snd latest - snd earliest in

      if time_diff < 60.0 then None  (* Need at least 1 minute of data *)
      else begin
        let growth_rate = float_of_int size_diff /. time_diff in
        (* Get current size from the most recent sample, but also check if we have
           a more current size from the active allocation record *)
        let base_current_size = snd latest in
        let current_size = base_current_size  (* For now, use sample data - will be improved with proper size tracking *)
        in

        (* Determine severity based on growth rate and size *)
        let severity, description, recommendations =
          if growth_rate > 1000.0 && current_size > 10000 then
            (`Critical,
             Printf.sprintf "Critical leak: %.0f bytes/sec growth rate, %d current size" growth_rate current_size,
             ["Immediate investigation required"; "Check for missing cleanup calls"; "Consider reducing data retention"])
          else if growth_rate > 100.0 && current_size > 1000 then
            (`High,
             Printf.sprintf "High leak risk: %.0f bytes/sec growth rate, %d current size" growth_rate current_size,
             ["Investigate allocation patterns"; "Verify cleanup is called"; "Monitor closely"])
          else if growth_rate > 10.0 && current_size > 100 then
            (`Medium,
             Printf.sprintf "Moderate leak: %.0f bytes/sec growth rate, %d current size" growth_rate current_size,
             ["Monitor growth rate"; "Check for expected behavior"; "Consider optimization"])
          else if growth_rate > 1.0 then
            (`Low,
             Printf.sprintf "Slow leak: %.0f bytes/sec growth rate, %d current size" growth_rate current_size,
             ["Monitor long-term trends"; "Verify expected behavior"])
          else
            (`Low, "", [])  (* No leak detected *)

        in
        match severity with
        | `Low when description = "" -> None  (* No leak *)
        | `Low | `Medium | `High | `Critical ->
            Some {
              structure_type = growth_tracker.structure_type;
              allocation_site = growth_tracker.allocation_site;
              domain_id = growth_tracker.domain_id;
              severity;
              growth_rate;
              current_size;
              time_window = time_diff;
              description;
              recommendations;
            }
      end

(** Process allocation event for leak detection *)
let process_allocation_event event =
  match event with
  | Allocation_tracker.Allocation record ->
      register_allocation record;

      (* Update growth tracker *)
      let key = tracker_key record.structure_type record.site record.domain_id in
      let timestamp = record.timestamp in
      let size = record.size in

      Mutex.lock detector_mutex;
      let tracker = match Allocation_tracker.TrackedHashtbl.find_opt growth_trackers key with
        | Some t -> t
        | None ->
            let t = {
              structure_type = record.structure_type;
              allocation_site = record.site;
              domain_id = record.domain_id;
              samples = [];
              last_alert_time = 0.0;
            } in
            Allocation_tracker.TrackedHashtbl.add growth_trackers key t;
            t
      in
      update_growth_tracker tracker timestamp size;
      Mutex.unlock detector_mutex

  | Allocation_tracker.Deallocation { id; _ } ->
      register_deallocation id

  | Allocation_tracker.SizeUpdate { id; new_size; timestamp; _ } ->
      (* Update the active allocation record and growth tracker *)
      Mutex.lock detector_mutex;
      (match Allocation_tracker.TrackedHashtbl.find_opt active_allocations id with
       | Some record ->
           (* Update growth tracker with new size *)
           let key = tracker_key record.structure_type record.site record.domain_id in
           (match Allocation_tracker.TrackedHashtbl.find_opt growth_trackers key with
            | Some tracker -> update_growth_tracker tracker timestamp new_size
            | None -> ())
       | None -> ());
      Mutex.unlock detector_mutex

(** Clean up stale growth trackers to prevent unbounded growth *)
let cleanup_growth_trackers () =
  Mutex.lock detector_mutex;
  let now = Unix.gettimeofday () in
  let stale_cutoff = now -. tracker_stale_seconds in

  (* Collect keys to remove *)
  let keys_to_remove = ref [] in
  Allocation_tracker.TrackedHashtbl.iter (fun key tracker ->
    (* Remove trackers that haven't been updated in the stale period *)
    let last_sample_time = match tracker.samples with
      | [] -> 0.0  (* Never updated *)
      | samples -> fst (List.hd (List.sort (fun (t1, _) (t2, _) -> Float.compare t2 t1) samples))
    in
    if last_sample_time < stale_cutoff then
      keys_to_remove := key :: !keys_to_remove
  ) growth_trackers;

  (* Remove stale trackers *)
  let stale_removed = List.length !keys_to_remove in
  List.iter (Allocation_tracker.TrackedHashtbl.remove growth_trackers) !keys_to_remove;

  (* If still over limit, remove oldest trackers *)
  let current_count = Allocation_tracker.TrackedHashtbl.length growth_trackers in
  let to_remove_more = max 0 (current_count - max_growth_trackers) in
  if to_remove_more > 0 then begin
    (* Collect all trackers with their last update times *)
    let trackers_with_times = ref [] in
    Allocation_tracker.TrackedHashtbl.iter (fun key tracker ->
      let last_sample_time = match tracker.samples with
        | [] -> 0.0
        | samples -> fst (List.hd (List.sort (fun (t1, _) (t2, _) -> Float.compare t2 t1) samples))
      in
      trackers_with_times := (key, last_sample_time) :: !trackers_with_times
    ) growth_trackers;

    (* Sort by last update time (oldest first) and remove oldest *)
    let sorted = List.sort (fun (_, t1) (_, t2) -> Float.compare t1 t2) !trackers_with_times in
    let oldest_to_remove = List.map fst (List.filteri (fun i _ -> i < to_remove_more) sorted) in
    List.iter (Allocation_tracker.TrackedHashtbl.remove growth_trackers) oldest_to_remove;
  end;

  let final_count = Allocation_tracker.TrackedHashtbl.length growth_trackers in
  Mutex.unlock detector_mutex;

  if stale_removed > 0 || to_remove_more > 0 then
    Logging.info_f ~section:"leak_detector" "Cleaned up growth trackers: removed %d stale + %d oldest, total trackers: %d/%d"
      stale_removed to_remove_more final_count max_growth_trackers

(** Analyze current allocation patterns for leaks *)
let analyze_for_leaks () =
  Mutex.lock detector_mutex;
  let current_time = Unix.gettimeofday () in

  (* Clean up stale growth trackers periodically *)
  cleanup_growth_trackers ();
  let new_leaks = ref [] in

  (* Analyze each growth tracker *)
  Allocation_tracker.TrackedHashtbl.iter (fun _ tracker ->
    (* Only analyze if we haven't alerted recently (prevent spam) *)
    if current_time -. tracker.last_alert_time > 300.0 then begin  (* 5 minutes between alerts *)
      match analyze_growth tracker with
      | Some leak ->
          tracker.last_alert_time <- current_time;
          new_leaks := leak :: !new_leaks
      | None -> ()
    end
  ) growth_trackers;

  (* Update global leak results *)
  leak_results := !new_leaks @ !leak_results;

  (* Keep only recent leaks (last 100) *)
  if List.length !leak_results > 100 then
    leak_results := take 100 !leak_results;

  let results = !new_leaks in
  Mutex.unlock detector_mutex;
  results

(** Get active allocation count by type *)
let get_allocation_counts_by_type () =
  Mutex.lock detector_mutex;
  let counts : (string, int) Allocation_tracker.TrackedHashtbl.t = Allocation_tracker.TrackedHashtbl.create 16 in

  Allocation_tracker.TrackedHashtbl.iter (fun _ record ->
    let current = Allocation_tracker.TrackedHashtbl.find_opt counts record.Allocation_tracker.structure_type |> Option.value ~default:0 in
    Allocation_tracker.TrackedHashtbl.replace counts record.structure_type (current + 1)
  ) active_allocations;

  let result = Allocation_tracker.TrackedHashtbl.fold (fun typ count acc -> (typ, count) :: acc) counts [] in
  Allocation_tracker.TrackedHashtbl.destroy counts;
  Mutex.unlock detector_mutex;
  result

(** Get allocation counts by domain *)
let get_allocation_counts_by_domain () =
  Mutex.lock detector_mutex;
  let counts : (int, int) Allocation_tracker.TrackedHashtbl.t = Allocation_tracker.TrackedHashtbl.create 16 in

  Allocation_tracker.TrackedHashtbl.iter (fun _ record ->
    let current = Allocation_tracker.TrackedHashtbl.find_opt counts record.Allocation_tracker.domain_id |> Option.value ~default:0 in
    Allocation_tracker.TrackedHashtbl.replace counts record.domain_id (current + 1)
  ) active_allocations;

  let result = Allocation_tracker.TrackedHashtbl.fold (fun domain_id count acc -> (domain_id, count) :: acc) counts [] in
  Allocation_tracker.TrackedHashtbl.destroy counts;
  Mutex.unlock detector_mutex;
  result

(** Get allocation counts by function per domain *)
let get_allocation_counts_by_function_per_domain () =
  Mutex.lock detector_mutex;
  let counts : (string, (string, int) Allocation_tracker.TrackedHashtbl.t) Allocation_tracker.TrackedHashtbl.t = Allocation_tracker.TrackedHashtbl.create 16 in

  Allocation_tracker.TrackedHashtbl.iter (fun _ record ->
    let domain_key = string_of_int record.Allocation_tracker.domain_id in
    let function_counts = Allocation_tracker.TrackedHashtbl.find_opt counts domain_key |> Option.value ~default:(Allocation_tracker.TrackedHashtbl.create 64) in
    let current = Allocation_tracker.TrackedHashtbl.find_opt function_counts record.site.Allocation_tracker.function_name |> Option.value ~default:0 in
    Allocation_tracker.TrackedHashtbl.replace function_counts record.site.function_name (current + 1);
    Allocation_tracker.TrackedHashtbl.replace counts domain_key function_counts
  ) active_allocations;

  let result = Allocation_tracker.TrackedHashtbl.fold (fun domain_str function_counts acc ->
    let domain_id = int_of_string domain_str in
    let function_list = Allocation_tracker.TrackedHashtbl.fold (fun func_name count func_acc -> (func_name, count) :: func_acc) function_counts [] in
    Allocation_tracker.TrackedHashtbl.destroy function_counts;
    (domain_id, function_list) :: acc
  ) counts [] in

  Allocation_tracker.TrackedHashtbl.destroy counts;
  Mutex.unlock detector_mutex;
  result

(** Detect domain-specific memory issues *)
let detect_domain_memory_issues () =
  let domain_counts = get_allocation_counts_by_domain () in
  let domain_perfs = Domain_profiler.get_domain_performance_metrics () in

  List.map (fun perf ->
    let alloc_count = List.assoc_opt perf.Domain_profiler.domain_id domain_counts |> Option.value ~default:0 in
    let pressure_score = match perf.pressure with
      | `Normal -> 0
      | `Moderate -> 1
      | `High -> 2
      | `Critical -> 3
    in

    (* High allocation count + high memory pressure = potential domain leak *)
    if alloc_count > 1000 && pressure_score >= 2 then
      Some {
        structure_type = "DomainMemory";
        allocation_site = {
          file = "domain";
          function_name = Printf.sprintf "domain_%d" perf.domain_id;
          line = 0;
          backtrace = "";
        };
        domain_id = perf.domain_id;
        severity = if pressure_score = 3 then `Critical else `High;
        growth_rate = perf.alloc_rate;
        current_size = alloc_count;
        time_window = perf.uptime;
        description = Printf.sprintf "Domain %d: %d allocations, %.0f alloc/sec, memory pressure %s"
          perf.domain_id alloc_count perf.alloc_rate
          (match perf.pressure with `Normal -> "normal" | `Moderate -> "moderate" | `High -> "high" | `Critical -> "critical");
        recommendations = [
          "Check for unbounded data structures in domain";
          "Verify cleanup of domain-specific resources";
          "Consider domain restart if leak persists";
        ];
      }
    else None
  ) domain_perfs |> List.filter_map Fun.id

(** Status summary record *)
type status_summary = {
  recent_leaks: int;
  domain_issues: int;
  total_active_allocations: int;
  allocation_types: (string * int) list;
  critical_leaks: int;
  high_leaks: int;
}

(** Get current leak status summary *)
let get_leak_status_summary () =
  let leaks = analyze_for_leaks () in
  let domain_issues = detect_domain_memory_issues () in
  let type_counts = get_allocation_counts_by_type () in

  {
    recent_leaks = List.length leaks;
    domain_issues = List.length domain_issues;
    total_active_allocations = Allocation_tracker.TrackedHashtbl.length active_allocations;
    allocation_types = type_counts;
    critical_leaks = List.length (List.filter (fun l -> l.severity = `Critical) leaks);
    high_leaks = List.length (List.filter (fun l -> l.severity = `High) leaks);
  }

(** Get detailed leak report *)
let get_detailed_leak_report () =
  Mutex.lock detector_mutex;
  let leaks = !leak_results in
  Mutex.unlock detector_mutex;

  (* Sort by severity (critical first) *)
  let severity_order = function
    | `Critical -> 4
    | `High -> 3
    | `Medium -> 2
    | `Low -> 1
  in
  List.sort (fun l1 l2 -> compare (severity_order l2.severity) (severity_order l1.severity)) leaks

(** Clear old leak results (for testing) *)
let clear_leak_results () =
  Mutex.lock detector_mutex;
  leak_results := [];
  Mutex.unlock detector_mutex

(** Get current growth trackers count for monitoring *)
let get_growth_trackers_count () =
  Mutex.lock detector_mutex;
  let count = Allocation_tracker.TrackedHashtbl.length growth_trackers in
  Mutex.unlock detector_mutex;
  count

(** Initialize leak detector - register callbacks *)
let init () =
  Allocation_tracker.add_callback process_allocation_event;
  Logging.info ~section:"leak_detector" "Leak detector initialized"
