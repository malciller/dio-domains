(** Live survivor profiler for leak hunting.

    [Gc.Memprof] samples allocations and, via the tracker callbacks, lets us follow each
    sampled block from allocation through promotion to deallocation. Counting
    [+1 on alloc, -1 on dealloc] per allocation site yields the *live* sample count per
    site, so the leaking allocation site (the one whose live count grows over time)
    surfaces directly — no guessing about which hashtable queue is unbounded.

    Enabled with [DIO_MEMPROF_LIVE=1]; sampling rate [DIO_MEMPROF_RATE] (default 1e-4).
    [report ()] logs the top live sites and their growth since the previous report. All
    dominated by survivor count, not total allocation, so a steady high-traffic site that
    does not leak stays flat.

    Diagnostics only: the callbacks run on the allocating thread and add overhead, so the
    production builds leave this off. *)

let section = "leak_probe"
let started = Atomic.make false
let live : (string, int) Hashtbl.t = Hashtbl.create 256
let prev : (string, int) Hashtbl.t = Hashtbl.create 256
let mutex = Mutex.create ()

let frame_of (a : Gc.Memprof.allocation) =
  let entries = Printexc.raw_backtrace_entries a.callstack in
  let rec go i =
    if i >= Array.length entries
    then "<no-debug-info>"
    else (
      match Printexc.backtrace_slots_of_raw_entry entries.(i) with
      | Some slots when Array.length slots > 0 ->
        let s = slots.(0) in
        let name =
          match Printexc.Slot.name s with
          | Some n -> n
          | None -> "?"
        in
        let loc =
          match Printexc.Slot.location s with
          | Some l -> Printf.sprintf "%s:%d" (Filename.basename l.filename) l.line_number
          | None -> "?"
        in
        name ^ " @ " ^ loc
      | _ -> go (i + 1))
  in
  go 0
;;

(* [try_lock]: the memprof callback can fire on a thread that already holds [mutex] - the
   report path allocates while holding it and sampling is active in that thread, so the
   alloc callback re-enters here. A blocking lock then raises
   [Sys_error "Mutex.lock: Resource deadlock avoided"]. Dropping the sample on contention
   is correct for a profiler. *)
let bump site d =
  if Mutex.try_lock mutex
  then (
    Hashtbl.replace
      live
      site
      ((try Hashtbl.find live site with
        | Not_found -> 0)
       + d);
    Mutex.unlock mutex)
;;

let enabled () = Sys.getenv_opt "DIO_MEMPROF_LIVE" <> None

let start () =
  if enabled () && not (Atomic.exchange started true)
  then (
    let rate =
      match Sys.getenv_opt "DIO_MEMPROF_RATE" with
      | Some s ->
        (try float_of_string s with
         | _ -> 1e-4)
      | None -> 1e-4
    in
    let tracker : (string, string) Gc.Memprof.tracker =
      { alloc_minor =
          (fun a ->
            let k = frame_of a in
            bump k 1;
            Some k)
      ; alloc_major =
          (fun a ->
            let k = frame_of a in
            bump k 1;
            Some k)
      ; promote = (fun k -> Some k)
      ; dealloc_minor = (fun k -> bump k (-1))
      ; dealloc_major = (fun k -> bump k (-1))
      }
    in
    ignore (Gc.Memprof.start ~sampling_rate:rate ~callstack_size:10 tracker);
    Logging.info_f ~section "live survivor profiler started (rate %g)" rate)
;;

(** Log the sites with the most live samples and their growth since the last call. The
    leaked site shows both a large absolute count and steady positive growth. *)
let report () =
  if Atomic.get started && Mutex.try_lock mutex
  then (
    (* Never let a diagnostic crash the trading loop. *)
    try
      let rows = Hashtbl.fold (fun k v acc -> (k, v) :: acc) live [] in
      let prevc k =
        try Hashtbl.find prev k with
        | Not_found -> 0
      in
      Hashtbl.reset prev;
      Hashtbl.iter (fun k v -> Hashtbl.replace prev k v) live;
      Mutex.unlock mutex;
      let rows = List.sort (fun (_, a) (_, b) -> compare b a) rows in
      Logging.info_f ~section "live survivor sites (samples; growth since last report):";
      List.iteri
        (fun i (k, v) ->
          if i < 12 then Logging.info_f ~section "  %6d  (+%d)  %s" v (v - prevc k) k)
        rows
    with
    | e ->
      (try Mutex.unlock mutex with
       | _ -> ());
      Logging.warn_f ~section "leak probe report failed: %s" (Printexc.to_string e))
;;
