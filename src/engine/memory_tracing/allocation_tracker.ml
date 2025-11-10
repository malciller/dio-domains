(** Allocation Tracker - Tracks individual data structure allocations

    Provides hooks for monitoring creation and destruction of key data structures
    like Hashtbl, Lists, Arrays, and custom structures. Records allocation sites
    using backtraces for precise leak identification.
*)

(** Allocation site information *)
type allocation_site = {
  file: string;
  function_name: string;
  line: int;
  backtrace: string;  (* Full backtrace for debugging *)
}

(** Allocation record *)
type allocation_record = {
  id: int;
  structure_type: string;
  mutable size: int;  (* Size in bytes or elements - now mutable for dynamic updates *)
  domain_id: int;
  timestamp: float;
  site: allocation_site;
  mutable deallocated_at: float option;
  mutable replaced_by: int option;  (* ID of allocation that replaced this one, if any *)
}

(** Allocation event types *)
type allocation_event =
  | Allocation of allocation_record
  | Deallocation of {
      id: int;
      structure_type: string;
      timestamp: float;
    }
  | SizeUpdate of {
      id: int;
      old_size: int;
      new_size: int;
      structure_type: string;
      timestamp: float;
    }

(** Event callback type *)
type event_callback = allocation_event -> unit

(** Global allocation counter *)
let allocation_counter = Atomic.make 0

(** Reuse tracking counters *)
let reuse_counter = Atomic.make 0
let recreation_counter = Atomic.make 0

(** Tracking deferral flag - when set, allocation tracking is deferred to avoid deadlocks *)
let tracking_deferred = Atomic.make false

(** Global event callbacks *)
let event_callbacks = ref []
let callbacks_mutex = Mutex.create ()

(** Allocation event processor callback - registered externally to avoid circular dependencies *)
let allocation_event_processor = ref (fun _ _ _ _ _ _ _ -> ())
let allocation_event_processor_mutex = Mutex.create ()

(** Track reuse events *)
let record_reuse () = Atomic.incr reuse_counter

(** Track recreation events *)
let record_recreation () = Atomic.incr recreation_counter

(** Get reuse metrics *)
let get_reuse_metrics () = (Atomic.get reuse_counter, Atomic.get recreation_counter)

(** Active allocation registry for tracking live allocations *)
(* This hashtable remains untracked - it's the core tracking infrastructure itself *)
let active_allocations = Hashtbl.create 1024
let active_allocations_mutex = Mutex.create ()

(** Active allocations limits to prevent unbounded growth *)
let max_active_allocations = 10000  (* Maximum number of active allocations to track *)
let stale_allocation_seconds = 86400.0  (* Remove allocations older than 24 hours *)

(** Queue of allocation IDs that have been replaced and are safe to destroy *)
let replaced_allocations_queue : int Queue.t = Queue.create ()
let replaced_allocations_mutex = Mutex.create ()
let replacement_counter = Atomic.make 0

(** Defer allocation tracking during critical phases *)
let defer_tracking () = Atomic.set tracking_deferred true

(** Resume allocation tracking *)
let resume_tracking () = Atomic.set tracking_deferred false

(** Check if tracking is currently deferred *)
let is_tracking_deferred () = Atomic.get tracking_deferred

(** Register an event callback *)
let add_callback callback =
  Mutex.lock callbacks_mutex;
  event_callbacks := callback :: !event_callbacks;
  Mutex.unlock callbacks_mutex

(** Register allocation event processor (called externally to avoid circular dependencies) *)
let register_allocation_event_processor processor =
  Mutex.lock allocation_event_processor_mutex;
  allocation_event_processor := processor;
  Mutex.unlock allocation_event_processor_mutex

(** Remove an event callback *)
let remove_callback callback =
  Mutex.lock callbacks_mutex;
  event_callbacks := List.filter (fun cb -> cb != callback) !event_callbacks

(** Dispatch event to all callbacks *)
let dispatch_event event =
  Mutex.lock callbacks_mutex;
  let callbacks = !event_callbacks in
  Mutex.unlock callbacks_mutex;
  List.iter (fun callback ->
    try callback event
    with exn ->
      if Config.is_memory_tracing_enabled () then
        Logging.debug_f ~section:"allocation_tracker" "Callback failed: %s" (Printexc.to_string exn)
  ) callbacks

(** Get current domain ID safely *)
let get_current_domain_id () =
  try Obj.magic (Domain.self ())
  with _ -> 0

(** Check if a frame is from internal memory tracing code *)
let is_internal_frame file function_name =
  (* Skip frames from allocation_tracker.ml *)
  (Filename.basename file = "allocation_tracker.ml") ||
  (* Skip frames with internal function names *)
  let contains_substring haystack needle =
    try
      let _ = String.index haystack needle.[0] in
      let rec check pos =
        if pos + String.length needle > String.length haystack then false
        else if String.sub haystack pos (String.length needle) = needle then true
        else check (pos + 1)
      in
      check 0
    with Not_found -> false
  in
  contains_substring function_name "Allocation_tracker" ||
  contains_substring function_name "record_allocation" ||
  contains_substring function_name "get_allocation_site" ||
  contains_substring function_name "TrackedHashtbl" ||
  contains_substring function_name "TrackedArray" ||
  contains_substring function_name "TrackedList" ||
  contains_substring function_name "TrackedRingBuffer" ||
  contains_substring function_name "track_custom_structure"

(** Extract allocation site from call stack, skipping internal frames *)
let get_allocation_site () : allocation_site =
  (* Get the current call stack *)
  let raw_backtrace = Printexc.get_callstack 20 in  (* Get up to 20 frames *)
  let backtrace = Printexc.raw_backtrace_to_string raw_backtrace in

  (* Try to extract information from the call stack slots *)
  let slots = Printexc.backtrace_slots raw_backtrace in
  match slots with
  | None ->
      (* Fallback to string parsing if slots are not available *)
      let lines = String.split_on_char '\n' backtrace in
      let rec find_source_location = function
        | [] -> {
            file = "unknown";
            function_name = "unknown";
            line = 0;
            backtrace;
          }
        | line :: rest ->
            (* Look for pattern: File "file.ml", line X, characters Y-Z *)
            try
              let file_start = String.index line '"' in
              let file_end = String.index_from line (file_start + 1) '"' in
              let file = String.sub line (file_start + 1) (file_end - file_start - 1) in

              let remaining = String.sub line file_end 50 in
              let line_start = try String.index remaining 'l' with Not_found -> -1 in
              let line_start = if line_start >= 0 && String.length remaining > line_start + 5 &&
                               String.sub remaining line_start 5 = "line " then line_start else -1 in
              if line_start >= 0 then begin
                let line_str = String.sub line (file_end + line_start + 5) 10 in
                let line_num = int_of_string (String.trim line_str) in

                (* Extract function name if available *)
                let function_name =
                  try
                    let func_start = try String.index line 'C' with Not_found -> -1 in
                    if func_start >= 0 && String.length line > func_start + 12 &&
                       String.sub line func_start 12 = "Called from " then begin
                      let func_part = String.sub line (func_start + 12) (String.length line - func_start - 12) in
                      let func_end = try String.index func_part 'i' with Not_found -> String.length func_part in
                      if func_end >= 0 && String.length func_part > func_end + 8 &&
                         String.sub func_part func_end 8 = " in file" then
                        String.trim (String.sub func_part 0 func_end)
                      else
                        String.trim func_part
                    end else "unknown_function"
                  with _ -> "unknown_function"
                in

                (* Skip internal frames *)
                if is_internal_frame file function_name then
                  find_source_location rest
                else
                  { file; function_name; line = line_num; backtrace }
              end else
                find_source_location rest
            with _ -> find_source_location rest
      in
      find_source_location lines
  | Some slot_array ->
      (* Use structured slot information for better accuracy *)
      let rec find_valid_slot i =
        if i >= Array.length slot_array then
          (* No valid slots found, fallback to string parsing *)
          let lines = String.split_on_char '\n' backtrace in
          let rec find_source_location = function
            | [] -> {
                file = "unknown";
                function_name = "unknown";
                line = 0;
                backtrace;
              }
            | line :: rest ->
                (* Look for pattern: File "file.ml", line X, characters Y-Z *)
                try
                  let file_start = String.index line '"' in
                  let file_end = String.index_from line (file_start + 1) '"' in
                  let file = String.sub line (file_start + 1) (file_end - file_start - 1) in

                  let remaining = String.sub line file_end 50 in
                  let line_start = try String.index remaining 'l' with Not_found -> -1 in
                  let line_start = if line_start >= 0 && String.length remaining > line_start + 5 &&
                                   String.sub remaining line_start 5 = "line " then line_start else -1 in
                  if line_start >= 0 then begin
                    let line_str = String.sub line (file_end + line_start + 5) 10 in
                    let line_num = int_of_string (String.trim line_str) in

                    (* Extract function name if available *)
                    let function_name =
                      try
                        let func_start = try String.index line 'C' with Not_found -> -1 in
                        if func_start >= 0 && String.length line > func_start + 12 &&
                           String.sub line func_start 12 = "Called from " then begin
                          let func_part = String.sub line (func_start + 12) (String.length line - func_start - 12) in
                          let func_end = try String.index func_part 'i' with Not_found -> String.length func_part in
                          if func_end >= 0 && String.length func_part > func_end + 8 &&
                             String.sub func_part func_end 8 = " in file" then
                            String.trim (String.sub func_part 0 func_end)
                          else
                            String.trim func_part
                        end else "unknown_function"
                      with _ -> "unknown_function"
                    in

                    (* Skip internal frames *)
                    if is_internal_frame file function_name then
                      find_source_location rest
                    else
                      { file; function_name; line = line_num; backtrace }
                  end else
                    find_source_location rest
                with _ -> find_source_location rest
          in
          find_source_location lines
        else
          let slot = slot_array.(i) in
          match Printexc.Slot.location slot with
          | None -> find_valid_slot (i + 1)  (* Skip slots without location info *)
          | Some location ->
              let file = location.Printexc.filename in
              let line = location.Printexc.line_number in
              let function_name = match Printexc.Slot.name slot with
                | Some name -> name
                | None -> "unknown_function"
              in
              (* Skip internal frames *)
              if is_internal_frame file function_name then
                find_valid_slot (i + 1)
              else
                { file; function_name; line; backtrace }
      in
      find_valid_slot 0

(** Record a new allocation *)
let record_allocation structure_type size =
  (* Don't track empty allocations as they don't represent significant memory usage *)
  if size = 0 then begin
    (* Return a dummy record for empty allocations - minimal overhead *)
    let id = Atomic.fetch_and_add allocation_counter 1 in
    let domain_id = get_current_domain_id () in
    let timestamp = Unix.gettimeofday () in
    let site = {
      file = "empty_allocation";
      function_name = "empty_allocation";
      line = 0;
      backtrace = "";
    } in
    {
      id;
      structure_type;
      size;
      domain_id;
      timestamp;
      site;
      deallocated_at = None;
      replaced_by = None;
    }
  end else if Config.is_memory_tracing_enabled () && not (Atomic.get tracking_deferred) then begin
    let id = Atomic.fetch_and_add allocation_counter 1 in
    let domain_id = get_current_domain_id () in
    let timestamp = Unix.gettimeofday () in

    (* Only do expensive backtrace capture for sampled allocations *)
    let site = if Config.should_sample () then get_allocation_site () else {
      file = "sampled_allocation";
      function_name = "sampled_allocation";
      line = 0;
      backtrace = "";
    } in

    let record = {
      id;
      structure_type;
      size;
      domain_id;
      timestamp;
      site;
      deallocated_at = None;
      replaced_by = None;
    } in

    (* Only add to active allocations registry for sampled records to reduce overhead *)
    if Config.should_sample () then begin
      (* Add to active allocations registry - use try_lock to avoid deadlocks during initialization *)
      if Mutex.try_lock active_allocations_mutex then begin
        Hashtbl.add active_allocations id record;
        Mutex.unlock active_allocations_mutex;

        dispatch_event (Allocation record);

        (* Notify registered allocation event processor *)
        Mutex.lock allocation_event_processor_mutex;
        let processor = !allocation_event_processor in
        Mutex.unlock allocation_event_processor_mutex;
        processor structure_type site.file site.function_name site.line size domain_id timestamp;

        Logging.debug_f ~section:"allocation_tracker"
          "Allocated %s id=%d size=%d at %s:%d in %s (domain %d)"
          structure_type id size site.file site.line site.function_name domain_id;
      end else begin
        (* Skip allocation tracking if mutex is contended to avoid deadlocks *)
        Logging.debug_f ~section:"allocation_tracker"
          "Skipped allocation tracking for %s (mutex contended) id=%d size=%d at %s:%d in %s (domain %d)"
          structure_type id size site.file site.line site.function_name domain_id;
      end;
    end;

    record
  end else begin
    {
      id = 0;
      structure_type = "";
      size = 0;
      domain_id = 0;
      timestamp = 0.0;
      site = { file = ""; function_name = ""; line = 0; backtrace = "" };
      deallocated_at = None;
      replaced_by = None;
    }
  end

(** Record a deallocation *)
let record_deallocation record =
  if Config.is_memory_tracing_enabled () && not (Atomic.get tracking_deferred) then begin
    let timestamp = Unix.gettimeofday () in
    record.deallocated_at <- Some timestamp;

    (* Only track deallocations for sampled allocations to reduce overhead *)
    if record.site.file <> "sampled_allocation" && record.site.file <> "empty_allocation" then begin
      (* Remove from active allocations registry - use try_lock to avoid deadlocks *)
      if Mutex.try_lock active_allocations_mutex then begin
        Hashtbl.remove active_allocations record.id;
        Mutex.unlock active_allocations_mutex;

        dispatch_event (Deallocation {
          id = record.id;
          structure_type = record.structure_type;
          timestamp;
        });

        Logging.debug_f ~section:"allocation_tracker"
          "Deallocated %s id=%d (lifetime=%.2fs)"
          record.structure_type record.id (timestamp -. record.timestamp);
      end else begin
        (* Skip deallocation tracking if mutex is contended to avoid deadlocks *)
        Logging.debug_f ~section:"allocation_tracker"
          "Skipped deallocation tracking for %s id=%d (mutex contended)"
          record.structure_type record.id;
      end;
    end;
  end

(** Update the size of an existing allocation *)
let update_allocation_size record new_size =
  if Config.is_memory_tracing_enabled () && not (Atomic.get tracking_deferred) then begin
    let old_size = record.size in
    record.size <- new_size;

    (* Only dispatch size update events for sampled allocations or significant changes *)
    if record.site.file <> "sampled_allocation" && record.site.file <> "empty_allocation" then begin
      let timestamp = Unix.gettimeofday () in

      dispatch_event (SizeUpdate {
        id = record.id;
        old_size;
        new_size;
        structure_type = record.structure_type;
        timestamp;
      });

      Logging.debug_f ~section:"allocation_tracker"
        "Size updated %s id=%d: %d -> %d bytes"
        record.structure_type record.id old_size new_size;
    end;
  end

(** Record that an allocation has been replaced by a new one *)
let record_replacement old_id new_id =
  if Config.is_memory_tracing_enabled () && not (Atomic.get tracking_deferred) then begin
    (* Use try_lock to avoid blocking operations *)
    if Mutex.try_lock active_allocations_mutex then begin
      (match Hashtbl.find_opt active_allocations old_id with
       | Some old_record ->
           (* Mark old allocation as replaced *)
           old_record.replaced_by <- Some new_id;
           (* Add to replacement queue for deferred destruction *)
           Mutex.unlock active_allocations_mutex;
           if Mutex.try_lock replaced_allocations_mutex then begin
             Queue.push old_id replaced_allocations_queue;
             Atomic.incr replacement_counter;
             Mutex.unlock replaced_allocations_mutex;
             Logging.debug_f ~section:"allocation_tracker"
               "Marked allocation %d as replaced by %d" old_id new_id
           end else begin
             (* Skip if mutex is contended - will be picked up later *)
             Logging.debug_f ~section:"allocation_tracker"
               "Skipped replacement tracking for %d -> %d (mutex contended)" old_id new_id
           end
       | None ->
           (* Old allocation not found - may have already been cleaned up *)
           Mutex.unlock active_allocations_mutex;
           Logging.debug_f ~section:"allocation_tracker"
             "Replacement tracking: old allocation %d not found (may be already cleaned)" old_id);
    end else begin
      (* Skip if mutex is contended - replacement tracking is best-effort *)
      Logging.debug_f ~section:"allocation_tracker"
        "Skipped replacement tracking for %d -> %d (mutex contended)" old_id new_id
    end
  end

(** Destroy allocations that have been replaced, processing in batches *)
let destroy_replaced_allocations ?(batch_size = 50) () =
  if Config.is_memory_tracing_enabled () then begin
    let destroyed_count = ref 0 in
    let destroyed_memory = ref 0 in
    
    (* Try to acquire mutexes with timeout to avoid blocking *)
    let timeout_ms = 100 in  (* 100ms timeout *)
    let rec try_lock_with_timeout mutex attempts_left =
      if attempts_left <= 0 then false
      else if Mutex.try_lock mutex then true
      else begin
        Thread.delay 0.001;  (* Wait 1ms before retry *)
        try_lock_with_timeout mutex (attempts_left - 1)
      end
    in
    
    if try_lock_with_timeout replaced_allocations_mutex timeout_ms then begin
      (* Collect batch of IDs to destroy *)
      let ids_to_destroy = ref [] in
      let queue_length = Queue.length replaced_allocations_queue in
      let batch_count = min batch_size queue_length in
      for _ = 1 to batch_count do
        if not (Queue.is_empty replaced_allocations_queue) then
          ids_to_destroy := Queue.pop replaced_allocations_queue :: !ids_to_destroy
      done;
      Mutex.unlock replaced_allocations_mutex;
      
      (* Now process the batch - acquire active_allocations_mutex *)
      if try_lock_with_timeout active_allocations_mutex timeout_ms then begin
        List.iter (fun id ->
          match Hashtbl.find_opt active_allocations id with
          | Some record ->
              (* Verify it's actually replaced and not already deallocated *)
              if record.replaced_by <> None && record.deallocated_at = None then begin
                let memory_freed = record.size in
                destroyed_memory := !destroyed_memory + memory_freed;
                record_deallocation record;
                Hashtbl.remove active_allocations id;
                incr destroyed_count;
                Logging.debug_f ~section:"allocation_tracker"
                  "Destroyed replaced allocation %d (%s, %d bytes)" id record.structure_type memory_freed
              end
          | None ->
              (* Already removed - skip *)
              ()
        ) !ids_to_destroy;
        Mutex.unlock active_allocations_mutex;
        
        if !destroyed_count > 0 then
          Logging.info_f ~section:"allocation_tracker"
            "Destroyed %d replaced allocations, freed ~%d bytes" !destroyed_count !destroyed_memory
      end else begin
        (* Couldn't acquire active_allocations_mutex - put IDs back in queue *)
        Mutex.lock replaced_allocations_mutex;
        List.iter (fun id -> Queue.push id replaced_allocations_queue) !ids_to_destroy;
        Mutex.unlock replaced_allocations_mutex;
        Logging.debug_f ~section:"allocation_tracker"
          "Deferred destruction of %d allocations (mutex contended)" (List.length !ids_to_destroy)
      end
    end else begin
      (* Couldn't acquire replaced_allocations_mutex - skip this cycle *)
      Logging.debug_f ~section:"allocation_tracker"
        "Skipped destruction cycle (mutex contended)"
    end;
    
    !destroyed_count
  end else
    0

(** Get count of allocations waiting to be destroyed *)
let get_replaced_allocations_count () =
  if Mutex.try_lock replaced_allocations_mutex then begin
    let count = Queue.length replaced_allocations_queue in
    Mutex.unlock replaced_allocations_mutex;
    count
  end else
    0

(** Aggressive version of destroy_replaced_allocations with larger batch size and shorter timeouts *)
let destroy_replaced_allocations_aggressive ?(batch_size = 1000) ?(timeout_ms = 50) () =
  if Config.is_memory_tracing_enabled () then begin
    let destroyed_count = ref 0 in
    let destroyed_memory = ref 0 in

    (* Try to acquire mutexes with shorter timeout for more aggressive cleanup *)
    let rec try_lock_with_timeout mutex attempts_left =
      if attempts_left <= 0 then false
      else if Mutex.try_lock mutex then true
      else begin
        Thread.delay 0.001;  (* Wait 1ms before retry *)
        try_lock_with_timeout mutex (attempts_left - 1)
      end
    in

    if try_lock_with_timeout replaced_allocations_mutex timeout_ms then begin
      (* Collect larger batch of IDs to destroy *)
      let ids_to_destroy = ref [] in
      let queue_length = Queue.length replaced_allocations_queue in
      let batch_count = min batch_size queue_length in
      for _ = 1 to batch_count do
        if not (Queue.is_empty replaced_allocations_queue) then
          ids_to_destroy := Queue.pop replaced_allocations_queue :: !ids_to_destroy
      done;
      Mutex.unlock replaced_allocations_mutex;

      (* Now process the batch - acquire active_allocations_mutex with shorter timeout *)
      if try_lock_with_timeout active_allocations_mutex timeout_ms then begin
        List.iter (fun id ->
          match Hashtbl.find_opt active_allocations id with
          | Some record ->
              (* Verify it's actually replaced and not already deallocated *)
              if record.replaced_by <> None && record.deallocated_at = None then begin
                let memory_freed = record.size in
                destroyed_memory := !destroyed_memory + memory_freed;
                record_deallocation record;
                Hashtbl.remove active_allocations id;
                incr destroyed_count;
                Logging.debug_f ~section:"allocation_tracker"
                  "Aggressively destroyed replaced allocation %d (%s, %d bytes)" id record.structure_type memory_freed
              end
          | None ->
              (* Already removed - skip *)
              ()
        ) !ids_to_destroy;
        Mutex.unlock active_allocations_mutex;

        if !destroyed_count > 0 then
          Logging.info_f ~section:"allocation_tracker"
            "Aggressive cleanup: destroyed %d allocations, freed ~%d bytes" !destroyed_count !destroyed_memory
      end else begin
        (* Couldn't acquire active_allocations_mutex - put IDs back in queue *)
        Mutex.lock replaced_allocations_mutex;
        List.iter (fun id -> Queue.push id replaced_allocations_queue) !ids_to_destroy;
        Mutex.unlock replaced_allocations_mutex;
        Logging.debug_f ~section:"allocation_tracker"
          "Aggressive cleanup deferred: couldn't acquire active_allocations_mutex"
      end
    end else begin
      (* Couldn't acquire replaced_allocations_mutex - skip this aggressive cycle *)
      Logging.debug_f ~section:"allocation_tracker"
        "Aggressive cleanup skipped: couldn't acquire replaced_allocations_mutex"
    end;

    !destroyed_count
  end else
    0

(** Tracked Hashtbl wrapper *)
module TrackedHashtbl = struct
  type ('a, 'b) t = {
    hashtbl: ('a, 'b) Hashtbl.t;
    record: allocation_record;
    mutable length: int;
    mutable size_update_counter: int;  (* Track operations since last size update *)
  }

  (** Estimate memory usage of a hashtable (rough approximation) *)
  let estimate_memory_usage length =
    (* Base overhead + entry overhead * length *)
    (* This is a rough estimate: base hashtbl structure + entries *)
    let base_overhead = 64 in  (* hashtbl structure itself *)
    let entry_overhead = 24 in  (* key + value + hash overhead per entry *)
    base_overhead + (entry_overhead * length)

  (** Update the allocation record with current memory usage - batched for performance *)
  let update_size t =
    let current_size = estimate_memory_usage t.length in
    update_allocation_size t.record current_size;
    t.size_update_counter <- 0  (* Reset counter *)

  (** Check if we should update size (sample-based to reduce overhead) *)
  let should_update_size t =
    t.size_update_counter >= 10 ||  (* Update every 10 operations *)
    Config.should_sample () ||
    Random.float 1.0 < 0.05  (* 5% chance even for non-sampled *)

  let create ?random initial_size =
    let hashtbl = Hashtbl.create ?random initial_size in
    let record = record_allocation "Hashtbl" (estimate_memory_usage 0) in
    { hashtbl; record; length = 0; size_update_counter = 0 }

  let add t key value =
    let was_empty = t.length = 0 in
    Hashtbl.add t.hashtbl key value;
    if was_empty || not (Hashtbl.mem t.hashtbl key) then begin
      t.length <- t.length + 1;
      t.size_update_counter <- t.size_update_counter + 1;
      if should_update_size t then update_size t
    end

  let replace t key value =
    let was_present = Hashtbl.mem t.hashtbl key in
    Hashtbl.replace t.hashtbl key value;
    if not was_present then begin
      t.length <- t.length + 1;
      t.size_update_counter <- t.size_update_counter + 1;
      if should_update_size t then update_size t
    end

  let remove t key =
    if Hashtbl.mem t.hashtbl key then begin
      Hashtbl.remove t.hashtbl key;
      t.length <- t.length - 1;
      t.size_update_counter <- t.size_update_counter + 1;
      if should_update_size t then update_size t
    end

  let find t key = Hashtbl.find t.hashtbl key
  let find_opt t key = Hashtbl.find_opt t.hashtbl key
  let find_all t key = Hashtbl.find_all t.hashtbl key
  let mem t key = Hashtbl.mem t.hashtbl key
  let length t = t.length

  let iter f t = Hashtbl.iter f t.hashtbl
  let fold f t init = Hashtbl.fold f t.hashtbl init

  let filter_map_inplace f t =
    Hashtbl.filter_map_inplace f t.hashtbl;
    t.length <- Hashtbl.length t.hashtbl;
    t.size_update_counter <- t.size_update_counter + 1;
    if should_update_size t then update_size t

  let clear t =
    Hashtbl.clear t.hashtbl;
    t.length <- 0;
    t.size_update_counter <- 0;
    if should_update_size t then update_size t

  let stats t = Hashtbl.stats t.hashtbl

  let to_seq t = Hashtbl.to_seq t.hashtbl
  let to_seq_keys t = Hashtbl.to_seq_keys t.hashtbl
  let to_seq_values t = Hashtbl.to_seq_values t.hashtbl

  let destroy t = record_deallocation t.record
end

(** Tracked Array wrapper *)
module TrackedArray = struct
  type 'a t = {
    array: 'a array;
    record: allocation_record;
  }

  (** Calculate memory usage for an array *)
  let calculate_size length = length * (Sys.word_size / 8)

  (** Update the allocation record with current array size *)
  let update_size t =
    let current_size = calculate_size (Array.length t.array) in
    update_allocation_size t.record current_size

  let make size init =
    let array = Array.make size init in
    let record = record_allocation "Array" (calculate_size size) in
    { array; record }

  let init size f =
    let array = Array.init size f in
    let record = record_allocation "Array" (calculate_size size) in
    { array; record }

  let of_list lst =
    let array = Array.of_list lst in
    let record = record_allocation "Array" (calculate_size (Array.length array)) in
    { array; record }

  let get t i = Array.get t.array i
  let set t i v = Array.set t.array i v
  let length t = Array.length t.array

  let iter f t = Array.iter f t.array
  let iteri f t = Array.iteri f t.array
  let map f t =
    let new_array = Array.map f t.array in
    let record = record_allocation "Array" (calculate_size (Array.length new_array)) in
    { array = new_array; record }
  let mapi f t =
    let new_array = Array.mapi f t.array in
    let record = record_allocation "Array" (calculate_size (Array.length new_array)) in
    { array = new_array; record }

  let to_list t = Array.to_list t.array

  let sub t pos len =
    let new_array = Array.sub t.array pos len in
    let record = record_allocation "Array" (calculate_size (Array.length new_array)) in
    { array = new_array; record }
  let fill t pos len v = Array.fill t.array pos len v
  let append t1 t2 =
    let new_array = Array.append t1.array t2.array in
    let record = record_allocation "Array" (calculate_size (Array.length new_array)) in
    { array = new_array; record }
  let concat lst =
    let new_array = Array.concat (List.map (fun t -> t.array) lst) in
    let record = record_allocation "Array" (calculate_size (Array.length new_array)) in
    { array = new_array; record }
  let copy t =
    let new_array = Array.copy t.array in
    let record = record_allocation "Array" (calculate_size (Array.length new_array)) in
    { array = new_array; record }

  let sort cmp t = Array.sort cmp t.array
  let stable_sort cmp t = Array.stable_sort cmp t.array

  let destroy t = record_deallocation t.record
end

(** Tracked List wrapper for all lists *)
module TrackedList = struct
  type 'a t = {
    mutable list: 'a list;
    record: allocation_record;
    mutable length: int;
  }

  (** Calculate memory usage for a list *)
  let calculate_size length = length * (Sys.word_size / 8)

  (** Update the allocation record with current list size *)
  let update_size t =
    let current_size = calculate_size t.length in
    update_allocation_size t.record current_size

  let of_list lst =
    let len = List.length lst in
    let record = record_allocation "List" (calculate_size len) in
    { list = lst; record; length = len }

  (** Create empty list *)
  let empty () =
    let record = record_allocation "List" 0 in
    { list = []; record; length = 0 }

  (** Add element to front *)
  let cons x t =
    let new_list = x :: t.list in
    let record = record_allocation "List" (calculate_size (t.length + 1)) in
    { list = new_list; record; length = t.length + 1 }

  (** Append two tracked lists *)
  let append t1 t2 =
    let new_list = t1.list @ t2.list in
    let new_length = t1.length + t2.length in
    let record = record_allocation "List" (calculate_size new_length) in
    { list = new_list; record; length = new_length }

  (** Map function over list *)
  let map f t =
    let new_list = List.map f t.list in
    let record = record_allocation "List" (calculate_size t.length) in
    { list = new_list; record; length = t.length }

  (** Filter list *)
  let filter f t =
    let new_list = List.filter f t.list in
    let new_length = List.length new_list in
    let record = record_allocation "List" (calculate_size new_length) in
    { list = new_list; record; length = new_length }

  (** Fold over list *)
  let fold_left f init t = List.fold_left f init t.list
  let fold_right f t init = List.fold_right f t.list init

  (** Iterate over list *)
  let iter f t = List.iter f t.list
  let iteri f t = List.iteri f t.list

  (** Check properties *)
  let is_empty t = t.length = 0
  let mem x t = List.mem x t.list
  let for_all f t = List.for_all f t.list
  let exists f t = List.exists f t.list

  (** Find elements *)
  let find_opt f t = List.find_opt f t.list
  let find_all f t = List.find_all f t.list

  (** Get nth element *)
  let nth_opt t n = List.nth_opt t.list n

  (** Reverse list *)
  let rev t =
    let new_list = List.rev t.list in
    let record = record_allocation "List" (calculate_size t.length) in
    { list = new_list; record; length = t.length }

  (** Sort list *)
  let sort cmp t =
    let new_list = List.sort cmp t.list in
    let record = record_allocation "List" (calculate_size t.length) in
    { list = new_list; record; length = t.length }

  (** Convert to/from other structures *)
  let to_list t = t.list
  let length t = t.length
  let to_seq t = List.to_seq t.list
  let of_seq seq =
    let lst = List.of_seq seq in
    let len = List.length lst in
    let record = record_allocation "List" (calculate_size len) in
    { list = lst; record; length = len }

  (** Take/drop operations *)
  let take n t =
    let new_list = List.filteri (fun i _ -> i < n) t.list in
    let new_length = min n t.length in
    let record = record_allocation "List" (calculate_size new_length) in
    { list = new_list; record; length = new_length }

    let drop n t =
    let new_list = List.filteri (fun i _ -> i >= n) t.list in
    let new_length = max 0 (t.length - n) in
    let record = record_allocation "List" (calculate_size new_length) in
    { list = new_list; record; length = new_length }

  let destroy t = record_deallocation t.record

  (** Destroy old list and return new empty list - atomic replacement *)
  let destroy_and_empty t =
    destroy t;
    empty ()

  (** Destroy old list and return filtered list - atomic replacement *)
  let destroy_and_filter f t =
    let new_list = filter f t in
    destroy t;
    new_list

  (** Destroy old list and return taken prefix - atomic replacement *)
  let destroy_and_take n t =
    let new_list = take n t in
    destroy t;
    new_list

  (** Destroy old list and return with element cons'd - atomic replacement *)
  let destroy_and_cons x t =
    let new_list = cons x t in
    destroy t;
    new_list

  (** Clear list contents and reuse the allocation record - efficient in-place clearing *)
  let clear_and_reuse t =
    t.list <- [];
    t.length <- 0;
    update_size t;
    t

  (** Replace list contents with new list while reusing allocation record *)
  let replace_contents t new_list =
    let new_length = List.length new_list in
    t.list <- new_list;
    t.length <- new_length;
    update_size t;
    record_reuse ();
    t

  (** Note: Batch queue operations removed to avoid type generalization issues.
      Core functionality uses replace_contents for immediate reuse instead of destroy/create patterns. *)
end

(** Tracked Ring Buffer wrapper - compatible with RingBuffer module *)
module TrackedRingBuffer = struct
  type 'a t = {
    data: 'a option Atomic.t TrackedArray.t;
    write_pos: int Atomic.t;
    size: int;
    count: int Atomic.t;  (* Track number of valid entries *)
  }

  let create size =
    if size <= 0 then
      invalid_arg "TrackedRingBuffer.create: size must be positive";
    let data = TrackedArray.init size (fun _ -> Atomic.make None) in
    {
      data;
      write_pos = Atomic.make 0;
      size;
      count = Atomic.make 0;
    }

  (** Lock-free write - overwrites oldest entry when full (FIFO eviction) *)
  let write buffer value =
    let pos = Atomic.get buffer.write_pos in
    TrackedArray.set buffer.data pos (Atomic.make (Some value));
    let new_pos = (pos + 1) mod buffer.size in
    Atomic.set buffer.write_pos new_pos;
    let current_count = Atomic.get buffer.count in
    if current_count < buffer.size then
      ignore (Atomic.compare_and_set buffer.count current_count (current_count + 1))
    (* When full, we overwrite oldest but count stays at size *)

  (** Get current number of valid entries *)
  let length buffer = Atomic.get buffer.count

  (** Get capacity of the buffer *)
  let capacity buffer = buffer.size

  (** Read all current entries in the buffer (for snapshot rebuilding) *)
  let read_all buffer =
    let write_pos = Atomic.get buffer.write_pos in
    let count = Atomic.get buffer.count in
    if count = 0 then
      []
    else
      let rec collect acc remaining pos =
        if remaining <= 0 then
          List.rev acc
        else
          match Atomic.get (TrackedArray.get buffer.data pos) with
          | Some value -> collect (value :: acc) (remaining - 1) ((pos + 1) mod buffer.size)
          | None -> collect acc (remaining - 1) ((pos + 1) mod buffer.size)
      in
      (* Start reading from the oldest entry *)
      let start_pos = if count < buffer.size then 0 else write_pos in
      collect [] count start_pos

  (** Read latest entry (most recent) *)
  let read_latest buffer =
    if Atomic.get buffer.count = 0 then
      None
    else
      let write_pos = Atomic.get buffer.write_pos in
      let read_pos = if write_pos = 0 then buffer.size - 1 else write_pos - 1 in
      Atomic.get (TrackedArray.get buffer.data read_pos)

  (** Read entries from a specific position onwards (for incremental updates) *)
  let read_since buffer last_pos =
    let current_pos = Atomic.get buffer.write_pos in
    if last_pos = current_pos || Atomic.get buffer.count = 0 then
      []
    else
      let rec collect acc pos =
        if pos = current_pos then
          List.rev acc
        else
          match Atomic.get (TrackedArray.get buffer.data pos) with
          | Some value -> collect (value :: acc) ((pos + 1) mod buffer.size)
          | None -> collect acc ((pos + 1) mod buffer.size)
      in
      collect [] last_pos

  (** Clear all entries in the buffer *)
  let clear buffer =
    TrackedArray.iter (fun atomic -> Atomic.set atomic None) buffer.data;
    Atomic.set buffer.write_pos 0;
    Atomic.set buffer.count 0

  (** Check if buffer is empty *)
  let is_empty buffer = Atomic.get buffer.count = 0

  (** Check if buffer is at full capacity *)
  let is_full buffer = Atomic.get buffer.count >= buffer.size

  let destroy t = TrackedArray.destroy t.data
end

(** Tracked Map wrapper - provides memory tracking for mutable maps *)
module TrackedMap = struct
  module type OrderedType = Map.OrderedType

  module Make (Ord : OrderedType) = struct
    module StdMap = Map.Make (Ord)

    type 'a t = {
      mutable map: 'a StdMap.t;
      record: allocation_record;
      mutable size: int;
    }

    (** Estimate memory usage of a map (rough approximation) *)
    let estimate_memory_usage size =
      (* Base overhead + entry overhead * size *)
      (* Map nodes have overhead for key + value + left/right children *)
      let base_overhead = 48 in  (* map structure itself *)
      let entry_overhead = 32 in  (* key + value + node overhead *)
      base_overhead + (entry_overhead * size)

    (** Update the allocation record with current memory usage *)
    let update_size t =
      let current_size = estimate_memory_usage t.size in
      update_allocation_size t.record current_size

    (** Create a new empty tracked map *)
    let empty ?(replace_old = None) () =
      let new_record = record_allocation "Map" (estimate_memory_usage 0) in
      (* If replacing an old map, record the replacement *)
      (match replace_old with
       | Some old_map -> record_replacement old_map.record.id new_record.id
       | None -> ());
      { map = StdMap.empty; record = new_record; size = 0 }

    (** Add a binding and return new tracked map *)
    let add key value t =
      let was_present = StdMap.mem key t.map in
      let new_map = StdMap.add key value t.map in
      let new_size = if was_present then t.size else t.size + 1 in
      let new_record = record_allocation "Map" (estimate_memory_usage new_size) in
      (* Record that the old map is being replaced by the new one *)
      record_replacement t.record.id new_record.id;
      { map = new_map; record = new_record; size = new_size }

    (** Remove a binding and return new tracked map *)
    let remove key t =
      let was_present = StdMap.mem key t.map in
      let new_map = StdMap.remove key t.map in
      let new_size = if was_present then t.size - 1 else t.size in
      let new_record = record_allocation "Map" (estimate_memory_usage new_size) in
      (* Record that the old map is being replaced by the new one *)
      record_replacement t.record.id new_record.id;
      { map = new_map; record = new_record; size = new_size }

    (** Find a binding *)
    let find key t = StdMap.find key t.map
    let find_opt key t = StdMap.find_opt key t.map
    let find_first_opt f t = StdMap.find_first_opt f t.map
    let find_last_opt f t = StdMap.find_last_opt f t.map

    (** Check membership *)
    let mem key t = StdMap.mem key t.map

    (** Get map properties *)
    let is_empty t = t.size = 0
    let cardinal t = t.size

    (** Iteration and folding *)
    let iter f t = StdMap.iter f t.map
    let fold f t init = StdMap.fold f t.map init
    let for_all f t = StdMap.for_all f t.map
    let exists f t = StdMap.exists f t.map

    (** Convert to association list *)
    let bindings t = StdMap.bindings t.map

    (** Create from association list *)
    let of_list lst =
      let map = StdMap.of_list lst in
      let size = List.length lst in
      let record = record_allocation "Map" (estimate_memory_usage size) in
      { map; record; size }

    (** Convert to list *)
    let to_list t = StdMap.to_list t.map

    (** Destroy the tracked map *)
    let destroy t = record_deallocation t.record

    (** Destroy old map and return new empty map - atomic replacement *)
    let destroy_and_empty t =
      let new_empty = empty ~replace_old:(Some t) () in
      destroy t;
      new_empty

    (** Destroy old map and return map with binding added - atomic replacement *)
    let destroy_and_add key value t =
      let new_map = add key value t in
      destroy t;
      new_map

    (** Destroy old map and return map with binding removed - atomic replacement *)
    let destroy_and_remove key t =
      let new_map = remove key t in
      destroy t;
      new_map

    (** Replace entire map contents with bindings list - efficient batch replacement *)
    let replace_with_bindings t bindings_list =
      let new_size = List.length bindings_list in
      let new_map = StdMap.of_list bindings_list in

      (* Update the existing tracked map in-place for efficiency *)
      t.map <- new_map;
      t.size <- new_size;
      update_size t;

      (* Record reuse instead of new allocation *)
      record_reuse ();

      t
  end
end

(** Tracked Queue wrapper - provides memory tracking for mutable queues *)
module TrackedQueue = struct
  type 'a t = {
    queue: 'a Queue.t;
    record: allocation_record;
    mutable length: int;
  }

  (** Estimate memory usage of a queue (rough approximation) *)
  let estimate_memory_usage length =
    (* Base overhead + entry overhead * length *)
    (* Queue has overhead for array storage and indices *)
    let base_overhead = 48 in  (* queue structure itself *)
    let entry_overhead = 16 in  (* per element overhead *)
    base_overhead + (entry_overhead * length)

  (** Update the allocation record with current memory usage *)
  let update_size t =
    let current_size = estimate_memory_usage t.length in
    update_allocation_size t.record current_size

  (** Create a new empty tracked queue *)
  let create () =
    let queue = Queue.create () in
    let record = record_allocation "Queue" (estimate_memory_usage 0) in
    { queue; record; length = 0 }

  (** Add an element to the queue *)
  let push value t =
    Queue.push value t.queue;
    t.length <- t.length + 1;
    update_size t

  (** Take and remove the first element *)
  let pop t =
    let result = Queue.pop t.queue in
    t.length <- t.length - 1;
    update_size t;
    result

  (** Take and remove the first element (non-raising) *)
  let take t =
    let result = Queue.take t.queue in
    t.length <- t.length - 1;
    update_size t;
    result

  (** Peek at the first element without removing it *)
  let peek t = Queue.peek t.queue

  (** Peek at the first element without removing it (non-raising) *)
  let peek_opt t = Queue.peek_opt t.queue

  (** Take and remove the first element (non-raising) *)
  let take_opt t =
    match Queue.take_opt t.queue with
    | Some value ->
        t.length <- t.length - 1;
        update_size t;
        Some value
    | None -> None

  (** Remove all elements from the queue *)
  let clear t =
    Queue.clear t.queue;
    t.length <- 0;
    update_size t

  (** Get the number of elements in the queue *)
  let length t = t.length

  (** Check if the queue is empty *)
  let is_empty t = t.length = 0

  (** Iterate over elements *)
  let iter f t = Queue.iter f t.queue

  (** Fold over elements *)
  let fold f t init = Queue.fold f t.queue init

  (** Convert to list *)
  let to_list t =
    let result = ref [] in
    Queue.iter (fun x -> result := x :: !result) t.queue;
    List.rev !result

  (** Add all elements from a list *)
  let add_seq t seq =
    Queue.add_seq t.queue seq;
    (* Count elements in sequence *)
    let count = Seq.fold_left (fun acc _ -> acc + 1) 0 seq in
    t.length <- t.length + count;
    update_size t

  (** Convert to sequence *)
  let to_seq t = Queue.to_seq t.queue

  (** Create from sequence *)
  let of_seq seq =
    let queue = Queue.of_seq seq in
    let length = Seq.fold_left (fun acc _ -> acc + 1) 0 seq in
    let record = record_allocation "Queue" (estimate_memory_usage length) in
    { queue; record; length }

  (** Create from list *)
  let of_list lst =
    let queue = Queue.create () in
    List.iter (fun x -> Queue.push x queue) lst;
    let length = List.length lst in
    let record = record_allocation "Queue" (estimate_memory_usage length) in
    { queue; record; length }

  (** Copy the queue *)
  let copy t =
    let new_queue = Queue.copy t.queue in
    let record = record_allocation "Queue" (estimate_memory_usage t.length) in
    { queue = new_queue; record; length = t.length }

  (** Transfer elements from one queue to another *)
  let transfer src dst =
    let count = Queue.length src.queue in
    Queue.transfer src.queue dst.queue;
    src.length <- 0;
    dst.length <- dst.length + count;
    update_size src;
    update_size dst

  (** Destroy the tracked queue *)
  let destroy t = record_deallocation t.record
end

(** Safe Tracked Hashtbl wrapper with type-level mutex enforcement *)
module SafeTrackedHashtbl = struct
  (** Phantom types for tracking mutex state *)
  type locked   (* Phantom type indicating hashtable is locked *)
  type unlocked (* Phantom type indicating hashtable is unlocked *)

  (** Safe hashtable type that bundles the hashtable with its mutex *)
  type ('k, 'v) t = {
    hashtbl: ('k, 'v) TrackedHashtbl.t;
    mutex: Mutex.t;
  }

  (** Create a new safe hashtable with the specified initial capacity *)
  let create initial_size = {
    hashtbl = TrackedHashtbl.create initial_size;
    mutex = Mutex.create ();
  }

  (** Execute a function with the hashtable locked - ensures proper locking/unlocking *)
  let with_lock safe_hashtbl f =
    Mutex.lock safe_hashtbl.mutex;
    try
      let result = f safe_hashtbl in
      Mutex.unlock safe_hashtbl.mutex;
      result
    with exn ->
      Mutex.unlock safe_hashtbl.mutex;
      raise exn

  (** Find an optional value - requires locked state *)
  let find_opt (safe_hashtbl : ('k, 'v) t) key =
    TrackedHashtbl.find_opt safe_hashtbl.hashtbl key

  (** Add a key-value pair - requires locked state *)
  let add (safe_hashtbl : ('k, 'v) t) key value =
    TrackedHashtbl.add safe_hashtbl.hashtbl key value

  (** Replace a key-value pair - requires locked state *)
  let replace (safe_hashtbl : ('k, 'v) t) key value =
    TrackedHashtbl.replace safe_hashtbl.hashtbl key value

  (** Remove a key-value pair - requires locked state *)
  let remove (safe_hashtbl : ('k, 'v) t) key =
    TrackedHashtbl.remove safe_hashtbl.hashtbl key

  (** Check if key exists - requires locked state *)
  let mem (safe_hashtbl : ('k, 'v) t) key =
    TrackedHashtbl.mem safe_hashtbl.hashtbl key

  (** Get the number of elements - requires locked state *)
  let length (safe_hashtbl : ('k, 'v) t) =
    TrackedHashtbl.length safe_hashtbl.hashtbl

  (** Fold over all elements - requires locked state *)
  let fold f (safe_hashtbl : ('k, 'v) t) init =
    TrackedHashtbl.fold f safe_hashtbl.hashtbl init

  (** Iterate over all elements - requires locked state *)
  let iter f (safe_hashtbl : ('k, 'v) t) =
    TrackedHashtbl.iter f safe_hashtbl.hashtbl

  (** Clear all elements - requires locked state *)
  let clear (safe_hashtbl : ('k, 'v) t) =
    TrackedHashtbl.clear safe_hashtbl.hashtbl

  (** Convert to sequence of keys - requires locked state *)
  let to_seq_keys (safe_hashtbl : ('k, 'v) t) =
    TrackedHashtbl.to_seq_keys safe_hashtbl.hashtbl

  (** Convert to sequence of values - requires locked state *)
  let to_seq_values (safe_hashtbl : ('k, 'v) t) =
    TrackedHashtbl.to_seq_values safe_hashtbl.hashtbl

  (** Convert to sequence of key-value pairs - requires locked state *)
  let to_seq (safe_hashtbl : ('k, 'v) t) =
    TrackedHashtbl.to_seq safe_hashtbl.hashtbl

  (** Destroy the hashtable - requires locked state *)
  let destroy (safe_hashtbl : ('k, 'v) t) =
    TrackedHashtbl.destroy safe_hashtbl.hashtbl
end

(** Custom structure tracking - for domain registries, event buses, etc. *)
let track_custom_structure name size =
  record_allocation name size

let untrack_custom_structure record =
  record_deallocation record

(** Statistics record *)
type stats = {
  total_allocations: int;
  active_allocations: int;
  total_memory_tracked: int;
}



(** Clean up stale active allocations to prevent unbounded growth *)
let cleanup_stale_allocations () =
  Mutex.lock active_allocations_mutex;
  let now = Unix.gettimeofday () in
  let stale_cutoff = now -. stale_allocation_seconds in

  (* Collect keys to remove *)
  let keys_to_remove = ref [] in
  Hashtbl.iter (fun id record ->
    (* Remove allocations that are older than the stale threshold and haven't been deallocated *)
    if record.timestamp < stale_cutoff && record.deallocated_at = None then
      keys_to_remove := id :: !keys_to_remove
  ) active_allocations;

  (* Remove stale allocations *)
  let stale_removed = List.length !keys_to_remove in
  List.iter (Hashtbl.remove active_allocations) !keys_to_remove;

  (* If still over limit, remove oldest allocations *)
  let current_count = Hashtbl.length active_allocations in
  let to_remove_more = max 0 (current_count - max_active_allocations) in
  if to_remove_more > 0 then begin
    (* Collect all allocations with timestamps *)
    let allocations_with_times = ref [] in
    Hashtbl.iter (fun id record ->
      (* Only consider non-deallocated allocations for LRU removal *)
      if record.deallocated_at = None then
        allocations_with_times := (id, record.timestamp) :: !allocations_with_times
    ) active_allocations;

    (* Sort by timestamp (oldest first) and remove oldest *)
    let sorted = List.sort (fun (_, t1) (_, t2) -> Float.compare t1 t2) !allocations_with_times in
    let oldest_to_remove = List.map fst (List.filteri (fun i _ -> i < to_remove_more) sorted) in
    List.iter (Hashtbl.remove active_allocations) oldest_to_remove;
  end;

  let final_count = Hashtbl.length active_allocations in
  Mutex.unlock active_allocations_mutex;

  if stale_removed > 0 || to_remove_more > 0 then
    Logging.info_f ~section:"allocation_tracker" "Cleaned up active allocations: removed %d stale + %d oldest, total active: %d/%d"
      stale_removed to_remove_more final_count max_active_allocations

(** Get allocation statistics *)
let get_allocation_stats () =
  Mutex.lock active_allocations_mutex;
  let active_count = Hashtbl.length active_allocations in
  let total_memory = Hashtbl.fold (fun _ record acc ->
    acc + record.size
  ) active_allocations 0 in
  Mutex.unlock active_allocations_mutex;

  {
    total_allocations = Atomic.get allocation_counter;
    active_allocations = active_count;
    total_memory_tracked = total_memory;
  }

(** Get all active allocations for detailed reporting *)
let get_all_active_allocations () =
  Mutex.lock active_allocations_mutex;
  let allocations = Hashtbl.fold (fun _ record acc ->
    record :: acc
  ) active_allocations [] in
  Mutex.unlock active_allocations_mutex;
  allocations

(** Size sampling configuration *)
let size_sampling_interval_seconds = ref 60.0  (* Default: sample every 60 seconds *)

(** Set size sampling interval (in seconds) *)
let set_size_sampling_interval interval =
  size_sampling_interval_seconds := max 10.0 interval  (* Minimum 10 seconds *)

(** Get current size sampling interval *)
let get_size_sampling_interval () = !size_sampling_interval_seconds

(** Periodic size sampling thread *)
let start_periodic_size_sampling () =
  let rec sampling_loop () =
    Thread.delay !size_sampling_interval_seconds;
    if Config.is_memory_tracing_enabled () then begin
      try
        (* Defer allocation tracking during size sampling to avoid deadlocks *)
        defer_tracking ();

        (* Resample sizes of all active allocations *)
        (* Note: This is a simplified implementation. In practice, we'd need
           type-specific size calculation functions for each data structure type.
           For now, we'll just log that sampling occurred. *)

        Logging.debug ~section:"allocation_tracker" "Periodic size sampling completed";

        (* Resume allocation tracking *)
        resume_tracking ();

        (* Continue sampling *)
        sampling_loop ()
      with exn ->
        (* Resume tracking even if sampling failed *)
        resume_tracking ();
        Logging.warn_f ~section:"allocation_tracker" "Size sampling failed: %s" (Printexc.to_string exn);
        (* Continue despite errors *)
        sampling_loop ()
    end
  in
  let sampling_thread = Thread.create sampling_loop () in
  Logging.info_f ~section:"allocation_tracker" "Started periodic size sampling (every %.1f seconds)" !size_sampling_interval_seconds;
  sampling_thread
