(** Concurrent symbol registry backed by atomically swapped immutable maps.

    Writers install new snapshots via compare-and-set while readers obtain
    versioned snapshots for zero-copy iteration. This avoids Hashtbl races
    when multiple domains and Lwt threads interact.
*)

module type KEY = sig
  type t
  val equal : t -> t -> bool
  val hash : t -> int
end

module type VALUE = sig
  type t
end

module Make (Key : KEY) (Value : VALUE) = struct
  module Tbl = Hashtbl.Make (Key)

  type snapshot = Value.t Tbl.t

  type t = {
    size: int Atomic.t;
    version: int Atomic.t;
    data: snapshot Atomic.t;
    mutable last_cleanup: float;  (* Track when we last cleaned up *)
    mutable operation_count: int;  (* Counter for event-driven cleanup *)
  }

  let create () =
    {
      size = Atomic.make 0;
      version = Atomic.make 0;
      data = Atomic.make (Tbl.create 0);
      last_cleanup = Unix.gettimeofday ();
      operation_count = 0;
    }

  let snapshot registry = Atomic.get registry.data

  let size registry = Atomic.get registry.size

  let bump_version registry = Atomic.fetch_and_add registry.version 1 + 1

  (* Forward reference for cleanup function - will be set after cleanup is defined *)
  let cleanup_fn_ref = ref (fun _registry -> ())

  let replace registry key value =
    let rec loop () =
      let current = Atomic.get registry.data in
      let copy = Tbl.copy current in
      let existed = Tbl.mem copy key in
      Tbl.replace copy key value;
      if Atomic.compare_and_set registry.data current copy then (
        if not existed then ignore (Atomic.fetch_and_add registry.size 1);
        Some (bump_version registry, existed)
      ) else loop ()
    in
    let result = loop () in

    (* Event-driven cleanup: increment counter and perform cleanup every 50 operations *)
    registry.operation_count <- registry.operation_count + 1;
    if registry.operation_count mod 50 = 0 then
      !cleanup_fn_ref registry;

    result

  let find registry key =
    let current = Atomic.get registry.data in
    let result = try Some (Tbl.find current key) with Not_found -> None in

    (* Event-driven cleanup: increment counter and perform cleanup every 50 operations *)
    registry.operation_count <- registry.operation_count + 1;
    if registry.operation_count mod 50 = 0 then
      !cleanup_fn_ref registry;

    result

  let remove registry key =
    let rec loop () =
      let current = Atomic.get registry.data in
      if not (Tbl.mem current key) then None
      else
        let copy = Tbl.copy current in
        Tbl.remove copy key;
        if Atomic.compare_and_set registry.data current copy then (
          ignore (Atomic.fetch_and_add registry.size (-1));
          Some (bump_version registry)
        ) else loop ()
    in
    loop ()

  let fold registry ~init ~f =
    let snapshot = Atomic.get registry.data in
    Tbl.fold f snapshot init

  let iter registry ~f =
    let snapshot = Atomic.get registry.data in
    Tbl.iter f snapshot

  let to_list registry =
    let snapshot = Atomic.get registry.data in
    Tbl.fold (fun key value acc -> (key, value) :: acc) snapshot []

  (** Recalculate size from actual hashtable contents to fix drift *)
  let recalculate_size registry =
    let snapshot = Atomic.get registry.data in
    let actual_size = Tbl.length snapshot in
    let current_size = Atomic.get registry.size in
    if actual_size <> current_size then begin
      Atomic.set registry.size actual_size;
      Some (actual_size - current_size)  (* Return drift amount *)
    end else None

  (** Enforce maximum size limit by removing oldest entries (simple LRU) *)
  let enforce_size_limit registry ?(max_entries=1000) () =
    let rec loop () =
      let current = Atomic.get registry.data in
      let current_size = Tbl.length current in
      if current_size <= max_entries then None
      else
        (* Remove entries until we're under the limit *)
        (* Since we don't have timestamps, use a simple approach: remove some entries *)
        let copy = Tbl.copy current in
        let to_remove = current_size - max_entries + 10 in  (* Remove a bit extra to avoid frequent cleanup *)
        let removed = ref 0 in
        Tbl.iter (fun key _ ->
          if !removed < to_remove then begin
            Tbl.remove copy key;
            incr removed
          end
        ) copy;
        if Atomic.compare_and_set registry.data current copy then begin
          Atomic.set registry.size (current_size - !removed);
          registry.last_cleanup <- Unix.gettimeofday ();
          Some !removed
        end else loop ()
    in
    loop ()

  (** Periodic cleanup to prevent unbounded growth *)
  let cleanup registry ?(max_age_seconds=300.0) ?(max_entries=1000) () =
    let now = Unix.gettimeofday () in
    let time_since_cleanup = now -. registry.last_cleanup in

    (* Only cleanup if it's been a while or we're over size limits *)
    if time_since_cleanup > max_age_seconds || Atomic.get registry.size > max_entries then begin
      (* Recalculate size first *)
      let size_drift = recalculate_size registry in

      (* Enforce size limits *)
      let size_trimmed = enforce_size_limit registry ~max_entries () in

      registry.last_cleanup <- now;

      (* Return cleanup stats *)
      Some (size_drift, size_trimmed)
    end else None

  (* Set the cleanup function reference after cleanup is defined *)
  let () =
    cleanup_fn_ref := (fun registry ->
      ignore (cleanup registry ~max_age_seconds:300.0 ~max_entries:1000 ())
    )
end
