(** Concurrent symbol registry backed by a mutex-protected mutable Hashtbl.

    Writers mutate the table in-place under a lock, eliminating the
    full-table copy that the previous CAS snapshot design allocated on
    every write and remove.  Readers also hold the lock briefly to
    obtain a consistent view.

    For snapshot/iteration callers that need a stable copy the
    `snapshot` function still returns a `Tbl.copy`, but those paths
    are infrequent (dashboard, periodic cleanup) so occasional
    allocation there is fine.
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
    mutex: Mutex.t;
    data: snapshot;
    size: int Atomic.t;
    mutable last_cleanup: float;
  }

  let create () =
    {
      mutex = Mutex.create ();
      data = Tbl.create 16;
      size = Atomic.make 0;
      last_cleanup = Unix.gettimeofday ();
    }

  (** Return a copy of the current snapshot.
      Only call from infrequent paths (dashboard, periodic cleanup). *)
  let snapshot registry =
    Mutex.lock registry.mutex;
    let copy = Tbl.copy registry.data in
    Mutex.unlock registry.mutex;
    copy

  let size registry = Atomic.get registry.size

  (** Replace or insert a key-value pair. Returns Some (new_size, was_existing). *)
  let replace registry key value =
    Mutex.lock registry.mutex;
    let existed = Tbl.mem registry.data key in
    Tbl.replace registry.data key value;
    if not existed then ignore (Atomic.fetch_and_add registry.size 1);
    Mutex.unlock registry.mutex;
    Some (Atomic.get registry.size, existed)

  (** Find the value for a key, or None. *)
  let find registry key =
    Mutex.lock registry.mutex;
    let result = Tbl.find_opt registry.data key in
    Mutex.unlock registry.mutex;
    result

  (** Remove a key. Returns Some new_size if the key existed, else None. *)
  let remove registry key =
    Mutex.lock registry.mutex;
    let existed = Tbl.mem registry.data key in
    if existed then begin
      Tbl.remove registry.data key;
      ignore (Atomic.fetch_and_add registry.size (-1))
    end;
    Mutex.unlock registry.mutex;
    if existed then Some (Atomic.get registry.size) else None

  let fold registry ~init ~f =
    Mutex.lock registry.mutex;
    let result = Tbl.fold f registry.data init in
    Mutex.unlock registry.mutex;
    result

  let iter registry ~f =
    Mutex.lock registry.mutex;
    Tbl.iter f registry.data;
    Mutex.unlock registry.mutex

  let to_list registry =
    Mutex.lock registry.mutex;
    let result = Tbl.fold (fun key value acc -> (key, value) :: acc) registry.data [] in
    Mutex.unlock registry.mutex;
    result

  (** Recalculate size from actual hashtable contents to fix drift **)
  let recalculate_size registry =
    Mutex.lock registry.mutex;
    let actual_size = Tbl.length registry.data in
    let current_size = Atomic.get registry.size in
    if actual_size <> current_size then begin
      Atomic.set registry.size actual_size;
      Mutex.unlock registry.mutex;
      Some (actual_size - current_size)
    end else begin
      Mutex.unlock registry.mutex;
      None
    end

  (** Enforce maximum size limit by removing the oldest-inserted entries.
      Since Hashtbl iteration order is unspecified we collect excess keys first. *)
  let enforce_size_limit registry ?(max_entries=1000) () =
    Mutex.lock registry.mutex;
    let current_size = Tbl.length registry.data in
    if current_size <= max_entries then begin
      Mutex.unlock registry.mutex;
      None
    end else begin
      let to_remove = current_size - max_entries + 10 in
      let removed = ref 0 in
      let keys_to_remove = ref [] in
      Tbl.iter (fun key _ ->
        if !removed < to_remove then begin
          keys_to_remove := key :: !keys_to_remove;
          incr removed
        end
      ) registry.data;
      List.iter (fun key -> Tbl.remove registry.data key) !keys_to_remove;
      Atomic.set registry.size (current_size - !removed);
      registry.last_cleanup <- Unix.gettimeofday ();
      Mutex.unlock registry.mutex;
      Some !removed
    end

  (** Periodic cleanup to prevent unbounded growth **)
  let cleanup registry ?(max_age_seconds=300.0) ?(max_entries=1000) () =
    let now = Unix.gettimeofday () in
    let time_since_cleanup = now -. registry.last_cleanup in
    if time_since_cleanup > max_age_seconds || Atomic.get registry.size > max_entries then begin
      let size_drift = recalculate_size registry in
      let size_trimmed = enforce_size_limit registry ~max_entries () in
      Some (size_drift, size_trimmed)
    end else None
end
