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
  }

  let create () =
    {
      size = Atomic.make 0;
      version = Atomic.make 0;
      data = Atomic.make (Tbl.create 0);
    }

  let snapshot registry = Atomic.get registry.data

  let size registry = Atomic.get registry.size

  let bump_version registry = Atomic.fetch_and_add registry.version 1 + 1

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
    loop ()

  let find registry key =
    let current = Atomic.get registry.data in
    try Some (Tbl.find current key) with Not_found -> None

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
end


