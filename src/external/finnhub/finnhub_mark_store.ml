type mark =
  { price : float
  ; size : float
  ; timestamp : float
  ; valid : bool
  }

type store =
  { symbol : string
  ; mark : mark Atomic.t
  }

let stores : (string, store) Hashtbl.t = Hashtbl.create 16
let stores_mutex = Mutex.create ()

let get_or_create_store symbol =
  Mutex.lock stores_mutex;
  let store =
    match Hashtbl.find_opt stores symbol with
    | Some s -> s
    | None ->
      let s =
        { symbol
        ; mark = Atomic.make { price = 0.0; size = 0.0; timestamp = 0.0; valid = false }
        }
      in
      Hashtbl.replace stores symbol s;
      s
  in
  Mutex.unlock stores_mutex;
  store
;;

let push symbol ~price ~size =
  if price > 0.0
  then (
    let store = get_or_create_store symbol in
    Atomic.set store.mark { price; size; timestamp = Unix.gettimeofday (); valid = true })
;;

let get_mark symbol =
  match Hashtbl.find_opt stores symbol with
  | None -> None
  | Some store ->
    let m = Atomic.get store.mark in
    if m.valid then Some (m.price, m.size, m.timestamp) else None
;;

let get_mark_age symbol =
  match get_mark symbol with
  | Some (_, _, ts) -> Some (Unix.gettimeofday () -. ts)
  | None -> None
;;
