(** Lighter L2 orderbook feed.
    Subscribes to [order_book/{MARKET_INDEX}] WebSocket channel.
    Receives full snapshot on subscription, then incremental deltas.
    Stores snapshots in per-symbol lock-free ring buffers. *)

let section = "lighter_orderbook"

let ring_buffer_size = 16

(** Maximum number of price levels retained per side.
    Bounds memory and matches downstream consumers (top-of-book only). *)
let max_depth = 50

type level = {
  price: float;
  size: float;
}

type orderbook = {
  symbol: string;
  bids: level array;
  asks: level array;
  timestamp: float;
}

(** Lock-free SPSC ring buffer for orderbook snapshots. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per-symbol store containing a ring buffer and an atomic readiness flag. *)
type store = {
  buffer: orderbook RingBuffer.t;
  ready: bool Atomic.t;
  (* Local orderbook state for incremental updates *)
  mutable local_bids: (float * float) list;  (** Sorted descending by price *)
  mutable local_asks: (float * float) list;  (** Sorted ascending by price *)
  ob_mutex: Mutex.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

(** Returns the store for [symbol], creating one if absent. Uses double-checked
    locking via [initialization_mutex] for thread safety. *)
let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      Mutex.lock initialization_mutex;
      let store = match Hashtbl.find_opt stores symbol with
        | Some store -> store
        | None ->
            let store = {
              buffer = RingBuffer.create ring_buffer_size;
              ready = Atomic.make false;
              local_bids = [];
              local_asks = [];
              ob_mutex = Mutex.create ();
            } in
            Hashtbl.add stores symbol store;
            store
      in
      Mutex.unlock initialization_mutex;
      store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

(** Apply an incremental update to a sorted price level list.
    size=0 means delete that price level, otherwise upsert. *)
let apply_delta levels price size ~is_bid =
  if size = 0.0 then
    List.filter (fun (p, _) -> p <> price) levels
  else
    let updated = ref false in
    let result = List.map (fun (p, s) ->
      if p = price then begin updated := true; (p, size) end
      else (p, s)
    ) levels in
    if !updated then result
    else
      let with_new = (price, size) :: result in
      if is_bid then
        List.sort (fun (a, _) (b, _) -> compare b a) with_new (* Descending *)
      else
        List.sort (fun (a, _) (b, _) -> compare a b) with_new (* Ascending *)

(** Truncate a level list to [max_depth] entries.
    Lists are pre-sorted (bids descending, asks ascending) so a simple
    take-first-N retains the best levels and drops the tail. *)
let truncate_to_depth levels =
  let rec take n acc = function
    | _ when n <= 0 -> List.rev acc
    | [] -> List.rev acc
    | x :: xs -> take (n - 1) (x :: acc) xs
  in
  if List.length levels <= max_depth then levels
  else take max_depth [] levels

(** Write current local state to ring buffer as snapshot.
    Uses Array.init with List.nth to avoid intermediate List.map allocation. *)
let flush_to_ring store symbol =
  let nb = min (List.length store.local_bids) max_depth in
  let na = min (List.length store.local_asks) max_depth in
  let bids = Array.init nb (fun i ->
    let (p, s) = List.nth store.local_bids i in
    { price = p; size = s }) in
  let asks = Array.init na (fun i ->
    let (p, s) = List.nth store.local_asks i in
    { price = p; size = s }) in
  let ob = { symbol; bids; asks; timestamp = Unix.gettimeofday () } in
  RingBuffer.write store.buffer ob;
  notify_ready store;
  Concurrency.Exchange_wakeup.signal ~symbol

(** Process an orderbook snapshot (full state). *)
let first_snapshot_logged = Atomic.make false

let get_list_field ob_data key1 key2 =
  let open Yojson.Safe.Util in
  let v = member key1 ob_data in
  if v <> `Null then (try to_list v with _ -> [])
  else let v2 = member key2 ob_data in
    if v2 <> `Null then (try to_list v2 with _ -> [])
    else []

let process_orderbook_snapshot ~market_index json =
  let open Yojson.Safe.Util in
  let symbol = match Lighter_instruments_feed.get_symbol ~market_index with
    | Some s -> s
    | None -> string_of_int market_index
  in
  try
    (* Lighter nests orderbook data under "order_book", not "data" *)
    let ob_data = member "order_book" json in
    if ob_data = `Null then begin
      (* Log first null snapshot to diagnose structure *)
      if not (Atomic.exchange first_snapshot_logged true) then begin
        let keys = try List.map fst (to_assoc json) with _ -> [] in
        Logging.info_f ~section "Orderbook snapshot for %s: order_book is null, top-level keys: [%s]"
          symbol (String.concat ", " keys)
      end
    end else begin
      (* Log first non-null snapshot structure for diagnostics *)
      if not (Atomic.exchange first_snapshot_logged true) then begin
        let keys = try List.map fst (to_assoc ob_data) with _ -> [] in
        Logging.info_f ~section "Orderbook snapshot keys for %s: [%s]"
          symbol (String.concat ", " keys)
      end;
      (* Try both abbreviated (b/a) and full (bids/asks) field names *)
      let bids_json = get_list_field ob_data "b" "bids" in
      let asks_json = get_list_field ob_data "a" "asks" in

      let parse_levels levels_json =
        List.map (fun level ->
          let price = Lighter_types.parse_json_float (member "price" level) in
          let size = Lighter_types.parse_json_float (member "size" level) in
          (price, size)
        ) levels_json
      in

      let bids = parse_levels bids_json in
      let asks = parse_levels asks_json in

      let store = ensure_store symbol in
      Mutex.lock store.ob_mutex;
      store.local_bids <- truncate_to_depth (List.sort (fun (a, _) (b, _) -> compare b a) bids);
      store.local_asks <- truncate_to_depth (List.sort (fun (a, _) (b, _) -> compare a b) asks);
      flush_to_ring store symbol;
      Mutex.unlock store.ob_mutex;

      Logging.debug_f ~section "Orderbook snapshot for %s: bids=%d asks=%d"
        symbol (List.length bids) (List.length asks)
    end
  with exn ->
    Logging.warn_f ~section "Failed to parse orderbook snapshot for market %d: %s"
      market_index (Printexc.to_string exn)

(** Process an orderbook delta update. *)
let process_orderbook_update ~market_index json =
  let open Yojson.Safe.Util in
  let symbol = match Lighter_instruments_feed.get_symbol ~market_index with
    | Some s -> s
    | None -> string_of_int market_index
  in
  try
    (* Lighter nests orderbook data under "order_book", not "data" *)
    let ob_data = member "order_book" json in
    if ob_data = `Null then () else begin
      let bids_json = get_list_field ob_data "b" "bids" in
      let asks_json = get_list_field ob_data "a" "asks" in

      let store = ensure_store symbol in
      Mutex.lock store.ob_mutex;

      List.iter (fun level ->
        let price = Lighter_types.parse_json_float (member "price" level) in
        let size = Lighter_types.parse_json_float (member "size" level) in
        store.local_bids <- apply_delta store.local_bids price size ~is_bid:true
      ) bids_json;
      store.local_bids <- truncate_to_depth store.local_bids;

      List.iter (fun level ->
        let price = Lighter_types.parse_json_float (member "price" level) in
        let size = Lighter_types.parse_json_float (member "size" level) in
        store.local_asks <- apply_delta store.local_asks price size ~is_bid:false
      ) asks_json;
      store.local_asks <- truncate_to_depth store.local_asks;

      flush_to_ring store symbol;
      Mutex.unlock store.ob_mutex;

      Logging.debug_f ~section "Orderbook delta for %s: %d bid updates, %d ask updates"
        symbol (List.length bids_json) (List.length asks_json)
    end
  with exn ->
    Logging.warn_f ~section "Failed to parse orderbook update for market %d: %s"
      market_index (Printexc.to_string exn)

let[@inline always] get_latest_orderbook symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

let[@inline always] get_best_bid_ask symbol =
  match get_latest_orderbook symbol with
  | Some { bids; asks; _ } when Array.length bids > 0 && Array.length asks > 0 ->
      let bid = bids.(0) in
      let ask = asks.(0) in
      Some (bid.price, bid.size, ask.price, ask.size)
  | _ -> None

(** Returns all orderbook snapshots written since [last_pos]. *)
let[@inline always] read_orderbook_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Iterates [f] over orderbook snapshots since [last_pos] without list allocation.
    Returns the new cursor position. *)
let[@inline always] iter_orderbook_events symbol last_pos f =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

(** Returns the current ring buffer write position for [symbol]. *)
let[@inline always] get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let has_orderbook_data symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.ready
  | None -> false

let initialize symbols =
  Logging.info_f ~section "Initializing Lighter orderbook feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Lighter orderbook buffer for %s" symbol
  ) symbols
