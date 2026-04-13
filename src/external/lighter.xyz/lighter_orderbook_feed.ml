(** Lighter Exchange Level 2 order book feed processing module.
    Responsible for managing WebSocket subscriptions to the order_book channels mapped by market index.
    The module handles an initial full state snapshot upon channel subscription,
    followed by processing continuous incremental price level deltas.
    State representation utilizes per symbol lock free ring buffers to maintain thread safe
    and non blocking access for low latency logic sequence consumers. *)

let section = "lighter_orderbook"

let ring_buffer_size = 16

(** Constant defining the maximum permitted depth of price levels retained per side of the order book.
    Imposes strict bounds on memory allocation and ensures structural alignment with downstream
    consumers that strictly require localized top of book liquidity evaluation. *)
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

(** Instantiation of a lock free Single Producer Single Consumer ring buffer
    specialized for storing dense order book snapshot configurations. *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

(** Per symbol state store encapsulating the ring buffer implementation, 
    an atomic readiness mechanism for synchronous initialization detection,
    and a mutually exclusive protected local state representation for delta application. *)
type store = {
  buffer: orderbook RingBuffer.t;
  ready: bool Atomic.t;
  (* Local mutable order book state utilized exclusively for sequential incremental update evaluations *)
  mutable local_bids: (float * float) list;  (** Maintains strictly sorted descending sequence by price coordinate *)
  mutable local_asks: (float * float) list;  (** Maintains strictly sorted ascending sequence by price coordinate *)
  ob_mutex: Mutex.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()
let initialization_mutex = Mutex.create ()

let get_store symbol =
  Hashtbl.find_opt stores symbol

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

(** Executes a single pass algorithmic substitution to merge incremental price level updates
    into a systematically sorted linked list of existing price levels.
    A received size value mapping to zero dictates explicit level removal,
    whereas non zero sizing evaluates to either an overriding upsert or a novel insertion.
    Because the input data structures maintain sorted invariances, updates mutating 
    extant levels will reconstruct the antecedent prefix while safely sharing the tail allocation.
    Novel price level insertions involve splicing the datum into sequence with an optimal 
    linear bounds scan, thereby averting the overhead of a post insertion sorting pass. *)
let apply_delta levels price size ~is_bid =
  if size = 0.0 then
    List.filter (fun (p, _) -> p <> price) levels
  else
    (* Single-pass upsert-or-insert over sorted list.
       [cmp] encodes sort direction: bids descending, asks ascending. *)
    let cmp = if is_bid
      then (fun a b -> compare b a)  (* descending *)
      else (fun a b -> compare a b)  (* ascending  *)
    in
    let rec go acc = function
      | [] ->
          (* Price not found; append at end (worst level). *)
          List.rev_append acc [(price, size)]
      | (p, _) :: tl when p = price ->
          (* Update existing level, share the tail. *)
          List.rev_append acc ((p, size) :: tl)
      | ((p, _) as hd) :: tl ->
          if cmp price p < 0 then
            (* Insert before this element (better level). *)
            List.rev_append acc ((price, size) :: hd :: tl)
          else
            go (hd :: acc) tl
    in
    go [] levels

(** Enforces structural truncation on a price sequence list to adhere to the defined maximum depth threshold.
    Due to the guaranteed pre sorted nature of the sequences, applying a sequential bounding procedure
    selectively preserves the highest priority liquidity layers while systematically dropping
    extraneous low priority tail records. *)
let truncate_to_depth levels =
  let rec take n acc = function
    | _ when n <= 0 -> List.rev acc
    | [] -> List.rev acc
    | x :: xs -> take (n - 1) (x :: acc) xs
  in
  if List.length levels <= max_depth then levels
  else take max_depth [] levels

(** Performs a conversion of the current local mutable list state into an immutable array snapshot,
    publishing the finalized form directly into the lock free ring buffer construct.
    Leverages initialization mechanisms coupled with optimized positional accessors 
    to expressly circumvent intermediate list mapping allocations and prevent excessive garbage collector pressure. *)
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

(** Processes an initial order book snapshot representing the fully synthesized dimensional state 
    at the time of channel inception. *)
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
    (* The Lighter exchange API structure specifies payload nesting under the order_book key rather than the standard data key. *)
    let ob_data = member "order_book" json in
    if ob_data = `Null then begin
      (* Captures diagnostic telemetry on null order book structures for structural debugging. *)
      if not (Atomic.exchange first_snapshot_logged true) then begin
        let keys = try List.map fst (to_assoc json) with _ -> [] in
        Logging.info_f ~section "Orderbook snapshot for %s: order_book is null, top-level keys: [%s]"
          symbol (String.concat ", " keys)
      end
    end else begin
      (* Logs the non null schema structure for internal verification purposes. *)
      if not (Atomic.exchange first_snapshot_logged true) then begin
        let keys = try List.map fst (to_assoc ob_data) with _ -> [] in
        Logging.info_f ~section "Orderbook snapshot keys for %s: [%s]"
          symbol (String.concat ", " keys)
      end;
      (* Resolves polymorphic field names to gracefully support both the abbreviated and fully expanded JSON properties. *)
      let bids_json = get_list_field ob_data "b" "bids" in
      let asks_json = get_list_field ob_data "a" "asks" in

      let parse_level_opt level =
        match level with
        | `List [price_json; size_json] ->
            let price = Lighter_types.parse_json_float price_json in
            let size = Lighter_types.parse_json_float size_json in
            Some (price, size)
        | `List (price_json :: size_json :: _) ->
            let price = Lighter_types.parse_json_float price_json in
            let size = Lighter_types.parse_json_float size_json in
            Some (price, size)
        | `Assoc _ ->
            let price = Lighter_types.parse_json_float (member "price" level) in
            let size = Lighter_types.parse_json_float (member "size" level) in
            Some (price, size)
        | _ -> None
      in

      let parse_levels levels_json =
        List.filter_map parse_level_opt levels_json
      in

      let bids = parse_levels bids_json in
      let asks = parse_levels asks_json in

      match get_store symbol with
      | None ->
          Logging.warn_f ~section "Orderbook snapshot for uninitialized symbol: %s" symbol
      | Some store ->
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

(** Systematically parses and applies an incoming order book delta update,
    routing incremental price level mutations to the locally synchronized cache representation. *)
let process_orderbook_update ~market_index json =
  let open Yojson.Safe.Util in
  let symbol = match Lighter_instruments_feed.get_symbol ~market_index with
    | Some s -> s
    | None -> string_of_int market_index
  in
  try
    (* The Lighter exchange API structure specifies payload nesting under the order_book key rather than the standard data key. *)
    let ob_data = member "order_book" json in
    if ob_data = `Null then () else begin
      let bids_json = get_list_field ob_data "b" "bids" in
      let asks_json = get_list_field ob_data "a" "asks" in

      match get_store symbol with
      | None -> ()
      | Some store ->
          Mutex.lock store.ob_mutex;

          let parse_level_opt level =
            match level with
            | `List [price_json; size_json] ->
                let price = Lighter_types.parse_json_float price_json in
                let size = Lighter_types.parse_json_float size_json in
                Some (price, size)
            | `List (price_json :: size_json :: _) ->
                let price = Lighter_types.parse_json_float price_json in
                let size = Lighter_types.parse_json_float size_json in
                Some (price, size)
            | `Assoc _ ->
                let price = Lighter_types.parse_json_float (member "price" level) in
                let size = Lighter_types.parse_json_float (member "size" level) in
                Some (price, size)
            | _ -> None
          in

          let b_len = ref (List.length store.local_bids) in
          List.iter (fun level ->
            match parse_level_opt level with
            | Some (price, size) ->
                store.local_bids <- apply_delta store.local_bids price size ~is_bid:true;
                incr b_len;
                if !b_len >= max_depth * 2 then begin
                  store.local_bids <- truncate_to_depth store.local_bids;
                  b_len := max_depth
                end
            | None -> ()
          ) bids_json;
          store.local_bids <- truncate_to_depth store.local_bids;

          let a_len = ref (List.length store.local_asks) in
          List.iter (fun level ->
            match parse_level_opt level with
            | Some (price, size) ->
                store.local_asks <- apply_delta store.local_asks price size ~is_bid:false;
                incr a_len;
                if !a_len >= max_depth * 2 then begin
                  store.local_asks <- truncate_to_depth store.local_asks;
                  a_len := max_depth
                end
            | None -> ()
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

let[@inline always] get_best_bid_ask_fast symbol =
  match get_store symbol with
  | Some store ->
      (fun () ->
         match RingBuffer.read_latest store.buffer with
         | Some { bids; asks; _ } when Array.length bids > 0 && Array.length asks > 0 ->
             let bid = bids.(0) in
             let ask = asks.(0) in
             Some (bid.price, bid.size, ask.price, ask.size)
         | _ -> None
      )
  | None -> (fun () -> None)

(** Materializes a vector of all fully structured order book snapshots seamlessly appended
    into the ring buffer subsequent to the specified logical cursor position. *)
let[@inline always] read_orderbook_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

(** Implements an algorithmic traversal applying the provided function across all order book
    snapshots committed following the supplied logical cursor.
    This operation inherently avoids allocating intermediate structures and
    returns the incremented terminal cursor position upon completion. *)
let[@inline always] iter_orderbook_events symbol last_pos f =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.iter_since store.buffer last_pos f
  | None -> last_pos

(** Resolves the currently active write sequence cursor position for the ring buffer paired with the provided symbol. *)
let[@inline always] get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let[@inline always] get_current_position_fast symbol =
  match get_store symbol with
  | Some store -> (fun () -> RingBuffer.get_position store.buffer)
  | None -> (fun () -> 0)

let has_orderbook_data symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> Atomic.get store.ready
  | None -> false

let wait_for_orderbook_data symbols timeout_seconds =
  Concurrency.Lwt_util.poll_until
    ~timeout:timeout_seconds
    ~wait_signal:(fun () -> Lwt_condition.wait ready_condition)
    ~check:(fun () -> List.for_all has_orderbook_data symbols)

let initialize symbols =
  Logging.info_f ~section "Initializing Lighter orderbook feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    Mutex.lock initialization_mutex;
    if not (Hashtbl.mem stores symbol) then begin
      let store = {
        buffer = RingBuffer.create ring_buffer_size;
        ready = Atomic.make false;
        local_bids = [];
        local_asks = [];
        ob_mutex = Mutex.create ();
      } in
      Hashtbl.add stores symbol store;
    end;
    Mutex.unlock initialization_mutex;
    Logging.debug_f ~section "Created Lighter orderbook buffer for %s" symbol
  ) symbols
