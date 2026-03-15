(** Hyperliquid Orderbook Feed *)



let section = "hyperliquid_orderbook"
let orderbook_depth = 20

type level = {
  price: string;
  size: string;
  price_float: float;
  size_float: float;
}

type orderbook = {
  symbol: string;
  bids: level array;
  asks: level array;
  timestamp: float;
}

(** Lock-free ring buffer for orderbook data *)
module RingBuffer = Concurrency.Ring_buffer.RingBuffer

module PriceMap = Map.Make(String)

type store = {
  mutable buffer: orderbook RingBuffer.t;
  mutable bids: level PriceMap.t;
  mutable asks: level PriceMap.t;
  ready: bool Atomic.t;
}

let stores : (string, store) Hashtbl.t = Hashtbl.create 32
let ready_condition = Lwt_condition.create ()

let ensure_store symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> store
  | None ->
      let store = {
        buffer = RingBuffer.create 64;
        bids = PriceMap.empty;
        asks = PriceMap.empty;
        ready = Atomic.make false;
      } in
      Hashtbl.add stores symbol store;
      store

let notify_ready store =
  if not (Atomic.get store.ready) then begin
    Atomic.set store.ready true;
    (try Lwt_condition.broadcast ready_condition () with _ -> ())
  end

let get_latest_orderbook symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_latest store.buffer
  | None -> None

let get_best_bid_ask symbol =
  match get_latest_orderbook symbol with
  | Some { bids; asks; _ } when Array.length bids > 0 && Array.length asks > 0 ->
      let bid = bids.(0) in
      let ask = asks.(0) in
      Some (bid.price, bid.size, ask.price, ask.size)
  | _ -> None

let read_orderbook_events symbol last_pos =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.read_since store.buffer last_pos
  | None -> []

let get_current_position symbol =
  match Hashtbl.find_opt stores symbol with
  | Some store -> RingBuffer.get_position store.buffer
  | None -> 0

let parse_levels levels_json =
  let open Yojson.Safe.Util in
  List.map (fun l ->
    let p = member "px" l |> to_string in
    let s = member "sz" l |> to_string in
    { price = p; size = s; price_float = float_of_string p; size_float = float_of_string s }
  ) levels_json

let update_map map levels =
  List.fold_left (fun acc l ->
    if l.size_float = 0.0 then PriceMap.remove l.price acc
    else PriceMap.add l.price l acc
  ) map levels

let map_to_array map sort_desc =
  let lst = PriceMap.fold (fun _ l acc -> l :: acc) map [] in
  let sorted = List.sort (fun l1 l2 ->
    if sort_desc then compare l2.price_float l1.price_float
    else compare l1.price_float l2.price_float
  ) lst in
  let limited = if List.length sorted > orderbook_depth then
    List.filteri (fun i _ -> i < orderbook_depth) sorted
  else sorted in
  Array.of_list limited

let find_registered_symbol coin =
  let result = ref None in
  Hashtbl.iter (fun registered_symbol _ ->
    if registered_symbol = coin || String.starts_with ~prefix:(coin ^ "/") registered_symbol then
      result := Some registered_symbol
  ) stores;
  !result

let process_market_data json =
  let open Yojson.Safe.Util in
  let channel = member "channel" json |> to_string_option in
  match channel with
  | Some "l2Book" ->
      let data = member "data" json in
      let coin = member "coin" data |> to_string in
      (match find_registered_symbol coin with
      | Some symbol ->
          let store = ensure_store symbol in
          let levels_json = member "levels" data |> to_list in
          let bids_json = List.nth levels_json 0 |> to_list in
          let asks_json = List.nth levels_json 1 |> to_list in
          
          store.bids <- update_map store.bids (parse_levels bids_json);
          store.asks <- update_map store.asks (parse_levels asks_json);
          
          let ob = {
            symbol;
            bids = map_to_array store.bids true;
            asks = map_to_array store.asks false;
            timestamp = Unix.gettimeofday ();
          } in
          RingBuffer.write store.buffer ob;
          notify_ready store
      | None -> ())
  | _ -> ()

let _processor_task =
  let sub = Hyperliquid_ws.subscribe_market_data () in
  Lwt.async (fun () ->
    Logging.info ~section "Starting Hyperliquid orderbook processor task";
    Lwt.catch (fun () ->
      Lwt_stream.iter process_market_data sub.stream
    ) (fun exn ->
      Logging.error_f ~section "Hyperliquid orderbook processor task crashed: %s" (Printexc.to_string exn);
      Lwt.return_unit
    )
  )

let initialize symbols =
  Logging.info_f ~section "Initializing Hyperliquid orderbook feed for %d symbols" (List.length symbols);
  List.iter (fun symbol ->
    let _ = ensure_store symbol in
    Logging.debug_f ~section "Created Hyperliquid orderbook buffer for %s" symbol
  ) symbols
