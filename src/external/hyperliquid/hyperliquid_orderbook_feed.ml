(** Hyperliquid Orderbook Feed *)

open Lwt.Infix
open Concurrency

let section = "hyperliquid_orderbook_feed"

type level = { price: string; size: string }
type orderbook = { bids: level array; asks: level array; timestamp: float }

module Orderbook_bus = Event_bus.Make (struct type t = orderbook end)

type state = {
  books: (string, orderbook) Hashtbl.t;
  bus: (string, Orderbook_bus.t) Hashtbl.t;
  mutex: Lwt_mutex.t;
}

let state = {
  books = Hashtbl.create 16;
  bus = Hashtbl.create 16;
  mutex = Lwt_mutex.create ();
}

let get_bus symbol =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match Hashtbl.find_opt state.bus symbol with
    | Some b -> Lwt.return b
    | None ->
        let b = Orderbook_bus.create (Printf.sprintf "hl_orderbook_%s" symbol) in
        Hashtbl.add state.bus symbol b;
        Lwt.return b
  )

let get_best_bid_ask symbol =
  match Hashtbl.find_opt state.books symbol with
  | Some book ->
      let bid_px, bid_sz = if Array.length book.bids > 0 then book.bids.(0).price, book.bids.(0).size else "0", "0" in
      let ask_px, ask_sz = if Array.length book.asks > 0 then book.asks.(0).price, book.asks.(0).size else "0", "0" in
      Some (bid_px, bid_sz, ask_px, ask_sz)
  | None -> None

let get_current_position _symbol = 0
let read_orderbook_events _symbol _start_pos = []

let parse_l2book json =
  let open Yojson.Safe.Util in
  try
    let channel = member "channel" json |> to_string_option in
    match channel with
    | Some "l2Book" ->
        let data = member "data" json in
        let coin = member "coin" data |> to_string in
        let time_ms = member "time" data |> to_float in
        let levels = member "levels" data |> to_list in
        (match levels with
         | [ bids_json; asks_json ] ->
             let extract_levels lst =
               lst |> to_list |> List.map (fun level ->
                 {
                   price = member "px" level |> to_string;
                   size = member "sz" level |> to_string;
                 }
               ) |> Array.of_list
             in
             let bids = extract_levels bids_json in
             let asks = extract_levels asks_json in
             Some (coin, { bids; asks; timestamp = time_ms /. 1000.0 })
         | _ -> None)
    | _ -> None
  with _ -> None

let process_market_data json =
  match parse_l2book json with
  | Some (symbol, new_book) ->
      Lwt.async (fun () ->
        Lwt_mutex.with_lock state.mutex (fun () ->
          Hashtbl.replace state.books symbol new_book;
          Lwt.return_unit
        ) >>= fun () ->
        get_bus symbol >|= fun bus ->
        Orderbook_bus.publish bus new_book
      )
  | None -> ()

let _processor_task =
  let sub = Hyperliquid_ws.subscribe_market_data () in
  Lwt_stream.iter process_market_data sub.stream
