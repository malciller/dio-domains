(** Hyperliquid Ticker Feed *)

open Lwt.Infix
open Concurrency

let section = "hyperliquid_ticker_feed"

type ticker = { bid: float; ask: float; timestamp: float }

module Ticker_bus = Event_bus.Make (struct type t = ticker end)

type state = {
  tickers: (string, ticker) Hashtbl.t;
  bus: (string, Ticker_bus.t) Hashtbl.t;
  mutex: Lwt_mutex.t;
}

let state = {
  tickers = Hashtbl.create 16;
  bus = Hashtbl.create 16;
  mutex = Lwt_mutex.create ();
}

let get_bus symbol =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match Hashtbl.find_opt state.bus symbol with
    | Some b -> Lwt.return b
    | None ->
        let b = Ticker_bus.create (Printf.sprintf "hl_ticker_%s" symbol) in
        Hashtbl.add state.bus symbol b;
        Lwt.return b
  )

let get_latest_ticker symbol =
  Hashtbl.find_opt state.tickers symbol

let get_current_position _symbol = 0

let read_ticker_events _symbol _start_pos = []

let parse_l2book_for_ticker json =
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
         | [ bids; asks ] ->
             let extract_top lst =
               match to_list lst with
               | top :: _ -> (member "px" top |> to_string |> float_of_string)
               | [] -> 0.0
             in
             let top_bid = extract_top bids in
             let top_ask = extract_top asks in
             if top_bid > 0.0 && top_ask > 0.0 then
               Some (coin, { bid = top_bid; ask = top_ask; timestamp = time_ms /. 1000.0 })
             else None
         | _ -> None)
    | _ -> None
  with _ -> None

let process_market_data json =
  match parse_l2book_for_ticker json with
  | Some (symbol, new_ticker) ->
      Lwt.async (fun () ->
        Lwt_mutex.with_lock state.mutex (fun () ->
          Hashtbl.replace state.tickers symbol new_ticker;
          Lwt.return_unit
        ) >>= fun () ->
        get_bus symbol >|= fun bus ->
        Ticker_bus.publish bus new_ticker
      )
  | None -> ()

let _processor_task =
  let sub = Hyperliquid_ws.subscribe_market_data () in
  Lwt_stream.iter process_market_data sub.stream
