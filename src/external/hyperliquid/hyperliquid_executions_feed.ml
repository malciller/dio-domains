(** Hyperliquid Executions Feed *)

open Concurrency

let section = "hyperliquid_executions_feed"

type side = Buy | Sell
type order_status = PendingNewStatus | NewStatus | PartiallyFilledStatus | FilledStatus | CanceledStatus | ExpiredStatus | RejectedStatus | UnknownStatus of string
type open_order = {
  order_id: string; symbol: string; side: side; order_qty: float; cum_qty: float;
  remaining_qty: float; limit_price: float option; order_status: order_status;
  order_userref: int option; cl_ord_id: string option;
}
type execution_event = {
  order_id: string; order_status: order_status; limit_price: float option; side: side;
  order_qty: float; cum_qty: float; timestamp: float;
}

module Execution_bus = Event_bus.Make (struct type t = execution_event end)

type state = {
  open_orders: (string, open_order) Hashtbl.t; (* Key: order_id *)
  bus: (string, Execution_bus.t) Hashtbl.t; (* Key: symbol *)
  mutex: Lwt_mutex.t;
}

let state = {
  open_orders = Hashtbl.create 1024;
  bus = Hashtbl.create 16;
  mutex = Lwt_mutex.create ();
}

let get_bus symbol =
  Lwt_mutex.with_lock state.mutex (fun () ->
    match Hashtbl.find_opt state.bus symbol with
    | Some b -> Lwt.return b
    | None ->
        let b = Execution_bus.create (Printf.sprintf "hl_executions_%s" symbol) in
        Hashtbl.add state.bus symbol b;
        Lwt.return b
  )

let get_open_order _symbol order_id =
  Hashtbl.find_opt state.open_orders order_id

let get_open_orders symbol =
  Hashtbl.fold (fun _ o acc -> if o.symbol = symbol then o :: acc else acc) state.open_orders []

let get_current_position _symbol = 0
let read_execution_events _symbol _start_pos = []

let process_market_data json =
  let open Yojson.Safe.Util in
  try
    let channel = member "channel" json |> to_string_option in
    match channel with
    | Some "webData2" ->
        (* TODO: parse user fills/order updates and update state.open_orders + publish to Event_bus *)
        Logging.debug ~section "Received webData2 user event, parsing not fully implemented";
        ()
    | _ -> ()
  with _ -> ()

let _processor_task =
  let sub = Hyperliquid_ws.subscribe_market_data () in
  Lwt_stream.iter process_market_data sub.stream
