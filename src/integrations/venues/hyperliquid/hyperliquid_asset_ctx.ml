(** Hyperliquid [activeAssetCtx] feed: perp funding, premium, mark, and oracle prices.

    The channel is subscribed per perp coin in [Hyperliquid_ws.subscribe_to_feeds] and is
    routed on the lossless user-state stream (it is low-frequency but must not be evicted
    by an l2Book burst). The parsed snapshot is published to [Dio_exchange.Market_context]
    so strategy code can read it without depending on this package. *)

open Lwt.Infix

let section = "hyperliquid_asset_ctx"
let exchange = "hyperliquid"
let logged_coins : (string, unit) Hashtbl.t = Hashtbl.create 8

let float_of_json json =
  match json with
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

(** Parses one frame. Unknown channels, malformed data, and frames without a positive mark
    are ignored. *)
let process_market_data (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  match json |> member "channel" with
  | `String "activeAssetCtx" ->
    (try
       let data = json |> member "data" in
       let coin = data |> member "coin" |> to_string in
       let ctx = data |> member "ctx" in
       let mark = ctx |> member "markPx" |> float_of_json in
       let oracle = ctx |> member "oraclePx" |> float_of_json in
       if coin <> "" && mark > 0.0
       then
         Dio_exchange.Market_context.update_funding
           ~exchange
           ~coin
           { funding = ctx |> member "funding" |> float_of_json
           ; premium = ctx |> member "premium" |> float_of_json
           ; mark
           ; oracle
           ; updated_at = Unix.gettimeofday ()
           };
       Concurrency.Exchange_wakeup.signal ~symbol:(coin ^ "/USDC");
       if not (Hashtbl.mem logged_coins coin)
       then (
         Hashtbl.replace logged_coins coin ();
         Logging.info_f
           ~section
           "funding snapshot %s: mark=%.4f oracle=%.4f premium=%.6f funding=%.8f"
           coin
           mark
           oracle
           (ctx |> member "premium" |> float_of_json)
           (ctx |> member "funding" |> float_of_json))
     with
     | _ -> ())
  | _ -> ()
;;

let get ~coin = Dio_exchange.Market_context.get_funding ~exchange ~coin

let funding_apr ~coin =
  match get ~coin with
  | Some c -> Some (c.funding *. 24.0 *. 365.0)
  | None -> None
;;

let mark ~coin =
  match get ~coin with
  | Some c -> Some c.mark
  | None -> None
;;

(** Lossless user-stream processor. Mirrors the balances/executions tasks: on stream end
    or crash, resubscribe. *)
let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_user_data () in
    Logging.info ~section "Asset-context processor started";
    Lwt.catch
      (fun () ->
        Concurrency.Lwt_util.consume_stream process_market_data sub.stream
        >>= fun () ->
        sub.close ();
        Logging.debug ~section "Asset-context stream ended, re-subscribing...";
        Lwt.async run;
        Lwt.return_unit)
      (fun exn ->
        sub.close ();
        Logging.error_f
          ~section
          "Asset-context processor crashed: %s. Re-subscribing..."
          (Printexc.to_string exn);
        Lwt.async run;
        Lwt.return_unit)
  in
  Lwt.async run
;;
