(** Hyperliquid perp position and margin snapshots from [webData2].

    [webData2] already arrives on the lossless user stream. This module's subscriber
    parses the position set and account summaries into [Dio_exchange.Market_context], so
    strategy code can read them without a venue dependency.

    [assetPositions] is authoritative for the whole account: the store is replaced on
    every frame, which makes a closed position read as flat instead of stale. Frames
    without an [assetPositions] array do not clear the store. *)

open Lwt.Infix

let section = "hyperliquid_positions"
let exchange = "hyperliquid"
let logged_snapshot = ref false
let logged_frame = ref false

let float_of_json json =
  match json with
  | `Float f -> f
  | `Int i -> float_of_int i
  | `String s ->
    (try float_of_string s with
     | _ -> 0.0)
  | _ -> 0.0
;;

let float_opt json =
  match json with
  | `Null -> None
  | `Float f -> Some f
  | `Int i -> Some (float_of_int i)
  | `String s ->
    (try Some (float_of_string s) with
     | _ -> None)
  | _ -> None
;;

let parse_position ~now (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  let p = json |> member "position" in
  let coin = p |> member "coin" |> to_string in
  let qty = p |> member "szi" |> float_of_json in
  let leverage =
    match p |> member "leverage" with
    | `Assoc _ as l -> float_opt (l |> member "value")
    | _ -> None
  in
  { Dio_exchange.Market_context.coin
  ; qty
  ; entry = p |> member "entryPx" |> float_of_json
  ; position_value = p |> member "positionValue" |> float_of_json
  ; unrealized_pnl = p |> member "unrealizedPnl" |> float_of_json
  ; margin_used = p |> member "marginUsed" |> float_of_json
  ; liquidation = float_opt (p |> member "liquidationPx")
  ; leverage
  ; updated_at = now
  }
;;

(** Raw margin inputs from one clearinghouse frame. Kept so the unified-account equity can
    be recomputed when the spot USDC balance moves without a new perp frame. *)
type margin_inputs =
  { per_dex_account_value : float
  ; total_margin_used : float
  ; cross_margin_used : float
  ; isolated_margin : float
  ; maintenance_margin : float
  ; total_ntl_pos : float
  ; withdrawable : float
  ; updated_at : float
  }

let last_margin_inputs : margin_inputs option Atomic.t = Atomic.make None
let logged_unified_margin = ref false

(** Publishes the margin snapshot. For unified/portfolio accounts the per-DEX
    [marginSummary.accountValue] is not meaningful (HL docs), so equity is the spot USDC
    total minus isolated margin. That is the numerator of [get_margin_ratio] (equity /
    maintenance), the inverse of HL's unified account ratio (maintenance / available
    equity). *)
let publish_margin (inputs : margin_inputs) =
  let account_value, withdrawable =
    if Hyperliquid_account_mode.uses_spot_collateral_now ()
    then (
      match Hyperliquid_balances.get_spot_snapshot "USDC" with
      | Some (_, spot_total) ->
        let equity = Float.max 0.0 (spot_total -. inputs.isolated_margin) in
        let transfer_required =
          Float.max inputs.cross_margin_used (0.1 *. inputs.total_ntl_pos)
        in
        if not !logged_unified_margin
        then (
          logged_unified_margin := true;
          Logging.info_f
            ~section
            "unified margin: spot USDC total=%.2f isolated=%.2f => equity=%.2f (per-DEX \
             accountValue=%.2f ignored); maintenance=%.2f"
            spot_total
            inputs.isolated_margin
            equity
            inputs.per_dex_account_value
            inputs.maintenance_margin);
        equity, Float.max 0.0 (equity -. transfer_required)
      | None -> inputs.per_dex_account_value, inputs.withdrawable)
    else inputs.per_dex_account_value, inputs.withdrawable
  in
  Atomic.set last_margin_inputs (Some inputs);
  Dio_exchange.Market_context.update_margin
    ~exchange
    { Dio_exchange.Market_context.account_value
    ; margin_used = inputs.total_margin_used
    ; maintenance_margin = inputs.maintenance_margin
    ; withdrawable
    ; updated_at = inputs.updated_at
    }
;;

(** Re-derives margin equity from the last clearinghouse frame. Called when a spot
    snapshot arrives, since in a unified account that snapshot is the collateral. *)
let recompute_margin_from_last_state () =
  match Atomic.get last_margin_inputs with
  | Some inputs -> publish_margin inputs
  | None -> ()
;;

let () = Hyperliquid_balances.on_spot_update := recompute_margin_from_last_state

(** Applies the inner clearinghouse state used by both [webData2] and the current
    [clearinghouseState] subscription. *)
let apply_clearinghouse_state ~now state =
  let open Yojson.Safe.Util in
  let applied =
    match state |> member "assetPositions" with
    | `Null ->
      if not !logged_snapshot
      then (
        logged_snapshot := true;
        Logging.info_f
          ~section
          "position snapshot: assetPositions absent; treating account as flat");
      true
    | `List positions ->
      let parsed =
        List.filter_map
          (fun p ->
            try
              let pos = parse_position ~now p in
              if pos.Dio_exchange.Market_context.coin = "" then None else Some pos
            with
            | _ -> None)
          positions
      in
      let live =
        List.filter (fun p -> Float.abs p.Dio_exchange.Market_context.qty > 1e-12) parsed
      in
      Dio_exchange.Market_context.replace_positions ~exchange ~updated_at:now live;
      List.iter
        (fun p ->
          Concurrency.Exchange_wakeup.signal
            ~symbol:(p.Dio_exchange.Market_context.coin ^ "/USDC"))
        live;
      if not !logged_snapshot
      then (
        logged_snapshot := true;
        Logging.info_f
          ~section
          "position snapshot: %d live position(s), accountValue=%.2f maintenance=%.2f"
          (List.length live)
          (state |> member "marginSummary" |> member "accountValue" |> float_of_json)
          (state |> member "crossMaintenanceMarginUsed" |> float_of_json));
      true
    | _ -> false
  in
  let summary =
    match state |> member "marginSummary" with
    | `Assoc _ as s -> s
    | _ -> state |> member "crossMarginSummary"
  in
  let cross_margin_used, isolated_margin =
    match state |> member "assetPositions" with
    | `List positions ->
      List.fold_left
        (fun (cross, isolated) p ->
          try
            let pos = p |> member "position" in
            let used = pos |> member "marginUsed" |> float_of_json in
            let kind = pos |> member "leverage" |> member "type" |> to_string in
            if String.equal kind "isolated"
            then cross, isolated +. used
            else cross +. used, isolated
          with
          | _ -> cross, isolated)
        (0.0, 0.0)
        positions
    | _ -> 0.0, 0.0
  in
  (try
     let maintenance =
       match float_opt (state |> member "crossMaintenanceMarginUsed") with
       | Some v -> v
       | None -> summary |> member "totalMarginUsed" |> float_of_json
     in
     publish_margin
       { per_dex_account_value = summary |> member "accountValue" |> float_of_json
       ; total_margin_used = summary |> member "totalMarginUsed" |> float_of_json
       ; cross_margin_used
       ; isolated_margin
       ; maintenance_margin = maintenance
       ; total_ntl_pos = summary |> member "totalNtlPos" |> float_of_json
       ; withdrawable = state |> member "withdrawable" |> float_of_json
       ; updated_at = now
       }
   with
   | _ -> ());
  applied
;;

(** Parses a [webData2] or [clearinghouseState] frame. Returns [true] when a position
    snapshot was applied. *)
let process_market_data (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  if not !logged_frame
  then (
    logged_frame := true;
    match json |> member "channel" with
    | `String c -> Logging.info_f ~section "first user frame: channel=%s" c
    | _ -> ());
  match json |> member "channel" with
  | `String "webData2" ->
    (try
       let data = json |> member "data" in
       let now = Unix.gettimeofday () in
       (match data with
        | `Assoc _ ->
          Dio_exchange.Market_context.mark_positions_updated ~exchange ~updated_at:now
        | _ -> ());
       let state =
         match data |> member "clearinghouseState" with
         | `Assoc _ as s -> s
         | _ -> data |> member "userState" |> member "clearinghouseState"
       in
       apply_clearinghouse_state ~now state
     with
     | exn ->
       Logging.warn_f ~section "webData2 parse failed: %s" (Printexc.to_string exn);
       false)
  | `String "clearinghouseState" ->
    (try
       let data = json |> member "data" in
       let now = Unix.gettimeofday () in
       (match data with
        | `Assoc _ ->
          Dio_exchange.Market_context.mark_positions_updated ~exchange ~updated_at:now
        | _ -> ());
       let state =
         match data |> member "clearinghouseState" with
         | `Assoc _ as s -> s
         | _ -> data
       in
       apply_clearinghouse_state ~now state
     with
     | exn ->
       Logging.warn_f
         ~section
         "clearinghouseState parse failed: %s"
         (Printexc.to_string exn);
       false)
  | `String "allDexsClearinghouseState" ->
    (try
       let data = json |> member "data" in
       let now = Unix.gettimeofday () in
       (match data with
        | `Assoc _ ->
          Dio_exchange.Market_context.mark_positions_updated ~exchange ~updated_at:now
        | _ -> ());
       match data |> member "clearinghouseStates" with
       | `List pairs ->
         List.exists
           (fun pair ->
             match pair with
             | `List [ _dex; state ] -> apply_clearinghouse_state ~now state
             | _ -> false)
           pairs
       | _ -> false
     with
     | exn ->
       Logging.warn_f
         ~section
         "allDexsClearinghouseState parse failed: %s"
         (Printexc.to_string exn);
       false)
  | _ -> false
;;

let get_position ~coin = Dio_exchange.Market_context.get_position ~exchange ~coin
let get_margin () = Dio_exchange.Market_context.get_margin ~exchange

let _processor_task =
  let rec run () =
    let sub = Hyperliquid_ws.subscribe_user_data () in
    Logging.info ~section "Position processor started";
    Lwt.catch
      (fun () ->
        Concurrency.Lwt_util.consume_stream
          (fun json -> ignore (process_market_data json : bool))
          sub.stream
        >>= fun () ->
        sub.close ();
        Logging.debug ~section "Position stream ended, re-subscribing...";
        Lwt.async run;
        Lwt.return_unit)
      (fun exn ->
        sub.close ();
        Logging.error_f
          ~section
          "Position processor crashed: %s. Re-subscribing..."
          (Printexc.to_string exn);
        Lwt.async run;
        Lwt.return_unit)
  in
  Lwt.async run
;;
