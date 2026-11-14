(** Positions for builder-deployed perp dexs (HIP-3, coin names like [xyz:SPCX]).

    A builder dex reports its user state only through [clearinghouseState] with a [dex]
    argument: [webData2] covers the main dex, and for a unified account the per-dex
    [marginSummary] is explicitly not meaningful (balances and holds live in the spot
    clearinghouse state). So positions are polled per dex and published straight into the
    shared market context, stamped per coin so this feed's freshness cannot be mistaken
    for the main dex's.

    Trading itself is unaffected: HIP-3 orders use the same action and asset id as any
    other perp, which {!Hyperliquid_instruments_feed} resolves from the dex's meta. *)

open Lwt.Infix

let section = "hyperliquid_dex_positions"

(** Poll interval. A dex frame is a small JSON document and the position view only needs
    to be fresh enough for sizing and reconciliation, not tick-accurate. *)
let poll_interval_s = 2.0

let endpoint ~testnet =
  if testnet
  then Uri.of_string "https://api.hyperliquid-testnet.xyz/info"
  else Uri.of_string "https://api.hyperliquid.xyz/info"
;;

let parse_position ~dex (json : Yojson.Safe.t)
  : Dio_exchange.Exchange_intf.Market_context.position option
  =
  let open Yojson.Safe.Util in
  try
    let p = json |> member "position" in
    let raw_coin = p |> member "coin" |> to_string in
    (* Builder coins are namespaced by the exchange; if a payload ever omits the prefix,
       rebuild it from the dex we asked for. *)
    let coin =
      if String.contains raw_coin ':' || String.equal dex ""
      then raw_coin
      else dex ^ ":" ^ raw_coin
    in
    let qty = p |> member "szi" |> to_string |> float_of_string in
    let entry = p |> member "entryPx" |> to_string |> float_of_string in
    let position_value = p |> member "positionValue" |> to_string |> float_of_string in
    let margin_used = p |> member "marginUsed" |> to_string |> float_of_string in
    let unrealized_pnl = p |> member "unrealizedPnl" |> to_string |> float_of_string in
    let liquidation =
      match p |> member "liquidationPx" with
      | `String s ->
        (try Some (float_of_string s) with
         | _ -> None)
      | _ -> None
    in
    let leverage =
      match p |> member "leverage" with
      | `Assoc _ as l ->
        (match l |> member "value" with
         | `Int i -> Some (float_of_int i)
         | `Float f -> Some f
         | _ -> None)
      | _ -> None
    in
    Some
      { Dio_exchange.Exchange_intf.Market_context.coin
      ; qty
      ; entry
      ; position_value
      ; unrealized_pnl
      ; margin_used
      ; liquidation
      ; leverage
      ; updated_at = 0.0
      }
  with
  | exn ->
    Logging.warn_f
      ~section
      "skipping an unparseable %s position: %s"
      dex
      (Printexc.to_string exn);
    None
;;

let fetch_dex ~testnet ~user ~dex =
  let body =
    Yojson.Safe.to_string
      (`Assoc
        [ "type", `String "clearinghouseState"; "user", `String user; "dex", `String dex ])
  in
  let headers = Cohttp.Header.init_with "Content-Type" "application/json" in
  Cohttp_lwt_unix.Client.post
    ~headers
    ~body:(Cohttp_lwt.Body.of_string body)
    (endpoint ~testnet)
  >>= fun (_resp, resp_body) ->
  Cohttp_lwt.Body.to_string resp_body
  >>= fun text -> Lwt.return (Yojson.Safe.from_string text)
;;

(** Publish one dex's positions into the shared market context. Coins the dex no longer
    reports are dropped so a closed position cannot linger. *)
let publish ~now ~dex (payload : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  match
    try Some (payload |> member "assetPositions" |> to_list) with
    | _ -> None
  with
  | None -> ()
  | Some positions ->
    List.iter
      (fun json ->
        match parse_position ~dex json with
        | Some (p : Dio_exchange.Exchange_intf.Market_context.position) ->
          Dio_exchange.Market_context.update_position
            ~at:now
            ~exchange:"hyperliquid"
            { p with updated_at = now }
        | None -> ())
      positions
;;

let start ~testnet ~user ~dexes () =
  let dexes =
    List.filter (fun d -> not (String.equal d "")) (List.sort_uniq String.compare dexes)
  in
  if dexes = []
  then Lwt.return_unit
  else (
    Logging.info_f
      ~section
      "polling builder-dex positions every %.0fs for %s"
      poll_interval_s
      (String.concat "," dexes);
    let rec loop () =
      Lwt_list.iter_p
        (fun dex ->
          Lwt.catch
            (fun () ->
              fetch_dex ~testnet ~user ~dex
              >>= fun payload ->
              publish ~now:(Unix.gettimeofday ()) ~dex payload;
              Lwt.return_unit)
            (fun exn ->
              Logging.debug_f
                ~section
                "poll of dex %s failed: %s"
                dex
                (Printexc.to_string exn);
              Lwt.return_unit))
        dexes
      >>= fun () ->
      Lwt_unix.sleep poll_interval_s
      >>= fun () ->
      (* Spawn the next poll independently: a chained loop retains one Lwt node per
         interval for the process lifetime. *)
      Lwt.async loop;
      Lwt.return_unit
    in
    Lwt.async (fun () -> loop ());
    Lwt.return_unit)
;;
