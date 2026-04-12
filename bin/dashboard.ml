(** Terminal dashboard for the Dio trading engine.

    Runs as an out-of-process binary for crash isolation. Connects to the
    engine over a Unix domain socket using a length-prefixed JSON protocol
    in watch mode, where the engine pushes snapshot frames on each tick.

    Panels:
    - Header:     uptime, fear-and-greed index, per-exchange connectivity
    - Memory/GC:  heap size, live/free KB, major/minor collections, compactions
    - Holdings:   per-strategy and non-strategy balances, mid-price, accumulated
                  holdings, pending buy/sell distances, unrealized sell value,
                  aggregated portfolio summary (cash, accum val, hold val, sell val)
    - Latency:    per-domain percentile profiling (p50, p90, p99, p999)
    - Domains:    running/stopped status, restart count, last restart age

    Rendering uses atomic frame writes via a pipe-based buffer to avoid
    EAGAIN partial-frame artifacts. Synchronized update ANSI sequences
    (DEC mode 2026) bracket each frame to eliminate flicker.

    The main loop manages raw terminal mode, alternate screen buffer, and
    automatic reconnection with PID-based socket discovery on engine restart.

    Usage: ./dio-dashboard [--socket /tmp/dio-<pid>.sock]
*)

open Notty

(* UDS Client *)

let socket_path = ref ""

(** Default socket path matching the engine's fixed location. *)
let default_socket_path = "/var/run/dio/dashboard.sock"

(** Discover candidate engine socket paths.
    Checks the fixed path first, then falls back to scanning /tmp/dio-*.sock
    for backward compatibility with older engine versions. *)
let discover_socket_candidates () =
  let fixed = [default_socket_path] |> List.filter Sys.file_exists in
  if fixed <> [] then fixed
  else begin
    let entries = try Sys.readdir "/tmp" with _ -> [||] in
    Array.to_list entries
    |> List.filter (fun f ->
      String.length f > 4 && String.sub f 0 4 = "dio-" &&
      let len = String.length f in
      String.sub f (len - 5) 5 = ".sock")
    |> List.sort (fun a b -> String.compare b a)
    |> List.map (fun f -> "/tmp/" ^ f)
  end

(** Read exactly [len] bytes from [fd] into [buf] at offset [off]. *)
let read_exact fd buf off len =
  let rec loop off remaining =
    if remaining = 0 then ()
    else
      let n = Unix.read fd buf off remaining in
      if n = 0 then raise End_of_file;
      loop (off + n) (remaining - n)
  in
  loop off len

(** Read a 4-byte big-endian length-prefixed JSON message from [fd]. *)
let read_message fd =
  let header = Bytes.create 4 in
  read_exact fd header 0 4;
  let len =
    (Bytes.get_uint8 header 0 lsl 24) lor
    (Bytes.get_uint8 header 1 lsl 16) lor
    (Bytes.get_uint8 header 2 lsl 8) lor
    (Bytes.get_uint8 header 3)
  in
  if len > 10_000_000 then failwith "message too large";
  let payload = Bytes.create len in
  read_exact fd payload 0 len;
  Bytes.to_string payload

(** Connect to the engine UDS at [path] and send the watch-mode command. *)
let connect_and_watch path =
  let fd = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.connect fd (Unix.ADDR_UNIX path);
  (* Send watch-mode command *)
  let _ = Unix.write_substring fd "W" 0 1 in
  fd

(* JSON helpers *)

let ( |?> ) json key =
  match json with
  | `Assoc l -> (try List.assoc key l with Not_found -> `Null)
  | _ -> `Null

let to_string_d d = function `String s -> s | _ -> d
let to_float_d d = function `Float f -> f | `Int i -> float_of_int i | _ -> d
let to_int_d d = function `Int i -> i | `Float f -> int_of_float f | _ -> d
let to_bool_d d = function `Bool b -> b | _ -> d
let to_list_d = function `List l -> l | _ -> []

(* Formatting *)

let format_duration secs =
  let s = int_of_float secs in
  if s < 60 then Printf.sprintf "%ds" s
  else if s < 3600 then Printf.sprintf "%dm%02ds" (s / 60) (s mod 60)
  else if s < 86400 then Printf.sprintf "%dh%02dm" (s / 3600) ((s mod 3600) / 60)
  else Printf.sprintf "%dd%02dh" (s / 86400) ((s mod 86400) / 3600)

let format_price f =
  if f >= 10000.0 then Printf.sprintf "$%.0f" f
  else if f >= 100.0 then Printf.sprintf "$%.1f" f
  else if f >= 1.0 then Printf.sprintf "$%.2f" f
  else Printf.sprintf "$%.4f" f

let format_qty f =
  if f >= 1.0 then Printf.sprintf "%.4f" f
  else Printf.sprintf "%.6f" f

let format_pnl f =
  if f >= 0.0 then Printf.sprintf "+$%.2f" f
  else Printf.sprintf "-$%.2f" (abs_float f)

let format_latency_us f =
  if f >= 1000.0 then Printf.sprintf "%.1fms" (f /. 1000.0)
  else Printf.sprintf "%.0fus" f

let truncate_string n s =
  if String.length s <= n then s
  else String.sub s 0 (n - 1) ^ "."

(* Color palette: RGB-888 constants for the TUI theme *)

let c_bg         = A.rgb_888 ~r:14  ~g:14  ~b:18
let c_panel      = A.rgb_888 ~r:24  ~g:24  ~b:32
let c_section_bg = A.rgb_888 ~r:30  ~g:30  ~b:40
let c_border     = A.rgb_888 ~r:60  ~g:60  ~b:75
let c_title      = A.rgb_888 ~r:255 ~g:255 ~b:255
let c_accent     = A.rgb_888 ~r:167 ~g:139 ~b:250
let c_label      = A.rgb_888 ~r:150 ~g:150 ~b:165
let c_text       = A.rgb_888 ~r:230 ~g:230 ~b:240
let c_bright     = A.rgb_888 ~r:255 ~g:255 ~b:255
let c_green      = A.rgb_888 ~r:52  ~g:211 ~b:153
let c_red        = A.rgb_888 ~r:248 ~g:113 ~b:113
let c_yellow     = A.rgb_888 ~r:251 ~g:191 ~b:36
let c_cyan       = A.rgb_888 ~r:34  ~g:211 ~b:238
let c_dim        = A.rgb_888 ~r:100 ~g:100 ~b:120
let c_near_fill  = A.rgb_888 ~r:25  ~g:45  ~b:35
(* Per-exchange brand colors *)
let c_exch_hl    = A.rgb_888 ~r:52  ~g:211 ~b:153  (* hyperliquid: vivid green *)
let c_exch_kr    = A.rgb_888 ~r:192 ~g:132 ~b:252  (* kraken: vivid purple *)
let c_exch_li    = A.rgb_888 ~r:96  ~g:165 ~b:250  (* lighter: bright blue *)
let c_exch_ib    = A.rgb_888 ~r:251 ~g:146 ~b:60   (* ibkr: bright orange *)
(* Attribute constructors: foreground + background + optional style *)

let a_label      = A.(fg c_label  ++ bg c_bg)
let a_text       = A.(fg c_text   ++ bg c_bg)
let a_bright     = A.(fg c_bright ++ bg c_bg         ++ st bold)
let a_green      = A.(fg c_green  ++ bg c_bg)
let a_red        = A.(fg c_red    ++ bg c_bg)
let a_yellow     = A.(fg c_yellow ++ bg c_bg)
let a_cyan       = A.(fg c_cyan   ++ bg c_bg)
let a_dim        = A.(fg c_dim    ++ bg c_bg)
let a_border     = A.(fg c_border ++ bg c_bg)
(* Near-fill highlight: subtle green-tinted background for rows close to buy fill *)
let a_near_fill  = A.(fg c_bright ++ bg c_near_fill ++ st bold)
let a_near_fill_green = A.(fg c_green ++ bg c_near_fill ++ st bold)

let c_near_sell  = A.rgb_888 ~r:75  ~g:35  ~b:35
let a_near_sell  = A.(fg c_bright ++ bg c_near_sell ++ st bold)
let a_near_sell_red = A.(fg c_red ++ bg c_near_sell ++ st bold)

(** Exchange-specific color for the SYMBOL column. Bold for active strategies. *)
let exch_sym_attr ?(dim=false) exchange =
  let c = match exchange with
    | "hyperliquid" -> c_exch_hl
    | "kraken"      -> c_exch_kr
    | "lighter"     -> c_exch_li
    | "ibkr"        -> c_exch_ib
    | _             -> c_bright
  in
  if dim then A.(fg c ++ bg c_bg)
  else A.(fg c ++ bg c_bg ++ st bold)


(* Drawing primitives *)



let pad_right w s =
  let len = String.length s in
  if len >= w then String.sub s 0 w
  else s ^ String.make (w - len) ' '

let col w attr s = I.string attr (pad_right w s)

let format_pct f =
  if abs_float f < 0.01 then "<0.01%"
  else if abs_float f >= 10.0 then Printf.sprintf "%.1f%%" f
  else Printf.sprintf "%.2f%%" f

let format_spread_bps bid ask =
  if bid > 0.0 && ask > 0.0 then
    let spread_bps = ((ask -. bid) /. ((bid +. ask) /. 2.0)) *. 10000.0 in
    if spread_bps >= 100.0 then Printf.sprintf "%.0fbp" spread_bps
    else Printf.sprintf "%.1fbp" spread_bps
  else "--"

(* Drawing helpers *)

let section_title w label =
  let lbl = " ╭── " ^ label ^ " " in
  let pad_count = max 0 (w - String.length lbl) in
  let pad_buf = Buffer.create (pad_count * 3) in
  for _ = 1 to pad_count do Buffer.add_string pad_buf "─" done;
  I.hcat [
    I.string A.(fg c_title ++ bg c_bg ++ st bold) lbl;
    I.string A.(fg c_border ++ bg c_bg) (Buffer.contents pad_buf);
  ]

(* Panel: Header bar *)

let render_header w json =
  let uptime = json |?> "uptime_s" |> to_float_d 0.0 in
  let fng    = json |?> "fear_and_greed" |> to_float_d 0.0 in
  (* Per-exchange connectivity: green if any strategy has a live bid/ask
     feed on that exchange, red otherwise. Deduplicated and sorted. *)
  let exch_connected =
    let strats = match json |?> "strategies" with `Assoc l -> l | _ -> [] in
    let tbl = Hashtbl.create 4 in
    List.iter (fun (_sym, data) ->
      let exch = data |?> "exchange" |> to_string_d "" in
      if exch <> "" then begin
        let market = data |?> "market" in
        let bid = market |?> "bid" |> to_float_d 0.0 in
        let ask = market |?> "ask" |> to_float_d 0.0 in
        let live = bid > 0.0 && ask > 0.0 in
        let cur = try Hashtbl.find tbl exch with Not_found -> false in
        Hashtbl.replace tbl exch (cur || live)
      end
    ) strats;
    let pairs = Hashtbl.fold (fun k v acc -> (k, v) :: acc) tbl [] in
    List.sort (fun (a, _) (b, _) -> String.compare a b) pairs
  in
  let conn_imgs, conn_w = List.fold_right (fun (exch, live) (imgs, w_acc) ->
    let tag = match exch with
      | "kraken"       -> "kraken"
      | "hyperliquid"  -> "hyperliquid"
      | e              -> truncate_string 10 e
    in
    let dot_attr =
      if live then A.(fg c_green ++ bg c_panel)
              else A.(fg c_red   ++ bg c_panel)
    in
    let seg = I.hcat [
      I.string A.(fg c_dim ++ bg c_panel) "  │  ";
      I.string dot_attr "◉";
      I.string A.(fg c_label ++ bg c_panel) (" " ^ tag);
    ] in
    (seg :: imgs, w_acc + 5 + 1 + 1 + String.length tag)
  ) exch_connected ([], 0) in
  let dur_str = format_duration uptime in
  let fng_str = Printf.sprintf "%.0f" fng in
  let base_w = 2 + 3 + 8 + 3 + String.length dur_str
             + 5 + 4 + String.length fng_str in
  I.hcat (
    [ I.string A.(bg c_panel) "  ";
      I.string A.(fg c_accent ++ bg c_panel ++ st bold) "Dio;";
      I.string A.(fg c_dim ++ bg c_panel) "        │  ";
      I.string A.(fg c_label ++ bg c_panel) "up ";
      I.string A.(fg c_text ++ bg c_panel) dur_str;
      I.string A.(fg c_dim ++ bg c_panel) "  │  ";
      I.string A.(fg c_label ++ bg c_panel) "f&g ";
      I.string A.(fg (if fng >= 60.0 then c_green
                      else if fng >= 40.0 then c_yellow
                      else c_red) ++ bg c_panel ++ st bold)
               fng_str;
    ]
    @ conn_imgs
    @ [ I.string A.(bg c_panel) (String.make (max 0 (w - base_w - conn_w)) ' ') ]
  )

(* Panel: Holdings and Strategy *)

let exch_tag_of = function
  | "kraken" -> "krkn" | "hyperliquid" -> "hypr"
  | "lighter" -> "ltr" | "ibkr" -> "ibkr"
  | e -> String.sub e 0 (min 3 (String.length e))

let render_strategies w json =
  let strats = match json |?> "strategies" with `Assoc l -> l | _ -> [] in
  let all_balances = json |?> "all_balances" |> to_list_d in
  (* Column header row *)
  let header = I.hcat [
    I.string a_border " │  ";
    col 16 a_label "SYMBOL";
    col 5 a_label "STGY";
    col 3 a_label "ST";
    col 12 a_label "PRICE";
    col 8 a_label "SPREAD";
    I.string a_border " │ ";
    col 12 a_label "HOLDING";
    col 10 a_label "HOLD VAL";
    col 12 a_label "ACCUM QTY";
    col 10 a_label "ACCUM VAL";
    I.string a_border " │ ";
    col 12 a_label "BUY @";
    I.string a_label "Δ BUY   ";
    I.string a_border " │ ";
    col 6 a_label "SELLS";
    I.string a_label "Δ SELL  ";
    col 12 a_label "SELL VAL";
    I.string a_border " │ ";
    col 19 a_label "";
  ] in

  (* Build one row per strategy *)
  let strategy_rows = List.map (fun (symbol, data) ->
    let exchange = data |?> "exchange" |> to_string_d "?" in
    let strat = data |?> "strategy" in
    let market = data |?> "market" in
    let stype = strat |?> "type" |> to_string_d "?" in
    let cap_low = strat |?> "capital_low" |> to_bool_d false in
    let _asset_low = strat |?> "asset_low" |> to_bool_d false in

    (* Market data: compute mid-price from bid/ask *)
    let bid = market |?> "bid" |> to_float_d 0.0 in
    let ask = market |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0
              else (max bid ask) in
    let base_bal = market |?> "base_balance" |> to_float_d 0.0 in
    let hold_value = base_bal *. mid in

    (* Strategy state: next buy price and completed sell count *)
    let buy_price = strat |?> "buy_price" |> to_float_d 0.0 in
    let sell_count = strat |?> "sell_count" |> to_int_d 0 in

    (* Sum mark-to-market proceeds and quantity across pending sell orders *)
    let sell_orders = strat |?> "sell_orders" |> to_list_d in
    let unrealized_profit, pending_sell_qty = List.fold_left (fun (up, qty_acc) s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then (up +. (sp *. sq), qty_acc +. sq)
      else (up, qty_acc)
    ) (0.0, 0.0) sell_orders in
    
    let accum_holding = max 0.0 (base_bal -. pending_sell_qty) in
    let accum_hold_value = accum_holding *. mid in

    (* Percentage distance from mid-price to pending buy, if active *)
    let buy_dist_pct =
      if (not cap_low) && buy_price > 0.0 && mid > 0.0 then
        Some (((buy_price -. mid) /. mid) *. 100.0)
      else None
    in

    (* Percentage distance from mid-price to nearest pending sell *)
    let closest_sell_dist_pct =
      let sell_prices = List.filter_map (fun s ->
        let sp = s |?> "price" |> to_float_d 0.0 in
        if sp > 0.0 && mid > 0.0 then Some (((sp -. mid) /. mid) *. 100.0)
        else None
      ) sell_orders in
      match sell_prices with
      | [] -> None
      | prices -> Some (List.fold_left (fun acc p -> if abs_float p < abs_float acc then p else acc) (List.hd prices) prices)
    in

    let grid_interval_lo = data |?> "grid_interval_lo" |> to_float_d 1.0 in
    let grid_interval = if grid_interval_lo > 0.0 then grid_interval_lo else 1.0 in
    let close_thresh = 0.5 *. grid_interval in
    let far_thresh = 2.0 *. grid_interval in

    let render_gauge b_pct s_pct =
      let half = 7 in
      let pos d =
        if d < 0.0 then half
        else if d < 0.25 *. grid_interval then 0
        else if d < 0.75 *. grid_interval then 1
        else if d < 1.5 *. grid_interval  then 2
        else if d < 3.0 *. grid_interval  then 3
        else if d < 6.0 *. grid_interval  then 4
        else if d < 12.0 *. grid_interval then 5
        else 6
      in
      let b_pos = match b_pct with Some d -> pos (abs_float d) | None -> half + 1 in
      let s_pos = match s_pct with Some d -> pos (abs_float d) | None -> half + 1 in
      let left_side = List.init half (fun i ->
        if half - 1 - i = b_pos then "B" else "─"
      ) |> String.concat "" in
      let right_side = List.init half (fun i ->
        if i = s_pos then "S" else "─"
      ) |> String.concat "" in
      I.hcat [
        I.string a_border "  [";
        I.string (if b_pos <= 1 then a_green else a_dim) left_side;
        I.string a_border "┼";
        I.string (if s_pos <= 1 then a_yellow else a_dim) right_side;
        I.string a_border "]";
      ]
    in
    let gauge_img = render_gauge buy_dist_pct closest_sell_dist_pct in

    (* Status indicator *)
    let status_str, status_attr =
      if cap_low then "⏸", a_yellow
      else "▶", a_green
    in

    let exch_tag = exch_tag_of exchange in
    let spread_str = format_spread_bps bid ask in
    let spread_attr =
      if bid <= 0.0 || ask <= 0.0 then a_dim
      else
        let bps = ((ask -. bid) /. ((bid +. ask) /. 2.0)) *. 10000.0 in
        if bps < 5.0 then a_green
        else if bps < 20.0 then a_cyan
        else if bps < 50.0 then a_yellow
        else a_red
    in

    (* Format buy distance with proximity-based color *)
    let buy_dist_str, buy_dist_attr = match buy_dist_pct with
      | None -> "--", a_dim
      | Some d ->
          let abs_d = abs_float d in
          let attr = if abs_d < close_thresh then a_green
                     else if abs_d < far_thresh then a_cyan
                     else a_dim in
          format_pct d, attr
    in
    (* Format sell distance with proximity-based color *)
    let sell_dist_str, sell_dist_attr = match closest_sell_dist_pct with
      | None -> "--", a_dim
      | Some d ->
          let abs_d = abs_float d in
          let attr = if abs_d < close_thresh then a_yellow
                     else if abs_d < far_thresh then a_cyan
                     else a_dim in
          format_pct d, attr
    in

    (* execution proximity: 0 = buy order, 100 = sell order *)
    let near_buy, near_sell =
      let sell_prices = List.filter_map (fun s ->
        let sp = s |?> "price" |> to_float_d 0.0 in
        if sp > 0.0 then Some sp else None
      ) sell_orders in
      let execution_proximity =
        match sell_prices with
        | [] -> None
        | prices ->
            let sell_price = List.fold_left min (List.hd prices) prices in
            if (not cap_low) && buy_price > 0.0 && mid > 0.0 && sell_price > buy_price then
              let range = sell_price -. buy_price in
              Some (((mid -. buy_price) /. range) *. 100.0)
            else None
      in
      match execution_proximity with
      | Some pos -> (pos < 25.0, pos > 75.0)
      | None ->
          let is_near_buy = match buy_dist_pct with
            | Some d -> abs_float d < close_thresh
            | None -> false
          in
          (is_near_buy, false)
    in

    let row_text =
      if near_buy then a_near_fill
      else if near_sell then a_near_sell
      else a_text
    in
    let sym_attr =
      if near_buy then a_near_fill
      else if near_sell then a_near_sell
      else exch_sym_attr exchange
    in

    let border_attr =
      let bg_color = if near_buy then c_near_fill else if near_sell then c_near_sell else c_bg in
      A.(fg c_border ++ bg bg_color)
    in
    I.hcat [
      I.string border_attr " │  ";
      col 16 sym_attr (Printf.sprintf "%s(%s)" (truncate_string 10 symbol) exch_tag);
      col 5 a_cyan (truncate_string 4 stype);
      I.hcat [ I.string status_attr status_str; I.string a_text "  " ];
      col 12 row_text (if mid > 0.0 then format_price mid else "--");
      col 8 spread_attr spread_str;
      I.string a_border " │ ";
      col 12 row_text (if base_bal > 0.0 then format_qty base_bal else "0");
      col 10 row_text (if hold_value > 0.01 then format_price hold_value else "--");
      col 12 row_text (if accum_holding > 0.0001 then format_qty accum_holding else "0");
      col 10 row_text (if accum_hold_value > 0.01 then format_price accum_hold_value else "--");
      I.string a_border " │ ";
      col 12 (if near_buy then a_near_fill_green
              else if near_sell then a_near_sell_red
              else if buy_price > 0.0 then a_green else a_dim)
        (if buy_price > 0.0 then format_price buy_price else "--");
      col 8 buy_dist_attr buy_dist_str;
      I.string a_border " │ ";
      col 6 (if sell_count > 0 then a_yellow else a_dim)
        (string_of_int sell_count);
      col 8 sell_dist_attr sell_dist_str;
      col 12 (if unrealized_profit >= 0.0 then a_green else a_red)
        (format_pnl unrealized_profit);
      I.string a_border " │ ";
      gauge_img;
    ]
  ) strats in

  (* Partition strategies into active and paused groups *)
  let active_rows, paused_rows = List.partition (fun (_symbol, data) ->
    let strat = data |?> "strategy" in
    let cap_low = strat |?> "capital_low" |> to_bool_d false in
    not cap_low
  ) strats in
  let active_images = List.map (fun (sym, _) ->
    List.assoc sym (List.combine (List.map fst strats) strategy_rows)
  ) active_rows in
  let paused_images = List.map (fun (sym, _) ->
    List.assoc sym (List.combine (List.map fst strats) strategy_rows)
  ) paused_rows in

  (* Build rows for non-strategy balances, enriched with market and order data *)
  let non_strategy_rows = List.filter_map (fun bal_json ->
    let exchange = bal_json |?> "exchange" |> to_string_d "?" in
    let asset = bal_json |?> "asset" |> to_string_d "?" in
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in

    if balance <= 0.0 then None
    else begin
      let exch_tag = exch_tag_of exchange in

      (* Market data from enriched balance snapshot *)
      let bid = bal_json |?> "bid" |> to_float_d 0.0 in
      let ask = bal_json |?> "ask" |> to_float_d 0.0 in
      let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0
                else (max bid ask) in
      let hold_value = balance *. mid in

      (* Pending sell orders for this balance *)
      let sell_orders = bal_json |?> "sell_orders" |> to_list_d in
      let sell_count = bal_json |?> "sell_count" |> to_int_d 0 in

      (* Unrealized proceeds and accumulated quantity from pending sells *)
      let unrealized_profit, pending_sell_qty = List.fold_left (fun (up, qty_acc) s ->
        let sp = s |?> "price" |> to_float_d 0.0 in
        let sq = s |?> "qty" |> to_float_d 0.0 in
        if sp > 0.0 && sq > 0.0 then (up +. (sp *. sq), qty_acc +. sq)
        else (up, qty_acc)
      ) (0.0, 0.0) sell_orders in
      
      let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" || asset = "ZUSD" || asset = "USDe" in
      
      let accum_holding = if is_quote then 0.0 else max 0.0 (balance -. pending_sell_qty) in
      let accum_hold_value = accum_holding *. mid in

      (* Percentage distance to nearest sell order *)
      let closest_sell_dist_pct =
        let sell_prices = List.filter_map (fun s ->
          let sp = s |?> "price" |> to_float_d 0.0 in
          if sp > 0.0 && mid > 0.0 then Some (((sp -. mid) /. mid) *. 100.0)
          else None
        ) sell_orders in
        match sell_prices with
        | [] -> None
        | prices -> Some (List.fold_left (fun acc p -> if abs_float p < abs_float acc then p else acc) (List.hd prices) prices)
      in
      let sell_dist_str, sell_dist_attr = match closest_sell_dist_pct with
        | None -> "--", a_dim
        | Some d ->
            let abs_d = abs_float d in
            let attr = if abs_d < 0.5 then a_yellow
                       else if abs_d < 2.0 then a_cyan
                       else a_dim in
            format_pct d, attr
      in

      let render_gauge b_pct s_pct =
        let half = 7 in
        let pos d =
          if d < 0.0 then half
          else if d < 0.25 then 0
          else if d < 0.75 then 1
          else if d < 1.5  then 2
          else if d < 3.0  then 3
          else if d < 6.0  then 4
          else if d < 12.0 then 5
          else 6
        in
        let b_pos = match b_pct with Some d -> pos (abs_float d) | None -> half + 1 in
        let s_pos = match s_pct with Some d -> pos (abs_float d) | None -> half + 1 in
        let left_side = List.init half (fun i ->
          if half - 1 - i = b_pos then "B" else "─"
        ) |> String.concat "" in
        let right_side = List.init half (fun i ->
          if i = s_pos then "S" else "─"
        ) |> String.concat "" in
        I.hcat [
          I.string a_border "  [";
          I.string (if b_pos <= 1 then a_green else a_dim) left_side;
          I.string a_border "┼";
          I.string (if s_pos <= 1 then a_yellow else a_dim) right_side;
          I.string a_border "]";
        ]
      in
      let gauge_img = render_gauge None closest_sell_dist_pct in

      let status_str, status_attr =
        if is_quote then "$", a_green
        else "⏹", a_red
      in
      let spread_str = format_spread_bps bid ask in
      let img = I.hcat [
        I.string a_border " │  ";
        col 16 (exch_sym_attr ~dim:true exchange) (Printf.sprintf "%s(%s)" (truncate_string 10 asset) exch_tag);
        col 5 a_dim "--";
        I.hcat [ I.string status_attr status_str; I.string a_text "  " ];
        col 12 a_text (if mid > 0.0 then format_price mid else "--");
        col 8 a_dim spread_str;
        I.string a_border " │ ";
        col 12 a_text (format_qty balance);
        col 10 a_text (if hold_value > 0.01 then format_price hold_value else "--");
        col 12 a_text (if accum_holding > 0.0001 then format_qty accum_holding else "0");
        col 10 a_text (if accum_hold_value > 0.01 then format_price accum_hold_value else "--");
        I.string a_border " │ ";
        col 12 a_dim "--";
        col 8 a_dim "--";
        I.string a_border " │ ";
        col 6 (if sell_count > 0 then a_yellow else a_dim)
          (string_of_int sell_count);
        col 8 sell_dist_attr sell_dist_str;
        col 12 (if unrealized_profit >= 0.0 && sell_count > 0 then a_green
                else if unrealized_profit > 0.0 then a_dim else a_dim)
          (if sell_count > 0 then format_pnl unrealized_profit else "--");
        I.string a_border " │ ";
        gauge_img;
      ] in
      Some (is_quote, img)
    end
  ) all_balances in

  (* Separate non-strategy rows into inactive assets and quote currencies *)
  let inactive_rows = List.filter_map (fun (is_q, img) -> if not is_q then Some img else None) non_strategy_rows in
  let quote_rows = List.filter_map (fun (is_q, img) -> if is_q then Some img else None) non_strategy_rows in

  (* Aggregate unrealized profit, hold value, and accum value across strategies *)
  let total_up_strats, total_hold_strats, total_accum_val_strats = List.fold_left (fun (up_acc, hold_acc, accum_val_acc) (_symbol, data) ->
    let strat = data |?> "strategy" in
    let market = data |?> "market" in
    
    (* Compute holding value from mid-price *)
    let bid = market |?> "bid" |> to_float_d 0.0 in
    let ask = market |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
    let base_bal = market |?> "base_balance" |> to_float_d 0.0 in
    
    (* Compute unrealized proceeds from pending sell orders *)
    let sell_orders = strat |?> "sell_orders" |> to_list_d in
    let strat_up, pending_sell_qty = List.fold_left (fun (a, q_acc) s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then (a +. (sp *. sq), q_acc +. sq) else (a, q_acc)
    ) (0.0, 0.0) sell_orders in
    
    let accum_holding = max 0.0 (base_bal -. pending_sell_qty) in
    let accum_hold_value = accum_holding *. mid in
    
    (up_acc +. strat_up, hold_acc +. (base_bal *. mid), accum_val_acc +. accum_hold_value)
  ) (0.0, 0.0, 0.0) strats in

  (* Aggregate unrealized profit, hold value, and accum value for non-strategy balances *)
  let total_up_bals, total_hold_bals, total_accum_val_bals = List.fold_left (fun (up_acc, hold_acc, accum_val_acc) bal_json ->
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in
    let asset = bal_json |?> "asset" |> to_string_d "?" in
    if balance <= 0.0 then (up_acc, hold_acc, accum_val_acc) else
    let bid = bal_json |?> "bid" |> to_float_d 0.0 in
    let ask = bal_json |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0 else (max bid ask) in
    
    let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" || asset = "ZUSD" || asset = "USDe" in
    
    let sell_orders = bal_json |?> "sell_orders" |> to_list_d in
    let bal_up, pending_sell_qty = List.fold_left (fun (a, q_acc) s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then (a +. (sp *. sq), q_acc +. sq) else (a, q_acc)
    ) (0.0, 0.0) sell_orders in
    
    let accum_holding = if is_quote then 0.0 else max 0.0 (balance -. pending_sell_qty) in
    let accum_hold_value = accum_holding *. mid in
    
    (up_acc +. bal_up, hold_acc +. (balance *. mid), accum_val_acc +. accum_hold_value)
  ) (0.0, 0.0, 0.0) all_balances in
  
  let total_up = total_up_strats +. total_up_bals in
  let total_hold_val = total_hold_strats +. total_hold_bals in
  let total_accum_val = total_accum_val_strats +. total_accum_val_bals in

  (* Sum all quote-currency balances (pegged to $1) *)
  let total_quote_val = List.fold_left (fun acc bal_json ->
    let asset   = bal_json |?> "asset"   |> to_string_d "" in
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in
    let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT"
                || asset = "ZUSD" || asset = "USDe" in
    if is_quote && balance > 0.0 then acc +. balance else acc
  ) 0.0 all_balances in

  (* Section header *)
  let title = section_title w "HOLDINGS & STRATEGY" in

  (* Visual separator between strategy rows and non-strategy rows *)
  let thin_sep label =
    let lbl = "  ── " ^ label ^ " " in
    let pad_count = max 0 (w - String.length lbl) in
    let pad_buf = Buffer.create (pad_count * 3) in
    for _ = 1 to pad_count do Buffer.add_string pad_buf "─" done;
    I.hcat [
      I.string A.(fg c_border ++ bg c_bg) lbl;
      I.string A.(fg c_border ++ bg c_bg) (Buffer.contents pad_buf);
    ]
  in
  let has_inactive = inactive_rows <> [] in
  let has_quote = quote_rows <> [] in

  (* Compose table: active, paused, separator, inactive, separator, quote rows *)
  let rows = [title; header]
    @ active_images
    @ paused_images
    @ (if has_inactive then [thin_sep "balances"] @ inactive_rows else [])
    @ (if has_quote then [thin_sep "cash"] @ quote_rows else [])
  in
  let main_table = I.vcat rows in

  (* Summary footer bar with aggregated portfolio metrics *)
  let up_attr = if total_up >= 0.0 then A.(fg c_green ++ bg c_bg ++ st bold)
                else A.(fg c_red   ++ bg c_bg ++ st bold) in
  let pipe = I.string A.(fg c_border ++ bg c_bg) "  │  " in
  let kv lbl value_s vattr =
    I.hcat [
      I.string A.(fg c_label ++ bg c_bg) ("  " ^ lbl ^ ": ");
      I.string vattr value_s;
    ]
  in
  let summary_bar = I.hcat [
    kv "Cash"      (format_price total_quote_val) A.(fg c_cyan   ++ bg c_bg ++ st bold);
    pipe;
    kv "Accum Val" (format_price total_accum_val) A.(fg c_bright ++ bg c_bg ++ st bold);
    pipe;
    kv "Hold Val"  (format_price total_hold_val)  A.(fg c_bright ++ bg c_bg ++ st bold);
    pipe;
    kv "Sell Val"  (format_pnl   total_up)        up_attr;
  ] in
  let summary_section = I.vcat (List.filter (fun img -> I.height img > 0) [
    I.string A.(fg c_title ++ bg c_section_bg ++ st bold) (pad_right w "  SUMMARY");
    summary_bar;
  ]) in

  I.vcat [ main_table; summary_section ]

(* Panel: Latency Profiling — compact one-row-per-domain *)

let render_latencies _w json =
  let lats = match json |?> "latencies" with `Assoc l -> l | _ -> [] in
  (* Build symbol -> exchange lookup from strategies *)
  let sym_to_exch = match json |?> "strategies" with
    | `Assoc l -> List.map (fun (sym, data) ->
        (sym, data |?> "exchange" |> to_string_d "")
      ) l
    | _ -> []
  in
  let exch_of_symbol sym =
    match List.assoc_opt sym sym_to_exch with
    | Some e when e <> "" -> e
    | _ -> ""
  in
  (* Filter to domains that have at least one metric with samples *)
  let active_lats = List.filter (fun (_symbol, metrics) ->
    let mlist = match metrics with `Assoc l -> l | _ -> [] in
    List.exists (fun (_label, data) ->
      data |?> "samples" |> to_int_d 0 > 0
    ) mlist
  ) lats in
  if active_lats = [] then I.empty
  else
    (* Per-metric latency thresholds: (yellow_us, red_us).
       These measure INTERNAL processing latency only — the code we control.
       Network round-trip to exchanges is excluded (not colocated).
       Thresholds are calibrated against achievable performance for each stage:
       - ticker/ob:  in-memory struct update from ring buffer; should be <5us
       - strategy:   grid logic + mutex + order push; target <25us p50
       - execution:  ringbuffer write + signal broadcast; short path
       - cycle:      full wakeup-to-sleep; sum of all stages *)
    let latency_thresholds label =
      match label with
      | "orderbook" -> (10.0,  30.0)
      | "strategy"  -> (30.0,  75.0)
      | "execution" -> (50.0,  150.0)
      | "cycle"     -> (50.0,  100.0)
      | _           -> (50.0,  100.0)
    in
    let severity label f samples =
      if samples = 0 then 3  (* dim *)
      else
        let (warn, crit) = latency_thresholds label in
        if f > crit then 2    (* red *)
        else if f > warn then 1  (* yellow *)
        else 0                 (* green *)
    in
    let attr_of_sev = function
      | 2 -> a_red | 1 -> a_yellow | 0 -> a_green | _ -> a_dim
    in
    (* Metric display order *)
    let metric_order = ["cycle"; "orderbook"; "strategy"; "execution"] in
    let metric_labels = ["CYCLE"; "OB"; "STRAT"; "EXEC"] in
    (* Two-row header: metric names on row 1, p50/p99 sub-headers on row 2 *)
    let header_row1 = I.hcat (
      [ I.string a_border " │  ";
        col 16 a_label "" ]
      @ List.mapi (fun i lbl ->
          let len = String.length lbl in
          let pad = (27 - len) / 2 in
          let s = String.make pad ' ' ^ lbl in
          let img = col 27 a_label s in
          if i = 0 then img else I.hcat [ I.string a_border " │ "; img ]
        ) metric_labels
    ) in
    let header_row2 = I.hcat (
      [ I.string a_border " │  ";
        col 16 a_label "DOMAIN" ]
      @ List.mapi (fun i _lbl ->
          let img = I.hcat [ col 9 a_dim "p50"; col 9 a_dim "p99"; col 9 a_dim "p999" ] in
          if i = 0 then img else I.hcat [ I.string a_border " │ "; img ]
        ) metric_labels
    ) in
    let header = I.vcat [header_row1; header_row2] in
    let rows = List.map (fun (symbol, metrics) ->
      let mlist = match metrics with `Assoc l -> l | _ -> [] in
      let find_metric label =
        match List.assoc_opt label mlist with
        | Some data ->
            let p50 = data |?> "p50" |> to_float_d 0.0 in
            let p99 = data |?> "p99" |> to_float_d 0.0 in
            let p999 = data |?> "p999" |> to_float_d 0.0 in
            let samples = data |?> "samples" |> to_int_d 0 in
            (p50, p99, p999, samples)
        | None -> (0.0, 0.0, 0.0, 0)
      in
      (* Compute worst severity across all metrics for the health dot *)
      let worst_sev = List.fold_left (fun worst label ->
        let (_, p99, _, samples) = find_metric label in
        if samples = 0 then worst
        else max worst (severity label p99 samples)
      ) 0 metric_order in
      let dot_attr = attr_of_sev worst_sev in
      let metric_cells = List.mapi (fun i label ->
        let (p50, p99, p999, samples) = find_metric label in
        let img = if samples = 0 then
          I.hcat [col 9 a_dim "--"; col 9 a_dim "--"; col 9 a_dim "--"]
        else
          let s50 = severity label p50 samples in
          let s99 = max s50 (severity label p99 samples) in
          let s999 = max s99 (severity label p999 samples) in
          I.hcat [
            col 9 (attr_of_sev s50) (format_latency_us p50);
            col 9 (attr_of_sev s99) (format_latency_us p99);
            col 9 (attr_of_sev s999) (format_latency_us p999);
          ]
        in
        if i = 0 then img else I.hcat [ I.string a_border " │ "; img ]
      ) metric_order in
      let exch = exch_of_symbol symbol in
      let sym_attr = if exch <> "" then exch_sym_attr exch else a_bright in
      I.hcat (
        [ I.string a_border " │  ";
          I.string dot_attr "●";
          I.string a_text " ";
          col 14 sym_attr (truncate_string 13 symbol) ]
        @ metric_cells
      )
    ) active_lats in
    I.vcat (header :: rows)

(* Panel: Memory and GC statistics *)

let render_memory w json =
  let mem = json |?> "memory" in
  let title = section_title w "MEMORY & GC" in
  let heap    = mem |?> "heap_mb"     |> to_int_d 0 in
  let live    = mem |?> "live_kb"     |> to_int_d 0 in
  let free    = mem |?> "free_kb"     |> to_int_d 0 in
  let major   = mem |?> "gc_major"    |> to_int_d 0 in
  let minor   = mem |?> "gc_minor"    |> to_int_d 0 in
  let compact = mem |?> "compactions" |> to_int_d 0 in
  let frags   = mem |?> "fragments"   |> to_int_d 0 in

  let total_kb = float_of_int (live + free) in
  let live_ratio = if total_kb > 0.0 then (float_of_int live) /. total_kb else 0.0 in
  let bar_len = 20 in
  let filled_len = int_of_float (live_ratio *. float_of_int bar_len) in
  let filled = String.make filled_len ' ' in
  let empty = String.make (max 0 (bar_len - filled_len)) ' ' in
  let bar = I.hcat [
    I.string A.(fg c_border ++ bg c_bg) "┣";
    I.string A.(bg c_accent) filled;
    I.string A.(bg c_border) empty;
    I.string A.(fg c_border ++ bg c_bg) "┫";
  ] in

  let kv lbl v =
    I.hcat [
      I.string a_dim ("  " ^ lbl ^ " ");
      I.string a_text (Printf.sprintf "%-8s" v);
    ]
  in
  let row1 = I.hcat [
    I.string a_border " │";
    kv "HEAP" (Printf.sprintf "%dMB" heap);
    kv "LIVE" (Printf.sprintf "%dKB" live);
    kv "FREE" (Printf.sprintf "%dKB" free);
    I.string a_dim "  PRESSURE "; bar;
  ] in
  let row2 = I.hcat [
    I.string a_border " │";
    kv "MAJOR" (string_of_int major);
    kv "MINOR" (string_of_int minor);
    kv "COMPACT" (string_of_int compact);
    kv "FRAGS" (string_of_int frags);
  ] in
  I.vcat [title; I.string a_text " "; row1; row2; I.string a_text " "]

(* Atomic frame rendering.
   Each render call opens a fresh pipe, writes the frame, closes the write
   end (producing EOF), drains the read end into a buffer, then performs a
   single write to stdout. This avoids EAGAIN races that caused partial
   frames with the previous shared-pipe approach. *)

let render_to_stdout (draw : out_channel -> unit) =
  let (pr, pw) = Unix.pipe () in
  let oc = Unix.out_channel_of_descr pw in
  draw oc;
  flush oc;
  Unix.close pw;                    (* EOF signals the drain loop to terminate *)
  let buf = Buffer.create 65536 in
  let tmp = Bytes.create 8192 in
  (try
    while true do
      let n = Unix.read pr tmp 0 8192 in
      if n = 0 then raise Exit;
      Buffer.add_subbytes buf tmp 0 n
    done
  with _ -> ());
  Unix.close pr;
  let frame = Buffer.contents buf in
  let len   = String.length frame in
  let rec go off rem =
    if rem > 0 then
      let n = Unix.write_substring Unix.stdout frame off rem in
      go (off + n) (rem - n)
  in
  go 0 len

(** Check if stdout is still connected to a live TTY.
    When SSH disconnects, the PTY dies and isatty returns false. *)
let stdout_alive () =
  try Unix.isatty Unix.stdout
  with Unix.Unix_error _ -> false

(** Exception raised by SIGALRM when a stdout write times out. *)
exception Render_timeout

(** Wrapper around [render_to_stdout] with an alarm-based timeout.
    Returns [true] if the render completed, [false] if it timed out
    (indicating the PTY/stdout is dead or blocked). *)
let render_to_stdout_safe ~timeout_s draw =
  let old_handler = Sys.signal Sys.sigalrm
    (Sys.Signal_handle (fun _ -> raise Render_timeout)) in
  let completed = ref false in
  (try
    ignore (Unix.alarm timeout_s);
    render_to_stdout draw;
    ignore (Unix.alarm 0);
    completed := true
  with
  | Render_timeout -> ignore (Unix.alarm 0)
  | exn -> ignore (Unix.alarm 0); raise exn);
  Sys.set_signal Sys.sigalrm old_handler;
  !completed

let render_wait_screen w h msg =
  let img = I.string A.(fg c_yellow ++ bg c_bg) msg
            |> I.hsnap ~align:`Left w
            |> I.vsnap ~align:`Top  h
  in
  render_to_stdout (fun oc ->
    output_string oc "\027[?2026h";
    output_string oc "\027[H";
    Notty_unix.output_image ~cap:Cap.ansi ~fd:oc img;
    output_string oc "\027[J";
    output_string oc "\027[?2026l")



(* Main loop *)

let () =
  (* Parse command-line arguments *)
  let speclist = [
    "--socket", Arg.Set_string socket_path, " Path to engine UDS (auto-discovers if not set)";
  ] in
  Arg.parse speclist (fun _ -> ()) "dio-dashboard [--socket /tmp/dio-<pid>.sock]";

  (* Manual terminal setup: bypass Notty Term to avoid its reader thread *)

  (* Save original termios for restoration on exit *)
  let saved_termios = Unix.tcgetattr Unix.stdin in

  (* Raw mode: disable echo, canonical input, and signal generation *)
  let raw_termios = { saved_termios with
    Unix.c_icanon = false;
    Unix.c_echo = false;
    Unix.c_isig = false;
    Unix.c_vmin = 0;
    Unix.c_vtime = 0;
  } in
  Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH raw_termios;

  (* Enter alternate screen buffer and hide cursor *)
  Printf.printf "\027[?1049h\027[?25l%!";

  (* Register at_exit handler to restore terminal state *)
  at_exit (fun () ->
    Printf.printf "\027[?25h\027[?1049l%!";  (* restore cursor, leave alt screen *)
    Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH saved_termios
  );

  let last_json = ref (`Assoc []) in
  let quit = ref false in
  let input_buf = Bytes.create 64 in

  (* SIGHUP handler: when the controlling terminal hangs up (SSH disconnect),
     the kernel sends SIGHUP to the process group.  Set quit so the event
     loop exits and goes through the disconnect path, sending 'Q' to the
     server.  Re-enable c_isig temporarily so the signal is delivered. *)
  Sys.set_signal Sys.sighup (Sys.Signal_handle (fun _ -> quit := true));

  (* Outer reconnect loop.
     On engine restart the socket path changes (new PID), so
     discover_socket is re-invoked on each reconnect attempt.
     The --socket override applies only to the first connection;
     subsequent reconnects always auto-discover the new PID. *)
  let fd_ref : Unix.file_descr option ref = ref None in

  let try_connect () =
    let candidates =
      if !socket_path <> "" && !fd_ref = None then
        (* First connection: use --socket override *)
        [!socket_path]
      else
        discover_socket_candidates ()
    in
    let rec try_candidates = function
      | [] -> None
      | p :: rest ->
          (try
            let fd = connect_and_watch p in
            fd_ref := Some fd;
            Some fd
          with Unix.Unix_error _ ->
            (* Stale socket — clean up and try the next candidate *)
            (try Unix.unlink p with _ -> ());
            try_candidates rest)
    in
    if List.length candidates > 1 then
      Printf.eprintf "Warning: multiple engine sockets found, trying newest first\n%!";
    try_candidates candidates
  in

  let disconnect fd =
    fd_ref := None;
    last_json := `Assoc [];
    (try let _ = Unix.write_substring fd "Q" 0 1 in () with _ -> ());
    (try Unix.close fd with _ -> ())
  in

  (* Display wait screen and poll for engine availability *)
  let rec wait_for_engine () =
    if !quit then ()
    else
      match try_connect () with
      | Some fd -> run_event_loop fd
      | None ->
          let (w, h) = match Notty_unix.winsize Unix.stdout with
            | Some (w, h) -> (w, h) | None -> (80, 24) in
          render_wait_screen w h "Waiting for engine...  (q to quit)";
          (* Poll stdin for quit input *)
          let ready, _, _ =
            try Unix.select [Unix.stdin] [] [] 2.0
            with Unix.Unix_error _ -> ([], [], [])
          in
          if List.mem Unix.stdin ready then begin
            let n = try Unix.read Unix.stdin input_buf 0 64 with _ -> 0 in
            if n = 0 then quit := true
            else begin
              for i = 0 to n - 1 do
                match Bytes.get input_buf i with
                | 'q' | 'Q' | '\027' -> quit := true
                | _ -> ()
              done
            end
          end;
          if not !quit then wait_for_engine ()

  and run_event_loop fd =
    (* Inner loop: read from socket and render until disconnect or quit *)
    let lost_connection = ref false in
    while not !quit && not !lost_connection do
      (* 1. Select on socket and stdin with 100ms timeout *)
      let ready, _, _ =
        try Unix.select [fd; Unix.stdin] [] [] 0.1
        with Unix.Unix_error _ -> ([], [], [])
      in

      (* 2. Process keyboard input *)
      if List.mem Unix.stdin ready then begin
        let n = try Unix.read Unix.stdin input_buf 0 64 with _ -> 0 in
        if n = 0 then quit := true
        else begin
          let rec check_bytes i =
            if i >= n then ()
            else begin
              (match Bytes.get input_buf i with
               | 'q' | 'Q' -> quit := true
               | '\027' ->
                   if i + 1 >= n then quit := true (* bare Escape key *)
               | _ -> ());
              check_bytes (i + 1)
            end
          in
          check_bytes 0
        end
      end;

      (* 3. Read and parse socket data if available *)
      if List.mem fd ready && not !quit then begin
        (try
          let msg = read_message fd in
          (try last_json := Yojson.Basic.from_string msg with _ -> ())
        with
        | End_of_file ->
            disconnect fd;
            lost_connection := true
        | Unix.Unix_error _ ->
            disconnect fd;
            lost_connection := true
        | _ ->
            (* Parse error: retain last known state *)
            ())
      end;

      (* 4. Check PTY liveness, render frame, then pong the server *)
      if not !quit && not !lost_connection then begin
        (* Fast-path: if stdout is no longer a TTY, the PTY is dead. *)
        if not (stdout_alive ()) then begin
          disconnect fd;
          quit := true
        end else begin
          let (w, h) = match Notty_unix.winsize Unix.stdout with
            | Some (w, h) -> (w, h)
            | None -> (80, 24)
          in
          let draw oc =
            output_string oc "\027[?2026h";
            output_string oc "\027[H";
            let img =
              let sep = I.string a_text " " in
              I.vcat [
                render_header w !last_json;
                sep;
                render_memory w !last_json;
                render_strategies w !last_json;
                sep;
                render_latencies w !last_json;
                sep;
                I.string A.(fg c_border ++ bg c_bg) " ╰──────────────────────────────────────────────────────────────────";
                I.string A.(fg c_dim ++ bg c_bg) (pad_right w "  q: quit  │  Diophant Solutions");
              ]
              |> I.hsnap ~align:`Left w
              |> I.vsnap ~align:`Top  h
            in
            Notty_unix.output_image ~cap:Cap.ansi ~fd:oc img;
            output_string oc "\027[J";
            output_string oc "\027[?2026l"
          in
          let rendered = render_to_stdout_safe ~timeout_s:2 draw in
          if not rendered then begin
            (* Stdout write timed out — PTY is dead. Clean up. *)
            disconnect fd;
            quit := true
          end else
            (* Heartbeat pong: confirms render succeeded and stdout is alive.
               If stdout is blocked (dead PTY), we never reach this, and the
               server prunes us after 3s without a pong. *)
            (try let _ = Unix.write_substring fd "P" 0 1 in () with _ -> ())
        end
      end
    done;
    (* On quit, ensure we disconnect cleanly so the server decrements active_clients. *)
    (match !fd_ref with Some fd -> disconnect fd | None -> ());
    (* On connection loss (not user quit), re-enter reconnect loop *)
    if not !quit then wait_for_engine ()
  in

  wait_for_engine ()

