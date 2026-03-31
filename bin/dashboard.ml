(** Dio Trading Engine — Terminal Dashboard
    
    Connects to the engine via Unix domain socket and renders
    a real-time TUI using Notty. Separate binary for crash isolation.
    
    Usage: ./dio-dashboard [--socket /tmp/dio-<pid>.sock]
*)

open Notty

(* ── UDS Client ──────────────────────────────────────────────────── *)

let socket_path = ref ""

(** Discover engine socket path by scanning /tmp/dio-*.sock *)
let discover_socket () =
  let entries = Sys.readdir "/tmp" in
  let socks = Array.to_list entries
    |> List.filter (fun f -> 
      String.length f > 4 && String.sub f 0 4 = "dio-" &&
      let len = String.length f in
      String.sub f (len - 5) 5 = ".sock")
    |> List.map (fun f -> "/tmp/" ^ f)
  in
  match socks with
  | [s] -> Some s
  | s :: _ -> Some s  (* Take first if multiple *)
  | [] -> None

(** Read exactly n bytes from fd *)
let read_exact fd buf off len =
  let rec loop off remaining =
    if remaining = 0 then ()
    else
      let n = Unix.read fd buf off remaining in
      if n = 0 then raise End_of_file;
      loop (off + n) (remaining - n)
  in
  loop off len

(** Read a length-prefixed JSON message from the socket *)
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

(** Connect to engine UDS and request watch mode *)
let connect_and_watch path =
  let fd = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  Unix.connect fd (Unix.ADDR_UNIX path);
  (* Send 'W' for watch mode *)
  let _ = Unix.write_substring fd "W" 0 1 in
  fd

(* ── JSON helpers ────────────────────────────────────────────────── *)

let ( |?> ) json key =
  match json with
  | `Assoc l -> (try List.assoc key l with Not_found -> `Null)
  | _ -> `Null

let to_string_d d = function `String s -> s | _ -> d
let to_float_d d = function `Float f -> f | `Int i -> float_of_int i | _ -> d
let to_int_d d = function `Int i -> i | `Float f -> int_of_float f | _ -> d
let to_bool_d d = function `Bool b -> b | _ -> d
let to_list_d = function `List l -> l | _ -> []

(* ── Formatting ──────────────────────────────────────────────────── *)

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

(* ── Color palette ───────────────────────────────────────────────── *)

let c_bg        = A.rgb_888 ~r:13  ~g:17  ~b:23
let c_panel     = A.rgb_888 ~r:22  ~g:27  ~b:34
let c_border    = A.rgb_888 ~r:48  ~g:54  ~b:61
let c_title     = A.rgb_888 ~r:88  ~g:166 ~b:255
let c_label     = A.rgb_888 ~r:125 ~g:133 ~b:144
let c_text      = A.rgb_888 ~r:201 ~g:209 ~b:217
let c_bright    = A.rgb_888 ~r:240 ~g:246 ~b:252
let c_green     = A.rgb_888 ~r:63  ~g:185 ~b:80
let c_red       = A.rgb_888 ~r:248 ~g:81  ~b:73
let c_yellow    = A.rgb_888 ~r:210 ~g:153 ~b:34
let c_cyan      = A.rgb_888 ~r:57  ~g:211 ~b:183
let c_dim       = A.rgb_888 ~r:72  ~g:79  ~b:88
let _c_orange   = A.rgb_888 ~r:210 ~g:105 ~b:30

(* ── Attribute constructors ──────────────────────────────────────── *)

let a_title     = A.(fg c_title  ++ bg c_bg   ++ st bold)
let a_label     = A.(fg c_label  ++ bg c_bg)
let a_text      = A.(fg c_text   ++ bg c_bg)
let a_bright    = A.(fg c_bright ++ bg c_bg   ++ st bold)
let a_green     = A.(fg c_green  ++ bg c_bg)
let a_red       = A.(fg c_red    ++ bg c_bg)
let a_yellow    = A.(fg c_yellow ++ bg c_bg)
let a_cyan      = A.(fg c_cyan   ++ bg c_bg)
let a_dim       = A.(fg c_dim    ++ bg c_bg)
let a_border    = A.(fg c_border ++ bg c_bg)
let a_header_bg = A.(fg c_bright ++ bg c_panel ++ st bold)
let _a_orange   = A.(fg _c_orange ++ bg c_bg)

(* ── Drawing primitives ──────────────────────────────────────────── *)

let hline w =
  (* Build a UTF-8 thin horizontal rule using U+2500 (─) *)
  let buf = Buffer.create (w * 3) in
  for _ = 1 to w do Buffer.add_string buf "─" done;
  I.string a_border (Buffer.contents buf)

let pad_right w s =
  let len = String.length s in
  if len >= w then String.sub s 0 w
  else s ^ String.make (w - len) ' '

let col w attr s = I.string attr (pad_right w s)

let spacer = I.string a_text " "

let format_pct f =
  if abs_float f < 0.01 then "<0.01%"
  else if abs_float f >= 10.0 then Printf.sprintf "%.1f%%" f
  else Printf.sprintf "%.2f%%" f

(* ── Panel: Header bar ───────────────────────────────────────────── *)

let render_header w json =
  let uptime = json |?> "uptime_s" |> to_float_d 0.0 in
  let fng = json |?> "fear_and_greed" |> to_float_d 0.0 in
  let mem = json |?> "memory" in
  let heap = mem |?> "heap_mb" |> to_int_d 0 in
  let live = mem |?> "live_kb" |> to_int_d 0 in
  let gc_major = mem |?> "gc_major" |> to_int_d 0 in
  let _fng_attr = if fng >= 60.0 then a_green
    else if fng >= 40.0 then a_yellow else a_red in
  I.hcat [
    I.string a_header_bg (pad_right 3 " ");
    I.string a_header_bg "DIO";
    I.string A.(fg c_dim ++ bg c_panel) " │ ";
    I.string A.(fg c_text ++ bg c_panel) (Printf.sprintf "UP %s" (format_duration uptime));
    I.string A.(fg c_dim ++ bg c_panel) " │ ";
    I.string A.(fg c_text ++ bg c_panel) (Printf.sprintf "HEAP %dMB" heap);
    I.string A.(fg c_dim ++ bg c_panel) " / ";
    I.string A.(fg c_text ++ bg c_panel) (Printf.sprintf "LIVE %dKB" live);
    I.string A.(fg c_dim ++ bg c_panel) " │ ";
    I.string A.(fg c_text ++ bg c_panel) (Printf.sprintf "GC %d" gc_major);
    I.string A.(fg c_dim ++ bg c_panel) " │ ";
    I.string A.(fg c_text ++ bg c_panel) "F&G ";
    I.string A.(fg (if fng >= 60.0 then c_green else if fng >= 40.0 then c_yellow else c_red) ++ bg c_panel ++ st bold) (Printf.sprintf "%.0f" fng);
    I.string A.(bg c_panel) (String.make (max 0 (w - 60)) ' ');
  ]

(* ── Panel: Connections ──────────────────────────────────────────── *)

let _render_connections w json =
  let conns = json |?> "connections" |> to_list_d in
  let title = I.hcat [
    I.string a_title " CONNECTIONS";
    I.string a_dim (String.make (max 0 (w - 13)) ' ');
  ] in
  let rows = List.map (fun c ->
    let name = c |?> "name" |> to_string_d "?" in
    let state = c |?> "state" |> to_string_d "?" in
    let uptime = c |?> "uptime_s" |> to_float_d 0.0 in
    let cb = c |?> "circuit_breaker" |> to_string_d "?" in
    let reconn = c |?> "reconnects" |> to_int_d 0 in
    let state_attr = match state with
      | "Connected" -> a_green | "Connecting" -> a_yellow | _ -> a_red in
    let indicator = match state with
      | "Connected" -> I.string a_green "●" 
      | "Connecting" -> I.string a_yellow "◌"
      | _ -> I.string a_red "✗" in
    let cb_attr = if cb = "Closed" then a_dim else a_red in
    I.hcat [
      I.string a_text " ";
      indicator; spacer;
      col 22 a_text (truncate_string 22 name); spacer;
      col 12 state_attr state; spacer;
      col 7 a_label (format_duration uptime); spacer;
      I.string a_label "rc:";
      col 3 (if reconn > 0 then a_yellow else a_dim) (string_of_int reconn); spacer;
      I.string a_label "cb:";
      col (max 0 (w - 58)) cb_attr cb;
    ]
  ) conns in
  I.vcat (title :: rows)

(* ── Panel: Holdings & Strategy ──────────────────────────────────── *)

let render_strategies _w json =
  let strats = match json |?> "strategies" with `Assoc l -> l | _ -> [] in
  let all_balances = json |?> "all_balances" |> to_list_d in
  (* Header row *)
  let header = I.hcat [
    I.string a_text " ";
    col 14 a_label "SYMBOL";
    col 5 a_label "STGY";
    col 3 a_label "ST";
    col 12 a_label "PRICE";
    col 12 a_label "HOLDING";
    col 10 a_label "HOLD VAL";
    col 12 a_label "BUY @";
    col 8 a_label "Δ BUY";
    col 6 a_label "SELLS";
    col 8 a_label "Δ SELL";
    col 12 a_label "uP";
  ] in

  (* Render strategy rows *)
  let strategy_rows = List.map (fun (symbol, data) ->
    let exchange = data |?> "exchange" |> to_string_d "?" in
    let strat = data |?> "strategy" in
    let market = data |?> "market" in
    let stype = strat |?> "type" |> to_string_d "?" in
    let cap_low = strat |?> "capital_low" |> to_bool_d false in
    let asset_low = strat |?> "asset_low" |> to_bool_d false in

    (* Market data *)
    let bid = market |?> "bid" |> to_float_d 0.0 in
    let ask = market |?> "ask" |> to_float_d 0.0 in
    let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0
              else (max bid ask) in
    let base_bal = market |?> "base_balance" |> to_float_d 0.0 in
    let hold_value = base_bal *. mid in

    (* Strategy data *)
    let buy_price = strat |?> "buy_price" |> to_float_d 0.0 in
    let sell_count = strat |?> "sell_count" |> to_int_d 0 in
    let _grid_qty = strat |?> "grid_qty" |> to_float_d 0.0 in

    (* Unrealized profit: sum of mark proceeds for all pending sells *)
    let sell_orders = strat |?> "sell_orders" |> to_list_d in
    let unrealized_profit = List.fold_left (fun acc s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then acc +. (sp *. sq)
      else acc
    ) 0.0 sell_orders in

    (* Distance to pending buy order — only when strategy is active (not paused) *)
    let buy_dist_pct =
      if (not cap_low) && buy_price > 0.0 && mid > 0.0 then
        Some (((buy_price -. mid) /. mid) *. 100.0)
      else None
    in

    (* Distance to closest pending sell order *)
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

    (* Status indicator: ▶ active, ⏸ paused *)
    let status_str, status_attr =
      if cap_low || asset_low then "⏸", a_yellow
      else "▶", a_green
    in

    let exch_tag = match exchange with
      | "kraken" -> "kr" | "hyperliquid" -> "hl" | e -> String.sub e 0 (min 2 (String.length e))
    in

    (* Buy distance formatting *)
    let buy_dist_str, buy_dist_attr = match buy_dist_pct with
      | None -> "--", a_dim
      | Some d ->
          let abs_d = abs_float d in
          let attr = if abs_d < 0.5 then a_green
                     else if abs_d < 2.0 then a_cyan
                     else a_dim in
          format_pct d, attr
    in
    (* Sell distance formatting *)
    let sell_dist_str, sell_dist_attr = match closest_sell_dist_pct with
      | None -> "--", a_dim
      | Some d ->
          let abs_d = abs_float d in
          let attr = if abs_d < 0.5 then a_yellow
                     else if abs_d < 2.0 then a_cyan
                     else a_dim in
          format_pct d, attr
    in

    I.hcat [
      I.string a_text " ";
      col 14 a_bright (Printf.sprintf "%s(%s)" (truncate_string 10 symbol) exch_tag);
      col 5 a_cyan (truncate_string 4 stype);
      col 3 status_attr status_str;
      col 12 a_text (if mid > 0.0 then format_price mid else "--");
      col 12 a_text (if base_bal > 0.0 then format_qty base_bal else "0");
      col 10 a_text (if hold_value > 0.01 then format_price hold_value else "--");
      col 12 (if buy_price > 0.0 then a_green else a_dim)
        (if buy_price > 0.0 then format_price buy_price else "--");
      col 8 buy_dist_attr buy_dist_str;
      col 6 (if sell_count > 0 then a_yellow else a_dim)
        (string_of_int sell_count);
      col 8 sell_dist_attr sell_dist_str;
      col 12 (if unrealized_profit >= 0.0 then a_green else a_red)
        (format_pnl unrealized_profit);
    ]
  ) strats in

  (* Split strategy rows into active vs paused *)
  let active_rows, paused_rows = List.partition (fun (_symbol, data) ->
    let strat = data |?> "strategy" in
    let cap_low = strat |?> "capital_low" |> to_bool_d false in
    let asset_low = strat |?> "asset_low" |> to_bool_d false in
    not (cap_low || asset_low)
  ) strats in
  let active_images = List.map (fun (sym, _) ->
    List.assoc sym (List.combine (List.map fst strats) strategy_rows)
  ) active_rows in
  let paused_images = List.map (fun (sym, _) ->
    List.assoc sym (List.combine (List.map fst strats) strategy_rows)
  ) paused_rows in

  (* Render non-strategy balance rows from all_balances — now enriched with market + order data *)
  let non_strategy_rows = List.filter_map (fun bal_json ->
    let exchange = bal_json |?> "exchange" |> to_string_d "?" in
    let asset = bal_json |?> "asset" |> to_string_d "?" in
    let balance = bal_json |?> "balance" |> to_float_d 0.0 in

    if balance <= 0.0 then None
    else begin
      let exch_tag = match exchange with
        | "kraken" -> "kr" | "hyperliquid" -> "hl" | e -> String.sub e 0 (min 2 (String.length e))
      in

      (* Market data from enriched snapshot *)
      let bid = bal_json |?> "bid" |> to_float_d 0.0 in
      let ask = bal_json |?> "ask" |> to_float_d 0.0 in
      let mid = if bid > 0.0 && ask > 0.0 then (bid +. ask) /. 2.0
                else (max bid ask) in
      let hold_value = balance *. mid in

      (* Open sell orders *)
      let sell_orders = bal_json |?> "sell_orders" |> to_list_d in
      let sell_count = bal_json |?> "sell_count" |> to_int_d 0 in

      (* Unrealized profit from pending sells *)
      let unrealized_profit = List.fold_left (fun acc s ->
        let sp = s |?> "price" |> to_float_d 0.0 in
        let sq = s |?> "qty" |> to_float_d 0.0 in
        if sp > 0.0 && sq > 0.0 then acc +. (sp *. sq)
        else acc
      ) 0.0 sell_orders in

      (* Distance to closest sell order *)
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

      let is_quote = asset = "USD" || asset = "USDC" || asset = "USDT" in
      let status_str, status_attr =
        if is_quote then "$", a_green
        else "⏹", a_red
      in
      let img = I.hcat [
        I.string a_text " ";
        col 14 a_dim (Printf.sprintf "%s(%s)" (truncate_string 10 asset) exch_tag);
        col 5 a_dim "--";
        col 3 status_attr status_str;
        col 12 a_text (if mid > 0.0 then format_price mid else "--");
        col 12 a_text (format_qty balance);
        col 10 a_text (if hold_value > 0.01 then format_price hold_value else "--");
        col 12 a_dim "--";
        col 8 a_dim "--";
        col 6 (if sell_count > 0 then a_yellow else a_dim)
          (string_of_int sell_count);
        col 8 sell_dist_attr sell_dist_str;
        col 12 (if unrealized_profit >= 0.0 && sell_count > 0 then a_green
                else if unrealized_profit > 0.0 then a_dim else a_dim)
          (if sell_count > 0 then format_pnl unrealized_profit else "--");
      ] in
      Some (is_quote, img)
    end
  ) all_balances in

  (* Split non-strategy rows: inactive assets vs quote currencies *)
  let inactive_rows = List.filter_map (fun (is_q, img) -> if not is_q then Some img else None) non_strategy_rows in
  let quote_rows = List.filter_map (fun (is_q, img) -> if is_q then Some img else None) non_strategy_rows in

  (* Compute total unrealized profit across all rows *)
  let total_up = List.fold_left (fun acc (_symbol, data) ->
    let strat = data |?> "strategy" in
    let sell_orders = strat |?> "sell_orders" |> to_list_d in
    acc +. List.fold_left (fun a s ->
      let sp = s |?> "price" |> to_float_d 0.0 in
      let sq = s |?> "qty" |> to_float_d 0.0 in
      if sp > 0.0 && sq > 0.0 then a +. (sp *. sq) else a
    ) 0.0 sell_orders
  ) 0.0 strats in
  (* Add non-strategy sell order unrealized profit *)
  let total_up = List.fold_left (fun acc (_, img_pair) ->
    ignore img_pair; acc
  ) total_up non_strategy_rows in
  ignore total_up;

  (* Section title *)
  let title = I.hcat [
    I.string a_title " HOLDINGS & STRATEGY";
    I.string a_dim (String.make (max 0 (103 - 22)) ' ');
  ] in

  (* Footer row with total unrealized profit, right-aligned to table edge *)
  let up_label = Printf.sprintf " Σ uP: %s " (format_pnl total_up) in
  let up_attr = if total_up >= 0.0 then A.(fg c_green ++ bg c_panel ++ st bold)
                else A.(fg c_red ++ bg c_panel ++ st bold) in
  (* Table columns: 1+14+5+3+12+12+10+12+8+6+8+12 = 103 *)
  let table_width = 103 in
  let gap = max 1 (table_width - String.length up_label) in
  let footer = I.hcat [
    I.string a_dim (String.make gap ' ');
    I.string up_attr up_label;
  ] in

  (* Assemble: active → paused → inactive → quotes → footer *)
  I.vcat (title :: header :: active_images @ paused_images @ inactive_rows @ quote_rows @ [footer])

(* ── Panel: Latencies ────────────────────────────────────────────── *)

let render_latencies w json =
  let lats = match json |?> "latencies" with `Assoc l -> l | _ -> [] in
  let title = I.hcat [
    I.string a_title " LATENCY PROFILING";
    I.string a_dim (String.make (max 0 (w - 25)) ' ');
  ] in
  (* Adaptive metric column: use more space when terminal is wide enough *)
  let metric_w = if w >= 100 then 14 else 8 in
  let header = I.hcat [
    I.string a_text " ";
    col 14 a_label "DOMAIN";
    col metric_w a_label "METRIC";
    col 10 a_label "p50";
    col 10 a_label "p90";
    col 10 a_label "p99";
    col 10 a_label "p999";
    col 10 a_label "SAMPLES";
  ] in
  let rows = List.concat_map (fun (symbol, metrics) ->
    let mlist = match metrics with `Assoc l -> l | _ -> [] in
    List.filter_map (fun (label, data) ->
      let p50 = data |?> "p50" |> to_float_d 0.0 in
      let p90 = data |?> "p90" |> to_float_d 0.0 in
      let p99 = data |?> "p99" |> to_float_d 0.0 in
      let p999 = data |?> "p999" |> to_float_d 0.0 in
      let samples = data |?> "samples" |> to_int_d 0 in
      if samples = 0 then None
      else
        let p99_attr = if p99 > 1000.0 then a_yellow
          else if p99 > 5000.0 then a_red else a_text in
        Some (I.hcat [
          I.string a_text " ";
          col 14 a_bright (truncate_string 13 symbol);
          col metric_w a_cyan (truncate_string (metric_w - 1) label);
          col 10 a_text (format_latency_us p50);
          col 10 a_text (format_latency_us p90);
          col 10 p99_attr (format_latency_us p99);
          col 10 (if p999 > 5000.0 then a_red else a_text) (format_latency_us p999);
          col 10 a_dim (string_of_int samples);
        ])
    ) mlist
  ) lats in
  if rows = [] then
    I.vcat [title; header; I.string a_dim "  -- no latency data --"]
  else
    I.vcat (title :: header :: rows)

(* ── Panel: Memory / GC ─────────────────────────────────────────── *)

let render_memory w json =
  let mem = json |?> "memory" in
  let title = I.hcat [
    I.string a_title " MEMORY & GC";
    I.string a_dim (String.make (max 0 (w - 14)) ' ');
  ] in
  let heap = mem |?> "heap_mb" |> to_int_d 0 in
  let live = mem |?> "live_kb" |> to_int_d 0 in
  let free = mem |?> "free_kb" |> to_int_d 0 in
  let major = mem |?> "gc_major" |> to_int_d 0 in
  let minor = mem |?> "gc_minor" |> to_int_d 0 in
  let compact = mem |?> "compactions" |> to_int_d 0 in
  let frags = mem |?> "fragments" |> to_int_d 0 in
  let chunks = mem |?> "heap_chunks" |> to_int_d 0 in
  let row = I.hcat [
    I.string a_text " ";
    I.string a_label "heap "; I.string a_text (Printf.sprintf "%dMB" heap); spacer; spacer;
    I.string a_label "live "; I.string a_text (Printf.sprintf "%dKB" live); spacer; spacer;
    I.string a_label "free "; I.string a_text (Printf.sprintf "%dKB" free); spacer; spacer;
    I.string a_label "major "; I.string a_text (string_of_int major); spacer; spacer;
    I.string a_label "minor "; I.string a_text (string_of_int minor); spacer; spacer;
    I.string a_label "compact "; I.string a_text (string_of_int compact); spacer; spacer;
    I.string a_label "frag "; I.string a_text (string_of_int frags); spacer; spacer;
    I.string a_label "chunks "; I.string a_text (string_of_int chunks);
    I.string a_dim (String.make (max 0 (w - 100)) ' ');
  ] in
  I.vcat [title; row]

(* ── Panel: Domains ──────────────────────────────────────────────── *)

let render_domains w json =
  let doms = json |?> "domains" |> to_list_d in
  let title = I.hcat [
    I.string a_title " DOMAINS";
    I.string a_dim (String.make (max 0 (w - 10)) ' ');
  ] in
  let rows = List.map (fun d ->
    let key = d |?> "key" |> to_string_d "?" in
    let running = d |?> "running" |> to_bool_d false in
    let restarts = d |?> "restart_count" |> to_int_d 0 in
    I.hcat [
      I.string a_text " ";
      I.string (if running then a_green else a_red) (if running then ">" else "x"); spacer;
      col 28 a_text (truncate_string 27 key); spacer;
      I.string a_label "restarts:";
      col (max 0 (w - 44)) (if restarts > 0 then a_yellow else a_dim) (string_of_int restarts);
    ]
  ) doms in
  I.vcat (title :: rows)

(* ── Full render ─────────────────────────────────────────────────── *)

(* ── Pipe-buffered rendering for atomic frame writes ─────────────── *)

let render_pipe_r, render_pipe_w = Unix.pipe ()
let render_oc = Unix.out_channel_of_descr render_pipe_w
let () = Unix.set_nonblock render_pipe_r

(** Render the full dashboard image, serialize to ANSI via a pipe,
    and return the entire frame as a string for a single write() call. *)
let render_frame w h json =
  let img =
    let sep = hline w in
    I.vcat [
      render_header w json;
      sep;
      render_memory w json;
      sep;
      render_strategies w json;
      sep;
      render_latencies w json;
      sep;
      render_domains w json;
      sep;
      I.hcat [
        I.string A.(fg c_dim ++ bg c_panel) (pad_right w "  q: quit  │  refreshes every 500ms");
      ];
    ]
  in
  (* Render image to pipe via Notty, wrapped in synchronized output markers *)
  output_string render_oc "\027[?2026h";  (* begin synchronized update *)
  output_string render_oc "\027[H";      (* cursor home *)
  Notty_unix.output_image ~cap:Cap.ansi ~fd:render_oc (I.vsnap h img);
  output_string render_oc "\027[J";      (* clear to end of screen *)
  output_string render_oc "\027[?2026l";  (* end synchronized update *)
  flush render_oc;
  (* Read entire rendered frame from pipe *)
  let buf = Buffer.create 16384 in
  let tmp = Bytes.create 8192 in
  (try
    while true do
      let n = Unix.read render_pipe_r tmp 0 8192 in
      if n = 0 then raise Exit;
      Buffer.add_subbytes buf tmp 0 n
    done
  with _ -> ());
  Buffer.contents buf

(* ── Main loop ───────────────────────────────────────────────────── *)

let () =
  (* Parse args *)
  let speclist = [
    "--socket", Arg.Set_string socket_path, " Path to engine UDS (auto-discovers if not set)";
  ] in
  Arg.parse speclist (fun _ -> ()) "dio-dashboard [--socket /tmp/dio-<pid>.sock]";

  (* Discover or use provided socket *)
  let path = if !socket_path <> "" then !socket_path
    else match discover_socket () with
      | Some p -> p
      | None ->
          Printf.eprintf "No engine socket found in /tmp/dio-*.sock\n";
          Printf.eprintf "Is the engine running?\n";
          exit 1
  in

  Printf.eprintf "Connecting to %s...\n%!" path;

  let fd = try connect_and_watch path with
    | Unix.Unix_error (e, _, _) ->
        Printf.eprintf "Failed to connect: %s\n" (Unix.error_message e);
        exit 1
  in

  Printf.eprintf "Connected. Launching dashboard...\n%!";

  (* ── Manual terminal setup (bypass Notty Term to avoid reader thread) ── *)

  (* Save original terminal attributes for cleanup *)
  let saved_termios = Unix.tcgetattr Unix.stdin in

  (* Set stdin to raw mode: no echo, no canonical, no signals, immediate read *)
  let raw_termios = { saved_termios with
    Unix.c_icanon = false;
    Unix.c_echo = false;
    Unix.c_isig = false;
    Unix.c_vmin = 0;
    Unix.c_vtime = 0;
  } in
  Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH raw_termios;

  (* Enter alternate screen buffer + hide cursor via ANSI escapes *)
  Printf.printf "\027[?1049h\027[?25l%!";

  (* Register cleanup on exit (covers normal exit, signals, exceptions) *)
  at_exit (fun () ->
    Printf.printf "\027[?25h\027[?1049l%!";  (* show cursor, leave alt screen *)
    Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH saved_termios
  );

  let last_json = ref (`Assoc []) in
  let quit = ref false in
  let input_buf = Bytes.create 64 in

  while not !quit do
    (* 1. Wait for socket data or keyboard input, 100ms timeout *)
    let ready, _, _ =
      try Unix.select [fd; Unix.stdin] [] [] 0.1
      with Unix.Unix_error _ -> ([], [], [])
    in

    (* 2. Check for keyboard input *)
    if List.mem Unix.stdin ready then begin
      let n = try Unix.read Unix.stdin input_buf 0 64 with _ -> 0 in
      let rec check_bytes i =
        if i >= n then ()
        else begin
          (match Bytes.get input_buf i with
           | 'q' | 'Q' -> quit := true
           | '\027' ->  (* Escape — could be bare Esc or start of sequence *)
               if i + 1 >= n then quit := true  (* bare Esc *)
               (* else it's an escape sequence, skip *)
           | _ -> ());
          check_bytes (i + 1)
        end
      in
      check_bytes 0
    end;

    (* 3. Read socket data if available *)
    if List.mem fd ready && not !quit then begin
      (try
        let msg = read_message fd in
        (try last_json := Yojson.Basic.from_string msg with _ -> ())
      with
      | End_of_file -> quit := true
      | _ -> ())
    end;

    (* 4. Render: buffer entire frame, write atomically *)
    if not !quit then begin
      let (w, h) = match Notty_unix.winsize Unix.stdout with
        | Some (w, h) -> (w, h)
        | None -> (80, 24)
      in
      let frame = render_frame w h !last_json in
      (* Write entire frame — loop to handle partial writes over SSH/PTY *)
      let len = String.length frame in
      let rec write_all off remaining =
        if remaining > 0 then
          let n = Unix.write_substring Unix.stdout frame off remaining in
          write_all (off + n) (remaining - n)
      in
      write_all 0 len
    end
  done;

  (* Cleanup: send quit to engine, close socket *)
  (try
    let _ = Unix.write_substring fd "Q" 0 1 in
    Unix.close fd
  with _ -> ())

