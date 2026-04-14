open Notty
open Theme

(** Recent Fills Feed Component.
    Scrolls recent filled orders horizontally across the screen below the holdings.
*)


let local_fills : Yojson.Basic.t list ref = ref []
let capacity = 10
let initialized = ref false

let render_fills w json =

  let engine_fills = json |?> "recent_fills" |> to_list_d in
  (* Merge engine fills into local_fills on every render cycle.
     On startup, the engine seeds historical fills into the ring buffer
     so engine_fills is non-empty from the first snapshot. New live fills
     are detected by timestamp comparison and prepended. *)
  let () =
    if not !initialized then begin
      if engine_fills <> [] then begin
        local_fills := [ List.hd engine_fills ];
        initialized := true
      end
    end else begin
      let latest_ts =
        match !local_fills with
        | [] -> 0.0
        | f :: _ -> f |?> "timestamp" |> to_float_d 0.0
      in
      let new_fills =
        List.filter (fun f ->
          let ts = f |?> "timestamp" |> to_float_d 0.0 in
          ts > latest_ts
        ) engine_fills
      in
      if new_fills <> [] then begin
        let combined = new_fills @ !local_fills in
        let rec take n l acc =
          if n <= 0 then List.rev acc
          else match l with
          | [] -> List.rev acc
          | h :: t -> take (n - 1) t (h :: acc)
        in
        local_fills := take capacity combined []
      end
    end
  in
  let fills = !local_fills in
  if fills = [] then I.empty
  else
      (* Build the ticker string chunks grouped by fill *)
    let chunks = List.map (fun bal_json ->
      let venue = bal_json |?> "venue" |> to_string_d "?" in
      let symbol = bal_json |?> "symbol" |> to_string_d "?" in
      let side = bal_json |?> "side" |> to_string_d "?" |> String.uppercase_ascii in
      let amount = bal_json |?> "amount" |> to_float_d 0.0 in
      let price = bal_json |?> "fill_price" |> to_float_d 0.0 in
      let timestamp = bal_json |?> "timestamp" |> to_float_d 0.0 in

      (* Format time difference *)
      let now = Unix.gettimeofday () in
      let diff = max 0.0 (now -. timestamp) in
      let time_str = 
        if diff < 60.0 then Printf.sprintf "%.0fs" diff
        else if diff < 3600.0 then Printf.sprintf "%.0fm" (diff /. 60.0)
        else Printf.sprintf "%.1fh" (diff /. 3600.0)
      in

      let side_attr = if side = "BUY" then A.(fg c_green ++ st bold) else A.(fg c_red ++ st bold) in
      let sym_attr = exch_sym_attr (String.lowercase_ascii venue) in

      let amount_str = if amount < 1.0 then Printf.sprintf "%.4g" amount else Printf.sprintf "%.2f" amount in

      I.hcat [
        I.string A.(fg c_dim) (time_str ^ " ago ");
        I.string sym_attr symbol;
        I.string A.(fg c_dim) " ";
        I.string side_attr side;
        I.string A.(fg c_text) (" " ^ amount_str ^ " @ " ^ format_price price);
      ]
    ) fills in

    let order_separator = I.string A.(fg c_dim ++ bg c_bg) "  •  " in
    let feed_start = I.string A.(fg c_accent ++ bg c_bg) "  ◈ RECENT FILLS ◈  " in
    let max_w = w - 2 in
    
    let final_img =
      List.fold_left (fun acc chunk ->
        let candidate = if I.width acc = I.width feed_start then I.hcat [acc; chunk] else I.hcat [acc; order_separator; chunk] in
        if I.width candidate > max_w then acc else candidate
      ) feed_start chunks
    in

    let padded = I.hsnap ~align:`Left w final_img in
    I.(padded </> I.string A.(bg c_bg) (String.make w ' '))
