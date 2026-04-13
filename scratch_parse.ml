type level = { price: float; size: float }

let get_coin msg =
  let coin_key = "\"coin\":\"" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> ""
    | Some idx ->
        if idx + 8 <= String.length msg && String.sub msg idx 8 = coin_key then
          let start_idx = idx + 8 in
          match String.index_from_opt msg start_idx '"' with
          | Some end_idx -> String.sub msg start_idx (end_idx - start_idx)
          | None -> ""
        else search (idx + 1)
  in search 0

let rec parse_levels msg pos end_pos count acc =
  if count >= 25 || pos >= end_pos then List.rev acc
  else
    match String.index_from_opt msg pos '{' with
    | None -> List.rev acc
    | Some brace_idx ->
        if brace_idx >= end_pos then List.rev acc
        else
          let px_key = "\"px\":\"" in
          let sz_key = "\"sz\":\"" in
          let rec find_key key start_idx =
            match String.index_from_opt msg start_idx '"' with
            | None -> None
            | Some p ->
                if p + 6 <= end_pos && String.sub msg p 6 = key then
                  let val_start = p + 6 in
                  match String.index_from_opt msg val_start '"' with
                  | Some val_end -> Some (String.sub msg val_start (val_end - val_start), val_end + 1)
                  | None -> None
                else find_key key (p + 1)
          in
          match find_key px_key brace_idx with
          | Some (px_str, next_pos) ->
              (match find_key sz_key next_pos with
               | Some (sz_str, after_sz) ->
                    let px = float_of_string px_str in
                    let sz = float_of_string sz_str in
                    parse_levels msg after_sz end_pos (count + 1) ({price=px; size=sz} :: acc)
               | None -> List.rev acc)
          | None -> List.rev acc

let get_bids_asks msg =
  let levels_key = "\"levels\":[[" in
  let rec search pos =
    match String.index_from_opt msg pos '"' with
    | None -> ([||], [||])
    | Some idx ->
        if idx + 11 <= String.length msg && String.sub msg idx 11 = levels_key then
          let bids_start = idx + 11 in
          match String.index_from_opt msg bids_start ']' with
          | None -> ([||], [||])
          | Some bids_end ->
              let bids_list = parse_levels msg bids_start bids_end 0 [] in
              let asks_start = bids_end + 2 in
              if asks_start < String.length msg then
                match String.index_from_opt msg asks_start ']' with
                | None -> (Array.of_list bids_list, [||])
                | Some asks_end ->
                    let asks_list = parse_levels msg asks_start asks_end 0 [] in
                    (Array.of_list bids_list, Array.of_list asks_list)
              else (Array.of_list bids_list, [||])
        else search (idx + 1)
  in search 0

let () =
  let sample = "{\"channel\":\"l2Book\",\"data\":{\"coin\":\"BTC\",\"time\":1776043711898,\"levels\":[[{\"px\":\"71093.0\",\"sz\":\"1.786\",\"n\":1}],[]]}}" in
  let coin = get_coin sample in
  let bids, asks = get_bids_asks sample in
  Printf.printf "coin: %s, bids: %d, asks: %d\n" coin (Array.length bids) (Array.length asks);
  if Array.length bids > 0 then Printf.printf "bid0: %f\n" bids.(0).price
