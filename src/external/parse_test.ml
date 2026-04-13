let parse_float_fast str start_idx end_idx =
  let len = end_idx - start_idx in
  if len = 0 then 0.0
  else
    let sign = if str.[start_idx] = '-' then -1.0 else 1.0 in
    let start_idx = if str.[start_idx] = '-' then start_idx + 1 else start_idx in
    let rec loop idx acc dec frac_div =
      if idx = end_idx then
        acc /. frac_div
      else
        let c = str.[idx] in
        if c = '.' then
          loop (idx + 1) acc true 1.0
        else if c >= '0' && c <= '9' then
          let d = float_of_int (Char.code c - Char.code '0') in
          if dec then
            loop (idx + 1) (acc +. (d /. (frac_div *. 10.0))) true (frac_div *. 10.0)
          else
            loop (idx + 1) ((acc *. 10.0) +. d) false 1.0
        else
          loop (idx + 1) acc dec frac_div
    in
    sign *. loop start_idx 0.0 false 1.0
