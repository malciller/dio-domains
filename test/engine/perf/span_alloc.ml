(* Allocation probe for the span codec path. Prints words allocated per extraction; the
   [@zero_alloc] and local returns in Json_scan/Kraken must keep these at 0. *)

let level = "[ \"100.25\", \"1.5\" ]"
let level_len = String.length level

let levels =
  "[ [\"100.0\",\"1.0\"], [\"99.0\",\"2.0\"], [\"98.0\",\"3.0\"], [\"97.0\",\"4.0\"] ]"
;;

let levels_len = String.length levels
let obj = "{\"price\":\"100.25\",\"qty\":\"1.5\",\"tags\":[1,2,3]}"
let obj_hi = String.length obj - 1

let fold_levels () =
  Json_scan.array_fold levels 1 (levels_len - 1) 0 (fun acc _ e -> acc + e)
;;

let lighter_frame =
  let lvl p s = Printf.sprintf {|{"price":"%s","size":"%s"}|} p s in
  let side base =
    String.concat "," (List.init 10 (fun i -> lvl (string_of_int (base + i)) "1.0"))
  in
  Printf.sprintf
    {|{"type":"snapshot","channel":"order_book:0","order_book":{"b":[%s],"a":[%s]}}|}
    (side 100)
    (side 101)
;;

let lighter_dom () =
  let json = Yojson.Safe.from_string lighter_frame in
  ignore json;
  1
;;

let lighter_span () =
  let s = lighter_frame in
  let len = String.length s in
  match Json_scan.find_field s 1 (max 0 (len - 1)) "order_book" with
  | Some (oi, oj) ->
    let lo, hi = Json_scan.object_interior s oi oj in
    let count = ref 0 in
    let scan key =
      match Json_scan.find_field s lo hi key with
      | Some (i, j) ->
        let a, b = Json_scan.array_interior s i j in
        Json_scan.array_iter s a b (fun e0 e1 ->
          ignore e0;
          ignore e1;
          incr count)
      | None -> ()
    in
    scan "b";
    scan "a";
    !count
  | None -> 0
;;

let extract_level () =
  match Kraken.Kraken_orderbook_feed.level_price_qty_spans level 0 level_len with
  | Some ((a, b), (c, d)) -> a + b + c + d
  | None -> 0
;;

let extract_fields () =
  match
    Json_scan.find_field obj 1 obj_hi "price", Json_scan.find_field obj 1 obj_hi "qty"
  with
  | Some (a, b), Some (c, d) -> a + b + c + d
  | _ -> 0
;;

let extract_interiors () =
  let a, b = Json_scan.object_interior obj 0 (String.length obj) in
  let c, d = Json_scan.array_interior obj 16 (String.length obj - 1) in
  a + b + c + d
;;

let measure name f n =
  Gc.compact ();
  let w0 = Gc.minor_words () in
  let acc = ref 0 in
  for _ = 1 to n do
    acc := !acc + f ()
  done;
  let w1 = Gc.minor_words () in
  Printf.printf "%-20s %8.2f words/call  (%d)\n" name ((w1 -. w0) /. float n) !acc
;;

let () =
  let n = 1_000_000 in
  measure "level_spans" extract_level n;
  measure "find_field x2" extract_fields n;
  measure "interiors" extract_interiors n;
  measure "array_fold" fold_levels n;
  measure "lighter_dom" lighter_dom (n / 10);
  measure "lighter_span" lighter_span (n / 10)
;;
