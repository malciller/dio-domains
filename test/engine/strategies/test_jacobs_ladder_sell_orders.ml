module SO = Dio_strategies.Jacobs_ladder_sell_orders

let id_check = Alcotest.(list (triple string (float 1e-9) (float 1e-9)))
let sample = [ "a", 100.0, 1.0; "b", 99.0, 2.5; "c", 98.0, 0.25 ]

let test_empty () =
  let t = SO.create 4 in
  Alcotest.(check bool) "empty" true (SO.is_empty t);
  Alcotest.(check int) "length 0" 0 (SO.length t);
  Alcotest.(check id_check) "to_list empty" [] (SO.to_list t)
;;

let test_push_order () =
  let t = SO.create 2 in
  List.iter (fun (id, p, q) -> SO.push t id p q) sample;
  Alcotest.(check int) "length" 3 (SO.length t);
  Alcotest.(check bool) "not empty" false (SO.is_empty t);
  Alcotest.(check id_check) "insertion order preserved" sample (SO.to_list t)
;;

let test_growth () =
  (* Start with capacity 1 and push past it repeatedly: values must survive the
     reallocation, in order. *)
  let t = SO.create 1 in
  let n = 100 in
  for i = 1 to n do
    SO.push t (Printf.sprintf "o%d" i) (float_of_int i) (float_of_int i /. 2.0)
  done;
  Alcotest.(check int) "length after growth" n (SO.length t);
  let l = SO.to_list t in
  Alcotest.(check int) "to_list length" n (List.length l);
  Alcotest.(check string)
    "first id"
    "o1"
    (let a, _, _ = List.nth l 0 in
     a);
  Alcotest.(check string)
    "last id"
    (Printf.sprintf "o%d" n)
    (let a, _, _ = List.nth l (n - 1) in
     a);
  Alcotest.(check (float 1e-9))
    "last price"
    (float_of_int n)
    (let _, p, _ = List.nth l (n - 1) in
     p)
;;

let test_clear () =
  let t = SO.of_list sample in
  SO.clear t;
  Alcotest.(check int) "cleared" 0 (SO.length t);
  Alcotest.(check bool) "empty after clear" true (SO.is_empty t);
  (* Reusable after clear. *)
  SO.push t "z" 1.0 1.0;
  Alcotest.(check id_check) "reuse after clear" [ "z", 1.0, 1.0 ] (SO.to_list t)
;;

let test_iter_fold () =
  let t = SO.of_list sample in
  let seen = ref [] in
  SO.iter t (fun id _ _ -> seen := id :: !seen);
  Alcotest.(check (list string))
    "iter visits all in order"
    [ "a"; "b"; "c" ]
    (List.rev !seen);
  let total = SO.fold t 0.0 (fun acc _ _ q -> acc +. q) in
  Alcotest.(check (float 1e-9)) "fold sums qty" 3.75 total
;;

let test_exists () =
  let t = SO.of_list sample in
  Alcotest.(check bool) "exists_id hit" true (SO.exists_id t "b");
  Alcotest.(check bool) "exists_id miss" false (SO.exists_id t "zzz");
  Alcotest.(check bool) "exists_price <=99" true (SO.exists_price t (fun p -> p <= 99.0));
  Alcotest.(check bool) "exists_price >100" false (SO.exists_price t (fun p -> p > 100.0))
;;

let test_exists_price_leq () =
  let t = SO.of_list sample in
  Alcotest.(check bool) "leq 99" true (SO.exists_price_leq t 99.0);
  Alcotest.(check bool) "leq 97.5" false (SO.exists_price_leq t 97.5);
  Alcotest.(check bool) "empty" false (SO.exists_price_leq (SO.create 4) 1e9)
;;

let test_sum_qty () =
  let t = SO.of_list sample in
  Alcotest.(check (float 1e-9)) "sum" 3.75 (SO.sum_qty t);
  Alcotest.(check (float 1e-9)) "empty sum" 0.0 (SO.sum_qty (SO.create 4))
;;

let test_find_first () =
  let t = SO.of_list sample in
  (match SO.find_first t (fun id _ _ -> id = "b") with
   | Some (id, p, q) ->
     Alcotest.(check string) "found id" "b" id;
     Alcotest.(check (float 1e-9)) "found price" 99.0 p;
     Alcotest.(check (float 1e-9)) "found qty" 2.5 q
   | None -> Alcotest.fail "expected Some");
  Alcotest.(check bool)
    "no match -> None"
    true
    (SO.find_first t (fun id _ _ -> id = "nope") = None)
;;

let test_replace_first () =
  (* Two elements satisfy the predicate; only the first may change. *)
  let t = SO.of_list [ "p1", 100.0, 1.0; "p2", 100.0, 2.0; "k", 50.0, 3.0 ] in
  SO.replace_first
    t
    (fun id _ _ -> String.starts_with ~prefix:"p" id)
    (fun _ p q -> "real", p, q);
  Alcotest.(check id_check)
    "first replaced, second untouched"
    [ "real", 100.0, 1.0; "p2", 100.0, 2.0; "k", 50.0, 3.0 ]
    (SO.to_list t)
;;

let test_remove_by_id () =
  let t = SO.of_list sample in
  Alcotest.(check bool) "removed b" true (SO.remove_by_id t "b");
  Alcotest.(check id_check)
    "survivors keep order"
    [ "a", 100.0, 1.0; "c", 98.0, 0.25 ]
    (SO.to_list t);
  Alcotest.(check bool) "removed missing is false" false (SO.remove_by_id t "b");
  Alcotest.(check int) "length after removes" 2 (SO.length t)
;;

let test_remove_first_and_last () =
  let t = SO.of_list sample in
  ignore (SO.remove_by_id t "a");
  Alcotest.(check id_check)
    "remove head"
    [ "b", 99.0, 2.5; "c", 98.0, 0.25 ]
    (SO.to_list t);
  ignore (SO.remove_by_id t "c");
  Alcotest.(check id_check) "remove tail" [ "b", 99.0, 2.5 ] (SO.to_list t);
  ignore (SO.remove_by_id t "b");
  Alcotest.(check bool) "emptied" true (SO.is_empty t)
;;

let test_roundtrip () =
  Alcotest.(check id_check) "of_list/to_list" sample (SO.to_list (SO.of_list sample))
;;

let test_blit () =
  (* Copy into an undersized, previously-used destination: it must grow and replace (not
     append to) the destination contents. *)
  let src = SO.of_list sample in
  let dst = SO.of_list [ "old", 1.0, 9.0 ] in
  SO.blit ~src ~dst;
  Alcotest.(check id_check) "blit replaces contents" sample (SO.to_list dst);
  Alcotest.(check int) "dst length" 3 (SO.length dst);
  (* blit of an empty source clears the destination. *)
  SO.clear src;
  SO.blit ~src ~dst;
  Alcotest.(check bool) "blit from empty clears" true (SO.is_empty dst)
;;

let test_remove_prefix () =
  let t =
    SO.of_list
      [ "pending_sell_1", 1.0, 1.0; "real", 2.0, 2.0; "pending_sell_2", 3.0, 3.0 ]
  in
  SO.remove_prefix t "pending_sell_";
  Alcotest.(check id_check)
    "prefix entries removed, order kept"
    [ "real", 2.0, 2.0 ]
    (SO.to_list t)
;;

let () =
  Alcotest.run
    "Jacobs Ladder sell orders"
    [ ( "store"
      , [ Alcotest.test_case "empty" `Quick test_empty
        ; Alcotest.test_case "push preserves order" `Quick test_push_order
        ; Alcotest.test_case "growth" `Quick test_growth
        ; Alcotest.test_case "clear/reuse" `Quick test_clear
        ; Alcotest.test_case "iter/fold" `Quick test_iter_fold
        ; Alcotest.test_case "exists" `Quick test_exists
        ; Alcotest.test_case "exists_price_leq" `Quick test_exists_price_leq
        ; Alcotest.test_case "sum_qty" `Quick test_sum_qty
        ; Alcotest.test_case "find_first" `Quick test_find_first
        ; Alcotest.test_case "replace_first" `Quick test_replace_first
        ; Alcotest.test_case "remove_by_id" `Quick test_remove_by_id
        ; Alcotest.test_case "remove head/tail" `Quick test_remove_first_and_last
        ; Alcotest.test_case "roundtrip" `Quick test_roundtrip
        ; Alcotest.test_case "blit" `Quick test_blit
        ; Alcotest.test_case "remove_prefix" `Quick test_remove_prefix
        ] )
    ]
;;
