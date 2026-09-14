module J = Json_scan

(* Interior of a top-level object/array literal (between its braces/brackets). *)
let interior s = 1, String.length s - 1

let get s key =
  let lo, hi = interior s in
  match J.find_field s lo hi key with
  | Some span -> span
  | None -> Alcotest.failf "field %S not found in %S" key s
;;

let fstr s key =
  let i, j = get s key in
  J.string_of_span s i j
;;

let ffloat s key =
  let i, j = get s key in
  J.float_of_span s i j
;;

let fint s key =
  let i, j = get s key in
  J.int_of_span s i j
;;

let test_string_fields () =
  let s = {|{"S":"AAPL","bp":123.45,"bs":10,"ap":124.5,"as":5,"t":"2026-01-01T00:00:00Z"}|} in
  Alcotest.(check string) "S" "AAPL" (fstr s "S");
  Alcotest.(check string) "t" "2026-01-01T00:00:00Z" (fstr s "t");
  Alcotest.(check (float 1e-9)) "bp" 123.45 (ffloat s "bp");
  Alcotest.(check (float 1e-9)) "as" 5.0 (ffloat s "as")
;;

let test_numbers () =
  let s = {|{"a":0,"b":-1,"c":-1.5e3,"d":2.5E-2,"e":12345,"f":1e9}|} in
  Alcotest.(check (float 1e-9)) "a" 0.0 (ffloat s "a");
  Alcotest.(check (float 1e-9)) "b" (-1.0) (ffloat s "b");
  Alcotest.(check (float 1e-6)) "c" (-1500.0) (ffloat s "c");
  Alcotest.(check (float 1e-9)) "d" 0.025 (ffloat s "d");
  Alcotest.(check int) "e" 12345 (fint s "e");
  Alcotest.(check (float 1.0)) "f" 1e9 (ffloat s "f")
;;

let test_nested_not_descended () =
  (* The inner "b" must not shadow the top-level "b". *)
  let s = {|{"a":{"b":1},"b":2}|} in
  Alcotest.(check int) "top-level b" 2 (fint s "b");
  Alcotest.(check int) "top-level a is an object" 0 (J.int_of_span s (fst (get s "a")) (snd (get s "a")))
;;

let test_deep_nesting () =
  let s = {|{"a":[[1,2],{"k":"],[}"}],"b":42}|} in
  Alcotest.(check int) "b after nested mess" 42 (fint s "b")
;;

let test_string_metachars () =
  let s = {|{"x":"a,b}c{d]e","y":1}|} in
  Alcotest.(check string) "x" "a,b}c{d]e" (fstr s "x");
  Alcotest.(check int) "y" 1 (fint s "y")
;;

let test_escapes () =
  let s = {|{"x":"a\"b\\c\n","y":2}|} in
  Alcotest.(check string) "x escapes" "a\"b\\c\n" (fstr s "x");
  Alcotest.(check int) "y" 2 (fint s "y")
;;

let test_missing () =
  let s = {|{"a":1}|} in
  let lo, hi = interior s in
  Alcotest.(check bool) "missing -> None" true (J.find_field s lo hi "z" = None);
  Alcotest.(check bool) "empty object" true (J.find_field "{}" 1 1 "z" = None)
;;

let test_array_of_objects () =
  let s = {|[{"T":"q","S":"A"},{"T":"t","S":"B"}]|} in
  let lo, hi = interior s in
  let seen = ref [] in
  J.array_iter s lo hi (fun i j ->
    let olo, ohi = J.object_interior s i j in
    let t =
      match J.find_field s olo ohi "T" with
      | Some (a, b) -> J.string_of_span s a b
      | None -> "?"
    in
    let sym =
      match J.find_field s olo ohi "S" with
      | Some (a, b) -> J.string_of_span s a b
      | None -> "?"
    in
    seen := (t, sym) :: !seen);
  Alcotest.(check (list (pair string string))) "elements" [ "q", "A"; "t", "B" ] (List.rev !seen)
;;

let test_array_fold_scalars () =
  let s = {|[1,2,3]|} in
  let lo, hi = interior s in
  let sum = J.array_fold s lo hi 0 (fun acc i j -> acc + J.int_of_span s i j) in
  Alcotest.(check int) "sum" 6 sum
;;

let test_whitespace () =
  let s = "{\n  \"a\" : 1 ,\n  \"b\" : \"x\"\n}" in
  Alcotest.(check int) "a" 1 (fint s "a");
  Alcotest.(check string) "b" "x" (fstr s "b")
;;

let () =
  Alcotest.run
    "Json_scan"
    [ ( "fields"
      , [ Alcotest.test_case "string fields" `Quick test_string_fields
        ; Alcotest.test_case "numbers" `Quick test_numbers
        ; Alcotest.test_case "nested not descended" `Quick test_nested_not_descended
        ; Alcotest.test_case "deep nesting" `Quick test_deep_nesting
        ; Alcotest.test_case "string metachars" `Quick test_string_metachars
        ; Alcotest.test_case "escapes" `Quick test_escapes
        ; Alcotest.test_case "missing" `Quick test_missing
        ; Alcotest.test_case "whitespace" `Quick test_whitespace
        ] )
    ; ( "arrays"
      , [ Alcotest.test_case "array of objects" `Quick test_array_of_objects
        ; Alcotest.test_case "array fold scalars" `Quick test_array_fold_scalars
        ] )
    ]
;;
