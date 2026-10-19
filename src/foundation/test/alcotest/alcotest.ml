(** Minimal in-tree Alcotest-compatible harness.

    The suite uses test cases, the standard testables, [check], [check_raises],
    [fail]/[failf], and [run]. This module provides those with the same names and
    semantics; failures are collected and the process exits non-zero. *)

type speed_level =
  [ `Quick
  | `Slow
  ]

type test_case = string * speed_level * (unit -> unit)

let test_case name speed f = name, speed, f

type 'a testable =
  { pp : Format.formatter -> 'a -> unit
  ; equal : 'a -> 'a -> bool
  }

let testable ?(pp = fun ppf _ -> Format.fprintf ppf "<opaque>") ?(equal = ( = )) _name =
  { pp; equal }
;;

exception Test_error of string

let fail msg = raise (Test_error msg)
let failf fmt = Format.kasprintf (fun msg -> raise (Test_error msg)) fmt

let show t v =
  let b = Buffer.create 64 in
  let ppf = Format.formatter_of_buffer b in
  t.pp ppf v;
  Format.pp_print_flush ppf ();
  Buffer.contents b
;;

let check t msg expected actual =
  if not (t.equal expected actual)
  then
    fail (Printf.sprintf "%s: expected %s, got %s" msg (show t expected) (show t actual))
;;

let check_raises msg expected f =
  match f () with
  | () -> fail (msg ^ ": expected an exception, but none was raised")
  | exception e ->
    let got = Printexc.to_string e in
    let want = Printexc.to_string expected in
    if got <> want then fail (Printf.sprintf "%s: expected %s, got %s" msg want got)
;;

let bool = { pp = (fun ppf v -> Format.fprintf ppf "%b" v); equal = Bool.equal }
let int = { pp = (fun ppf v -> Format.fprintf ppf "%d" v); equal = Int.equal }
let int32 = { pp = (fun ppf v -> Format.fprintf ppf "%ld" v); equal = Int32.equal }
let int64 = { pp = (fun ppf v -> Format.fprintf ppf "%Ld" v); equal = Int64.equal }

let float tol =
  { pp = (fun ppf v -> Format.fprintf ppf "%.17g" v)
  ; equal =
      (fun a b ->
        a = b || (Float.is_nan a && Float.is_nan b) || Float.abs (a -. b) <= tol)
  }
;;

let string = { pp = (fun ppf v -> Format.fprintf ppf "%S" v); equal = String.equal }
let char = { pp = (fun ppf v -> Format.fprintf ppf "%C" v); equal = Char.equal }

let bytes =
  { pp = (fun ppf v -> Format.fprintf ppf "%S" (Bytes.to_string v)); equal = Bytes.equal }
;;

let unit = { pp = (fun ppf () -> Format.fprintf ppf "()"); equal = (fun () () -> true) }

let exn =
  { pp = (fun ppf e -> Format.fprintf ppf "%s" (Printexc.to_string e))
  ; equal = (fun a b -> Printexc.to_string a = Printexc.to_string b)
  }
;;

let option t =
  { pp =
      (fun ppf -> function
        | None -> Format.fprintf ppf "None"
        | Some v -> Format.fprintf ppf "Some (%a)" t.pp v)
  ; equal =
      (fun a b ->
        match a, b with
        | None, None -> true
        | Some a, Some b -> t.equal a b
        | _ -> false)
  }
;;

let pair a b =
  { pp = (fun ppf (x, y) -> Format.fprintf ppf "(%a, %a)" a.pp x b.pp y)
  ; equal = (fun (a1, b1) (a2, b2) -> a.equal a1 a2 && b.equal b1 b2)
  }
;;

let triple a b c =
  { pp = (fun ppf (x, y, z) -> Format.fprintf ppf "(%a, %a, %a)" a.pp x b.pp y c.pp z)
  ; equal =
      (fun (a1, b1, c1) (a2, b2, c2) -> a.equal a1 a2 && b.equal b1 b2 && c.equal c1 c2)
  }
;;

let quad a b c d =
  { pp =
      (fun ppf (w, x, y, z) ->
        Format.fprintf ppf "(%a, %a, %a, %a)" a.pp w b.pp x c.pp y d.pp z)
  ; equal =
      (fun (a1, b1, c1, d1) (a2, b2, c2, d2) ->
        a.equal a1 a2 && b.equal b1 b2 && c.equal c1 c2 && d.equal d1 d2)
  }
;;

let list t =
  let rec equal a b =
    match a, b with
    | [], [] -> true
    | x :: xs, y :: ys -> t.equal x y && equal xs ys
    | _ -> false
  in
  { pp =
      (fun ppf xs ->
        Format.fprintf ppf "[";
        List.iteri
          (fun i x ->
            if i > 0 then Format.fprintf ppf "; ";
            t.pp ppf x)
          xs;
        Format.fprintf ppf "]")
  ; equal
  }
;;

let array t =
  { pp =
      (fun ppf xs ->
        Format.fprintf ppf "[|";
        Array.iteri
          (fun i x ->
            if i > 0 then Format.fprintf ppf "; ";
            t.pp ppf x)
          xs;
        Format.fprintf ppf "|]")
  ; equal =
      (fun a b ->
        a == b || (Array.length a = Array.length b && Array.for_all2 t.equal a b))
  }
;;

let result ok err =
  { pp =
      (fun ppf -> function
        | Ok v -> Format.fprintf ppf "Ok (%a)" ok.pp v
        | Error e -> Format.fprintf ppf "Error (%a)" err.pp e)
  ; equal =
      (fun a b ->
        match a, b with
        | Ok a, Ok b -> ok.equal a b
        | Error a, Error b -> err.equal a b
        | _ -> false)
  }
;;

let run name suites =
  let failures = ref 0 in
  let total = ref 0 in
  Printf.printf "%s\n" name;
  List.iter
    (fun (suite, cases) ->
      List.iter
        (fun (case_name, _speed, f) ->
          incr total;
          match f () with
          | () -> Printf.printf "  [OK]   %s / %s\n%!" suite case_name
          | exception Test_error msg ->
            incr failures;
            Printf.printf "  [FAIL] %s / %s: %s\n%!" suite case_name msg
          | exception e ->
            incr failures;
            Printf.printf
              "  [FAIL] %s / %s: %s\n%!"
              suite
              case_name
              (Printexc.to_string e))
        cases)
    suites;
  if !failures > 0
  then (
    Printf.printf "%d/%d tests failed\n%!" !failures !total;
    exit 1)
  else Printf.printf "%d tests passed\n%!" !total
;;
