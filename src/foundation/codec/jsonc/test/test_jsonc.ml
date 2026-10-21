let failures = ref 0

let check name expected actual =
  if String.equal expected actual
  then ()
  else (
    incr failures;
    Printf.eprintf "FAIL %s\n  expected %S\n  actual   %S\n" name expected actual)
;;

let () =
  let open Yojson.Basic.Util in
  let json =
    Jsonc.from_string
      {|{
        // a line comment
        "a": 1,
        /* a block comment
           spanning lines */
        "s": "http://example.test // not a comment",
        "esc": "quote \" and /* not a comment */",
        "arr": [1, 2,],
      }|}
  in
  check "line and block comments" "1" (member "a" json |> to_int |> string_of_int);
  check
    "comment markers inside strings"
    "http://example.test // not a comment"
    (member "s" json |> to_string);
  check
    "escaped quote inside string"
    "quote \" and /* not a comment */"
    (member "esc" json |> to_string);
  check
    "trailing comma in array"
    "2"
    (member "arr" json |> to_list |> List.length |> string_of_int);
  let bom_json = Jsonc.from_string "\xef\xbb\xbf{\"b\":true}" in
  check "utf-8 bom" "true" (member "b" bom_json |> to_bool |> string_of_bool);
  if !failures > 0 then exit 1
;;
