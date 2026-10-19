(** In-tree Base64 (RFC 4648) implementation.

    Only the encode/decode surface the engine uses is provided. The module name and
    function signatures match the former opam package. *)

type alphabet = string
type sub = string * int * int

let default_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
let uri_safe_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

let make_alphabet s =
  if String.length s <> 64
  then invalid_arg "Base64.make_alphabet: alphabet must be 64 characters";
  s
;;

let length_alphabet = String.length
let alphabet a = a

let bounds s off len =
  if off < 0 || len < 0 || off + len > String.length s
  then Error (`Msg "Base64: invalid bounds")
  else Ok ()
;;

let encode ?(pad = true) ?(alphabet = default_alphabet) ?(off = 0) ?len s =
  let len =
    match len with
    | Some l -> l
    | None -> String.length s - off
  in
  match bounds s off len with
  | Error _ as e -> e
  | Ok () ->
    let out = Buffer.create ((len + 2) / 3 * 4) in
    let byte i = Char.code s.[off + i] in
    let rec loop i =
      if i >= len
      then ()
      else (
        let n = min 3 (len - i) in
        let b0 = byte i in
        let b1 = if n > 1 then byte (i + 1) else 0 in
        let b2 = if n > 2 then byte (i + 2) else 0 in
        Buffer.add_char out alphabet.[b0 lsr 2];
        Buffer.add_char out alphabet.[((b0 land 0x3) lsl 4) lor (b1 lsr 4)];
        if n > 1
        then Buffer.add_char out alphabet.[((b1 land 0xf) lsl 2) lor (b2 lsr 6)]
        else if pad
        then Buffer.add_char out '=';
        if n > 2
        then Buffer.add_char out alphabet.[b2 land 0x3f]
        else if pad
        then Buffer.add_char out '=';
        loop (i + 3))
    in
    loop 0;
    Ok (Buffer.contents out)
;;

let encode_exn ?pad ?alphabet ?off ?len s =
  match encode ?pad ?alphabet ?off ?len s with
  | Ok s -> s
  | Error (`Msg msg) -> invalid_arg msg
;;

let encode_string ?pad ?alphabet s = encode_exn ?pad ?alphabet s

let decode_value alphabet c =
  let rec loop i =
    if i >= String.length alphabet
    then -1
    else if alphabet.[i] = c
    then i
    else loop (i + 1)
  in
  loop 0
;;

let decode ?(pad = true) ?(alphabet = default_alphabet) ?(off = 0) ?len s =
  let len =
    match len with
    | Some l -> l
    | None -> String.length s - off
  in
  match bounds s off len with
  | Error _ as e -> e
  | Ok () ->
    let out = Buffer.create (len * 3 / 4) in
    let vals = Array.make 4 0 in
    let count = ref 0 in
    let pad_count = ref 0 in
    let error = ref None in
    let flush n =
      if n >= 2
      then (
        let b0 = (vals.(0) lsl 2) lor (vals.(1) lsr 4) in
        Buffer.add_char out (Char.chr b0);
        if n >= 3
        then (
          let b1 = ((vals.(1) land 0xf) lsl 4) lor (vals.(2) lsr 2) in
          Buffer.add_char out (Char.chr b1));
        if n >= 4
        then (
          let b2 = ((vals.(2) land 0x3) lsl 6) lor vals.(3) in
          Buffer.add_char out (Char.chr b2)));
      count := 0;
      pad_count := 0
    in
    (try
       for i = 0 to len - 1 do
         let c = s.[off + i] in
         match c with
         | ' ' | '\t' | '\r' | '\n' -> ()
         | '=' ->
           incr pad_count;
           incr count;
           if !count = 4 then flush (4 - !pad_count)
         | _ ->
           let v = decode_value alphabet c in
           if v < 0
           then (
             error := Some (Printf.sprintf "Base64: invalid character %C" c);
             raise Exit);
           vals.(!count - !pad_count) <- v;
           incr count;
           if !count = 4 then flush (4 - !pad_count)
       done
     with
     | Exit -> ());
    (match !error with
     | Some msg -> Error (`Msg msg)
     | None ->
       if !pad_count > 0
       then Error (`Msg "Base64: incomplete padding")
       else (
         if !count > 0 then flush !count;
         if pad && len mod 4 <> 0 && !count = 0
         then Ok (Buffer.contents out)
         else Ok (Buffer.contents out)))
;;

let decode_exn ?pad ?alphabet ?off ?len s =
  match decode ?pad ?alphabet ?off ?len s with
  | Ok s -> s
  | Error (`Msg msg) -> invalid_arg msg
;;
