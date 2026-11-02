(* RFC 6455 framing round-trip.

   Pins the encoder's exact bytes and then drives the encoder's output back through the
   decoder for every length class (inline 7-bit, 16-bit, 64-bit) plus mask-tail sizes, so
   the word-at-a-time unmask can never drift from the byte it replaces. *)

module Mem = struct
  type ic =
    { src : string
    ; mutable pos : int
    }

  type oc = Buffer.t

  let read ic count =
    let avail = String.length ic.src - ic.pos in
    let n = min count avail in
    if n <= 0
    then Lwt.return ""
    else (
      let s = String.sub ic.src ic.pos n in
      ic.pos <- ic.pos + n;
      Lwt.return s)
  ;;

  let read_into ic buf off len =
    let avail = String.length ic.src - ic.pos in
    let n = min len avail in
    if n <= 0
    then Lwt.return 0
    else (
      Bytes.blit_string ic.src ic.pos buf off n;
      ic.pos <- ic.pos + n;
      Lwt.return n)
  ;;

  let write oc s =
    Buffer.add_string oc s;
    Lwt.return_unit
  ;;

  let flush _ = Lwt.return_unit
end

module Wire = Websocket.Make (Mem)

let key = "\x01\x02\x03\x04"
let key_fn _ = key

let encode content =
  let frame = Websocket.Frame.create ~content () in
  let buf = Buffer.create 64 in
  Wire.write_frame_to_buf ~mode:(Wire.Client key_fn) buf frame;
  Buffer.contents buf
;;

let test_encoder_bytes () =
  let wire = encode "A" in
  let expected = "\x81\x81" ^ key ^ String.make 1 (Char.chr (Char.code 'A' lxor 0x01)) in
  Alcotest.(check string) "known frame bytes" expected wire
;;

let roundtrip len =
  let content = String.init len (fun i -> Char.chr (i land 0xff)) in
  let wire = encode content in
  let ic = { Mem.src = wire; pos = 0 } in
  let oc = Buffer.create 0 in
  let decoded = Lwt_main.run (Wire.make_read_frame ~mode:(Wire.Client key_fn) ic oc ()) in
  Alcotest.(check string)
    (Printf.sprintf "payload len=%d" len)
    content
    decoded.Websocket.Frame.content;
  Alcotest.(check bool) "final" true decoded.Websocket.Frame.final;
  Alcotest.(check int) "consumed exactly the frame" (String.length wire) ic.Mem.pos
;;

let test_lengths () =
  List.iter
    roundtrip
    [ 0; 1; 2; 3; 7; 8; 9; 125; 126; 127; 255; 256; 1000; 65535; 65536; 70000 ]
;;

(* The unmask loop finishes [len mod 8] bytes byte-wise; make sure that tail is correct
   for every remainder independently of the lengths above. *)
let test_mask_tails () = List.iter roundtrip (List.init 8 (fun i -> 120 + i))

let () =
  Alcotest.run
    "ws_framing"
    [ ( "framing"
      , [ Alcotest.test_case "encoder bytes" `Quick test_encoder_bytes
        ; Alcotest.test_case "roundtrip lengths" `Quick test_lengths
        ; Alcotest.test_case "mask tails" `Quick test_mask_tails
        ] )
    ]
;;
