(* Differential parity test for the span-based orderbook path.

   [process_orderbook_message_span] must reproduce the DOM reference
   [process_orderbook_message] state-for-state: level rendering (fixed decimals,
   wire strings bit-identical so CRC32 checksums stay valid), sequence
   rollback/gap handling, zero-size removal, depth truncation, checksum
   validation at the throttled ticks, ring-buffer frames, and readiness. The two
   paths are driven on distinct symbols so their global per-symbol stores cannot
   collide, and the store state is fingerprinted after every frame. *)

let hb () = ()

let sym_counter = ref 0

let fresh_symbols () =
  incr sym_counter;
  Printf.sprintf "PARI%02dA/USD" !sym_counter, Printf.sprintf "PARI%02dB/USD" !sym_counter
;;

let clear_stores () = Kraken.Kraken_orderbook_feed.clear_all_stores ()

(** Replace all occurrences of [needle] in [s] with [repl]. Frames declare the
    per-symbol channel via a [$SYM$] placeholder so a single template can carry
    several entries for one symbol. *)
let replace_all ~needle ~repl s =
  let buf = Buffer.create (String.length s) in
  let nlen = String.length needle in
  let slen = String.length s in
  let rec go i =
    if i >= slen
    then ()
    else if i + nlen <= slen && String.sub s i nlen = needle
    then (
      Buffer.add_string buf repl;
      go (i + nlen))
    else (
      Buffer.add_char buf s.[i];
      go (i + 1))
  in
  go 0;
  Buffer.contents buf
;;

let book_frame ~typ ~bids ~asks ~seq ~checksum =
  Printf.sprintf
    "{\"channel\":\"book\",\"type\":\"%s\",\"data\":[{\"symbol\":\"$SYM$\",\"sequence\":%s,\
     \"bids\":[%s],\"asks\":[%s],\"checksum\":%s}]}"
    typ
    seq
    bids
    asks
    checksum
;;

let lvl_fp l =
  String.concat
    "|"
    [ l.Kraken.Kraken_orderbook_feed.price
    ; l.Kraken.Kraken_orderbook_feed.price_wire
    ; l.Kraken.Kraken_orderbook_feed.size
    ; Printf.sprintf "%.17g" l.Kraken.Kraken_orderbook_feed.price_float
    ; Printf.sprintf "%.17g" l.Kraken.Kraken_orderbook_feed.size_float
    ]
;;

let lvl_compare (a : Kraken.Kraken_orderbook_feed.level) b =
  let c =
    Float.compare
      a.Kraken.Kraken_orderbook_feed.price_float
      b.Kraken.Kraken_orderbook_feed.price_float
  in
  if c <> 0
  then c
  else (
    let c = String.compare a.Kraken.Kraken_orderbook_feed.price b.Kraken.Kraken_orderbook_feed.price in
    if c <> 0
    then c
    else (
      let c =
        String.compare
          a.Kraken.Kraken_orderbook_feed.price_wire
          b.Kraken.Kraken_orderbook_feed.price_wire
      in
      if c <> 0 then c else String.compare a.Kraken.Kraken_orderbook_feed.size b.Kraken.Kraken_orderbook_feed.size))
;;

let levels_fp arr =
  let sorted = List.sort lvl_compare (Array.to_list arr) in
  Printf.sprintf "[%d]%s" (List.length sorted) (String.concat ";" (List.map lvl_fp sorted))
;;

(* Canonical per-symbol store fingerprint covering every state variable the two
   paths could diverge on, including the latest ring-buffer frame (timestamp
   excluded - it is wall-clock, not derived from the frame). *)
let store_fp symbol =
  match Kraken.Kraken_orderbook_feed.store_opt symbol with
  | None -> "NO_STORE"
  | Some store ->
    let bids =
      Kraken.Kraken_orderbook_feed.levels_to_array
        ~sort_desc:true
        store.Kraken.Kraken_orderbook_feed.bids
        10
    in
    let asks =
      Kraken.Kraken_orderbook_feed.levels_to_array
        ~sort_desc:false
        store.Kraken.Kraken_orderbook_feed.asks
        10
    in
    let ring =
      match Kraken.Kraken_orderbook_feed.get_latest_orderbook symbol with
      | None -> "none"
      | Some ob ->
        Printf.sprintf
          "seq=%s crc=%s bids=%s asks=%s"
          (match ob.Kraken.Kraken_orderbook_feed.sequence with
           | Some s -> Int64.to_string s
           | None -> "none")
          (match ob.Kraken.Kraken_orderbook_feed.checksum with
           | Some c -> Printf.sprintf "%ld" c
           | None -> "none")
          (levels_fp ob.Kraken.Kraken_orderbook_feed.bids)
          (levels_fp ob.Kraken.Kraken_orderbook_feed.asks)
    in
    Printf.sprintf
      "seq=%s snap=%b ready=%b tick=%d bids=%s asks=%s ring=%s"
      (match Atomic.get store.Kraken.Kraken_orderbook_feed.last_sequence with
       | Some s -> Int64.to_string s
       | None -> "none")
      (Atomic.get store.Kraken.Kraken_orderbook_feed.has_snapshot)
      (Atomic.get store.Kraken.Kraken_orderbook_feed.ready)
      store.Kraken.Kraken_orderbook_feed.checksum_tick
      (levels_fp bids)
      (levels_fp asks)
      ring
;;

let check_parity sym_a sym_b label =
  Alcotest.(check string)
    (label ^ ": DOM vs span store parity")
    (store_fp sym_a)
    (store_fp sym_b)
;;

(** [has_snapshot sym] *)
let has_snapshot sym =
  match Kraken.Kraken_orderbook_feed.store_opt sym with
  | Some s -> Atomic.get s.Kraken.Kraken_orderbook_feed.has_snapshot
  | None -> false
;;

(** Drive [frames] through both paths, comparing store fingerprints after every
    frame. *)
let run_both sym_a sym_b frames =
  List.iter
    (fun (reset, frame) ->
       let fa = replace_all ~needle:"$SYM$" ~repl:sym_a frame in
       let fb = replace_all ~needle:"$SYM$" ~repl:sym_b frame in
       ignore
         (Kraken.Kraken_orderbook_feed.process_orderbook_message ~reset
            (Yojson.Safe.from_string fa)
            hb);
       ignore (Kraken.Kraken_orderbook_feed.process_orderbook_message_span ~reset fb hb);
       check_parity sym_a sym_b "frame")
    frames
;;

(* ---- Scalar rendering: decimal + integer span readers vs the DOM ---------- *)

let test_decimal_span_matches_dom () =
  let cases =
    [ `Float 5.1e-05, "5.1e-05", 8, "0.00005100"
    ; `Float 0.000051, "0.000051", 8, "0.00005100"
    ; `Float 1.5, "1.5", 2, "1.50"
    ; `Float 100.0, "100.0", 8, "100.00000000"
    ; `Int 42, "42", 8, "42"
    ; `Int 0, "-0", 8, "0"
    ; `Intlit "12345678901234567890", "12345678901234567890", 8, "12345678901234567890"
    ; `String "45000.50", "\"45000.50\"", 8, "45000.50"
    ; `String "1.20000000", "\"1.20000000\"", 8, "1.20000000"
    ]
  in
  List.iter
    (fun (json, text, dec, expected) ->
       let dom = Kraken.Kraken_orderbook_feed.to_decimal_str ~trim_trailing:false ~dec json in
       let span = Kraken.Kraken_orderbook_feed.decimal_str_of_span ~dec text 0 (String.length text) in
       Alcotest.(check string) ("decimal dom " ^ text) expected dom;
       Alcotest.(check string) ("decimal span " ^ text) expected span)
    cases
;;

let test_scalar_span_matches_dom () =
  let cases =
    [ `Int 42, "42", Some 42L, Some 42l
    ; `Int 0, "-0", Some 0L, Some 0l
    ; ( `Int 9007199254740993
      , "9007199254740993"
      , Some 9007199254740993L
      , Some (Int32.of_int 9007199254740993) )
    ; ( `Int 3310070434
      , "3310070434"
      , Some 3310070434L
      , Some (Int32.of_int 3310070434) )
    ; `Int 2147483647, "2147483647", Some 2147483647L, Some 2147483647l
    ; `Intlit "12345678901234567890", "12345678901234567890", None, None
    ; `Float 45001.5, "45001.5", Some (Int64.of_float 45001.5), Some (Int32.of_float 45001.5)
    ; `String "123", "\"123\"", Some 123L, Some 123l
    ]
  in
  List.iter
    (fun (json, text, e64, e32) ->
       let d64 = Kraken.Kraken_orderbook_feed.int64_of_json json in
       let s64 = Kraken.Kraken_orderbook_feed.int64_of_span text 0 (String.length text) in
       Alcotest.(check (option int64)) ("int64 span " ^ text) e64 s64;
       Alcotest.(check (option int64)) ("int64 dom " ^ text) d64 e64;
       let d32 = Kraken.Kraken_orderbook_feed.int32_of_json json in
       let s32 = Kraken.Kraken_orderbook_feed.int32_of_span text 0 (String.length text) in
       Alcotest.(check (option int32)) ("int32 span " ^ text) e32 s32;
       Alcotest.(check (option int32)) ("int32 dom " ^ text) d32 e32)
    cases
;;

(* ---- Differential scenarios ---------------------------------------------- *)

let test_checksum_validation_parity () =
  clear_stores ();
  let sym_a, sym_b = fresh_symbols () in
  let snapshot =
    ( true
    , book_frame ~typ:"snapshot"
        ~bids:
          "{\"price\":45000.5,\"qty\":2},{\"price\":45000.0,\"qty\":1},\
           {\"price\":44999.5,\"qty\":0.5},{\"price\":44998.0,\"qty\":1.25}"
        ~asks:
          "{\"price\":45001.0,\"qty\":3},{\"price\":45002.5,\"qty\":1.5},\
           {\"price\":45003.0,\"qty\":2},{\"price\":45004.0,\"qty\":0.75}"
        ~seq:"1"
        ~checksum:"0" )
  in
  let updates_1_8 =
    List.init 8 (fun i ->
        let seq = i + 2 in
        ( false
        , book_frame ~typ:"update"
            ~bids:(Printf.sprintf "{\"price\":%f,\"qty\":%f}" (45000.5 -. float i) (2.0 +. float i))
            ~asks:(Printf.sprintf "{\"price\":%f,\"qty\":%f}" (45001.0 +. float i) (3.0 +. float i))
            ~seq:(string_of_int seq)
            ~checksum:"0" ))
  in
  run_both sym_a sym_b (snapshot :: updates_1_8);
  (* Tick is now 9. The next frame lands on a validation tick (tick = 10); its
     expected CRC is whatever the DOM path computes from its own store right
     now. If the span path's rendering diverged anywhere, its own computed CRC
     would differ and this frame would desync only one of the two books. *)
  let expected_crc =
    match Kraken.Kraken_orderbook_feed.store_opt sym_a with
    | Some store ->
      Kraken.Kraken_orderbook_feed.calculate_checksum
        sym_a
        (Kraken.Kraken_orderbook_feed.levels_to_array
           ~sort_desc:true
           store.Kraken.Kraken_orderbook_feed.bids
           10)
        (Kraken.Kraken_orderbook_feed.levels_to_array
           ~sort_desc:false
           store.Kraken.Kraken_orderbook_feed.asks
           10)
    | None -> Alcotest.fail "expected store after snapshot feed"
  in
  let valid_clear_frame =
    ( false
    , book_frame ~typ:"update"
        ~bids:""
        ~asks:""
        ~seq:"10"
        ~checksum:(string_of_int (Int32.to_int expected_crc)) )
  in
  run_both sym_a sym_b [ valid_clear_frame ];
  Alcotest.(check bool)
    "correct checksum keeps DOM book populated"
    true
    (has_snapshot sym_a);
  Alcotest.(check bool)
    "correct checksum keeps span book populated"
    true
    (has_snapshot sym_b);
  (* Nine more no-op updates (ticks 11..19), then a wrong checksum at tick 20. *)
  let updates_11_19 =
    List.init 9 (fun i ->
        let seq = 11 + i in
        (false, book_frame ~typ:"update" ~bids:"" ~asks:"" ~seq:(string_of_int seq) ~checksum:"0"))
  in
  run_both sym_a sym_b updates_11_19;
  let wrong_crc = Int32.add expected_crc 1l in
  let reject_frame =
    ( false
    , book_frame ~typ:"update"
        ~bids:""
        ~asks:""
        ~seq:"20"
        ~checksum:(string_of_int (Int32.to_int wrong_crc)) )
  in
  run_both sym_a sym_b [ reject_frame ];
  Alcotest.(check bool) "checksum mismatch clears DOM book" false (has_snapshot sym_a);
  Alcotest.(check bool) "checksum mismatch clears span book" false (has_snapshot sym_b)
;;

let test_shape_and_encoding_parity () =
  let sym_a, sym_b = fresh_symbols () in
  let frames =
    [ ( true
      , book_frame ~typ:"snapshot"
          ~bids:
            "[45000.5,2],[\"45001.0\",\"1.5\"],[\"44999.0\",0],\
             {\"qty\":5,\"foo\":{\"price\":1,\"qty\":2},\"price\":\"45000.25\"},\
             [44998.5,4.75,1699999999],[44997.0],[44996.0,1,1699999999,\"extra\"]"
          ~asks:"[45002.0,3.25],[45001.5,5.1e-05],[45003.0,1e2]"
          ~seq:"1"
          ~checksum:"null" )
    ; ( false
      , book_frame ~typ:"update"
          ~bids:"[45000.5,2.5],[44997.0,0]"
          ~asks:"[45001.0,3.5]"
          ~seq:"2"
          ~checksum:"null" )
    ; ( false
      , book_frame ~typ:"update"
          ~bids:"[\"45000.5\",\"0\"]"
          ~asks:"[45001.0,-0.0]"
          ~seq:"3"
          ~checksum:"null" )
    ; ( false
      , book_frame ~typ:"update"
          ~bids:"{\"price\":\"45001.0\",\"qty\":0.0}"
          ~asks:"{\"price\":45004.0,\"qty\":0}"
          ~seq:"4"
          ~checksum:"null" )
    ]
  in
  run_both sym_a sym_b frames;
  (* The 4-element array level and the 1-element array level are skipped by
     both paths; zero-size (0, -0.0, "0", 0.0) removals cleared the same keys
     on both sides. *)
  Alcotest.(check bool)
    "zero-size removals keep fp equal"
    true
    ((store_fp sym_a) = (store_fp sym_b))
;;

let test_truncation_parity () =
  let sym_a, sym_b = fresh_symbols () in
  let mk_levels n base bid =
    List.init n (fun i ->
        let p = if bid then base -. float i *. 1.0 else base +. float i *. 1.0 in
        Printf.sprintf "{\"price\":%f,\"qty\":%f}" p (1.0 +. float i))
    |> String.concat ","
  in
  let snapshot =
    ( true
    , book_frame ~typ:"snapshot"
        ~bids:(mk_levels 15 45100.0 true)
        ~asks:(mk_levels 15 45101.0 false)
        ~seq:"1"
        ~checksum:"0" )
  in
  run_both sym_a sym_b [ snapshot ];
  let bids_len sym =
    match Kraken.Kraken_orderbook_feed.store_opt sym with
    | Some s -> Hashtbl.length s.Kraken.Kraken_orderbook_feed.bids
    | None -> -1
  in
  Alcotest.(check int) "DOM bids truncated to depth" 10 (bids_len sym_a);
  Alcotest.(check int) "span bids truncated to depth" 10 (bids_len sym_b)
;;

let test_batch_multi_entry_parity () =
  let sym_a, sym_b = fresh_symbols () in
  let snapshot =
    book_frame ~typ:"snapshot" ~bids:"{\"price\":45000.5,\"qty\":2}" ~asks:"[]" ~seq:"0" ~checksum:"null"
  in
  let batch =
    "{\"channel\":\"book\",\"type\":\"update\",\"data\":[{\"symbol\":\"$SYM$\",\"sequence\":1,\"bids\":[{\"price\":45000.0,\"qty\":1}],\"asks\":[],\"checksum\":0},{\"symbol\":\"$SYM$\",\"sequence\":2,\"bids\":[{\"price\":44999.0,\"qty\":4}],\"asks\":[],\"checksum\":0}]}"
  in
  run_both sym_a sym_b [ true, snapshot; false, batch ]
;;

let test_rollback_parity () =
  let sym_a, sym_b = fresh_symbols () in
  let frames =
    [ ( true
      , book_frame ~typ:"snapshot"
          ~bids:"{\"price\":45000.5,\"qty\":2}"
          ~asks:"{\"price\":45001.0,\"qty\":3}"
          ~seq:"10"
          ~checksum:"0" )
    ; ( false
      , book_frame ~typ:"update"
          ~bids:"{\"price\":45000.5,\"qty\":2.5}"
          ~asks:"{\"price\":45001.0,\"qty\":3}"
          ~seq:"11"
          ~checksum:"0" )
    ; ( false
      , book_frame ~typ:"update"
          ~bids:"{\"price\":45000.0,\"qty\":1}"
          ~asks:"{\"price\":45001.0,\"qty\":3}"
          ~seq:"9"
          ~checksum:"0" )
    ]
  in
  run_both sym_a sym_b frames;
  Alcotest.(check bool) "rollback clears DOM book" false (has_snapshot sym_a);
  Alcotest.(check bool) "rollback clears span book" false (has_snapshot sym_b)
;;

let test_gap_parity () =
  let sym_a, sym_b = fresh_symbols () in
  let frames =
    [ ( true
      , book_frame ~typ:"snapshot"
          ~bids:"{\"price\":45000.5,\"qty\":2}"
          ~asks:"{\"price\":45001.0,\"qty\":3}"
          ~seq:"10"
          ~checksum:"0" )
    ; ( false
      , book_frame ~typ:"update"
          ~bids:"{\"price\":45000.0,\"qty\":1}"
          ~asks:"{\"price\":45001.0,\"qty\":3}"
          ~seq:"14"
          ~checksum:"0" )
    ]
  in
  run_both sym_a sym_b frames;
  Alcotest.(check bool) "gap clears DOM book" false (has_snapshot sym_a);
  Alcotest.(check bool) "gap clears span book" false (has_snapshot sym_b)
;;

let test_update_before_snapshot_parity () =
  let sym_a, sym_b = fresh_symbols () in
  let frames =
    [ ( false
      , book_frame ~typ:"update"
          ~bids:"{\"price\":45000.5,\"qty\":2}"
          ~asks:"{\"price\":45001.0,\"qty\":3}"
          ~seq:"5"
          ~checksum:"0" )
    ]
  in
  run_both sym_a sym_b frames;
  Alcotest.(check bool) "update-before-snapshot leaves no snapshot" false (has_snapshot sym_a);
  match Kraken.Kraken_orderbook_feed.store_opt sym_a with
  | Some s ->
    Alcotest.(check int)
      "update-before-snapshot applies no bids"
      0
      (Hashtbl.length s.Kraken.Kraken_orderbook_feed.bids)
  | None -> Alcotest.fail "store should exist after an ignored update"
;;

(* ---- Live router ---------------------------------------------------------- *)

let test_router_smoke () =
  clear_stores ();
  let sym = "PARTRT/USD" in
  let hb_count = ref 0 in
  Atomic.set Kraken.Kraken_orderbook_feed.current_on_heartbeat (Some (fun () -> incr hb_count));
  let f typ bids asks seq crc =
    let raw = book_frame ~typ ~bids ~asks ~seq ~checksum:crc in
    replace_all ~needle:"$SYM$" ~repl:sym raw
  in
  Kraken.Kraken_orderbook_feed.process_parse_domain_frame "{\"channel\":\"heartbeat\"}";
  Kraken.Kraken_orderbook_feed.process_parse_domain_frame "{\"method\":\"heartbeat\"}";
  Kraken.Kraken_orderbook_feed.process_parse_domain_frame
    (f "snapshot" "{\"price\":45000.5,\"qty\":2}" "{\"price\":45001.0,\"qty\":3},{\"price\":45002.0,\"qty\":1}" "1" "0");
  Kraken.Kraken_orderbook_feed.process_parse_domain_frame
    (f "update" "{\"price\":45000.5,\"qty\":2.5}" "{\"price\":45002.0,\"qty\":0}" "2" "0");
  Atomic.set Kraken.Kraken_orderbook_feed.current_on_heartbeat None;
  Alcotest.(check int)
    "heartbeat frames + book entries routed to on_heartbeat"
    4
    !hb_count;
  match Kraken.Kraken_orderbook_feed.get_best_bid_ask sym with
  | Some (bp, bs, ap, as_) ->
    Alcotest.(check (float 1e-9)) "router best bid price" 45000.5 bp;
    Alcotest.(check (float 1e-9)) "router best bid size" 2.5 bs;
    Alcotest.(check (float 1e-9)) "router best ask price" 45001.0 ap;
    Alcotest.(check (float 1e-9)) "router best ask size" 3.0 as_
  | None -> Alcotest.fail "router produced no top of book"
;;

let () =
  Alcotest.run
    "kraken_span_orderbook"
    [ ( "scalar parity"
      , [ Alcotest.test_case "decimal span rendering" `Quick test_decimal_span_matches_dom
        ; Alcotest.test_case "int span readers match DOM readers" `Quick test_scalar_span_matches_dom
        ] )
    ; ( "differential"
      , [ Alcotest.test_case "checksum validation (accept + reject)" `Quick test_checksum_validation_parity
        ; Alcotest.test_case "level shapes and encodings" `Quick test_shape_and_encoding_parity
        ; Alcotest.test_case "depth truncation" `Quick test_truncation_parity
        ; Alcotest.test_case "batch multi-entry frames" `Quick test_batch_multi_entry_parity
        ; Alcotest.test_case "sequence rollback clears both" `Quick test_rollback_parity
        ; Alcotest.test_case "sequence gap clears both" `Quick test_gap_parity
        ; Alcotest.test_case "update before snapshot ignored" `Quick test_update_before_snapshot_parity
        ] )
    ; ( "router"
      , [ Alcotest.test_case "span router handles book and heartbeat" `Quick test_router_smoke
        ] )
    ]
;;