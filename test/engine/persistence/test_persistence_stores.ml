(* Tests for the split persistence stores: base_accumulation_store and
   sell_levels_store (pure decision logic, JSON round-trips, corrupt-file
   handling, legacy migration). Hermetic via DIO_DATA_DIR. *)

let temp_dir () =
  let dir =
    Filename.concat
      (Filename.get_temp_dir_name ())
      (Printf.sprintf
         "dio_persistence_test_%d_%d"
         (Unix.getpid ())
         (Random.int 1_000_000))
  in
  Unix.mkdir dir 0o755;
  dir
;;

let with_hermetic_dir f =
  let dir = temp_dir () in
  Unix.putenv "DIO_DATA_DIR" dir;
  Fun.protect
    ~finally:(fun () ->
      (* Best-effort cleanup: background writers may leave .tmp artifacts,
         so sweep every file before removing the dir. *)
      (try
         Array.iter
           (fun f -> try Sys.remove (Filename.concat dir f) with _ -> ())
           (Sys.readdir dir)
       with _ -> ());
      (try Unix.rmdir dir with _ -> ()))
    (fun () -> f dir)
;;

module A = Dio_persistence.Base_accumulation_store
module S = Dio_persistence.Sell_levels_store

(* -- base_accumulation_store ------------------------------------------- *)

let test_apply_buy_fill () =
  let t = A.default in
  let t = A.apply_buy_fill t ~price:100.0 ~qty:0.5 ~oid:"o1" in
  Alcotest.(check (option (float 1e-12)))
    "buy price recorded"
    (Some 100.0)
    t.A.last_buy_fill_price;
  Alcotest.(check (option (float 1e-12)))
    "buy qty recorded"
    (Some 0.5)
    t.A.last_buy_fill_qty
;;

let test_apply_sell_fill_profit_and_reserve () =
  let t = A.apply_buy_fill A.default ~price:100.0 ~qty:0.5 ~oid:"o1" in
  (* Profitable sell: profit = (110 - 100) * 0.5 = 5.0 > buffer 2.0 -> reserve
     oracle_qty * sell_mult = 1.0 * 2.0 = 2.0 and reset the window. *)
  let t =
    A.apply_sell_fill
      t
      ~price:110.0
      ~qty:0.5
      ~oid:"o2"
      ~buffer:2.0
      ~sell_mult:2.0
      ~oracle_qty:1.0
      ()
  in
  Alcotest.(check (float 1e-12)) "reserved_base accrued" 2.0 t.A.reserved_base;
  Alcotest.(check (float 1e-12)) "accumulated_profit reset" 0.0 t.A.accumulated_profit;
  Alcotest.(check (option (float 1e-12)))
    "sell price recorded"
    (Some 110.0)
    t.A.last_sell_fill_price;
  Alcotest.(check (option Alcotest.string))
    "last oid updated"
    (Some "o2")
    t.A.last_fill_oid
;;

let test_apply_sell_fill_below_buffer () =
  let t = A.apply_buy_fill A.default ~price:100.0 ~qty:0.5 ~oid:"o1" in
  (* Profit 1.0 < buffer 2.0: window accumulates, no reservation. *)
  let t =
    A.apply_sell_fill
      t
      ~price:102.0
      ~qty:0.5
      ~oid:"o2"
      ~buffer:2.0
      ~sell_mult:2.0
      ~oracle_qty:1.0
      ()
  in
  Alcotest.(check (float 1e-12)) "no reservation under buffer" 0.0 t.A.reserved_base;
  Alcotest.(check (float 1e-12)) "profit accumulated" 1.0 t.A.accumulated_profit
;;

let test_apply_sell_fill_unprofitable () =
  let t = A.apply_buy_fill A.default ~price:100.0 ~qty:0.5 ~oid:"o1" in
  let t =
    A.apply_sell_fill
      t
      ~price:90.0
      ~qty:0.5
      ~oid:"o2"
      ~buffer:2.0
      ~sell_mult:2.0
      ~oracle_qty:1.0
      ()
  in
  Alcotest.(check (float 1e-12)) "loss not accumulated" 0.0 t.A.accumulated_profit;
  Alcotest.(check (float 1e-12)) "no reservation on loss" 0.0 t.A.reserved_base
;;

(* -- Round-trip serialization + persistence ---------------------------- *)

let test_accumulation_round_trip () =
  with_hermetic_dir (fun _dir ->
    let key = A.key_of ~strategy:"Grid" ~symbol:"BTC/USDC" ~venue:"hyperliquid" in
    let t =
      { A.reserved_base = 1.25
      ; accumulated_profit = 3.75
      ; last_fill_oid = Some "abc"
      ; last_buy_fill_price = Some 100.0
      ; last_buy_fill_qty = Some 0.5
      ; last_sell_fill_price = None
      ; last_sell_fill_qty = None
      }
    in
    A.save ~key t;
    let loaded = A.load ~key in
    Alcotest.(check (float 1e-12)) "round-trip reserved_base" 1.25 loaded.A.reserved_base;
    Alcotest.(check (float 1e-12))
      "round-trip accumulated_profit"
      3.75
      loaded.A.accumulated_profit;
    Alcotest.(check (option Alcotest.string))
      "round-trip oid"
      (Some "abc")
      loaded.A.last_fill_oid;
    Alcotest.(check (option (float 1e-12)))
      "round-trip buy price"
      (Some 100.0)
      loaded.A.last_buy_fill_price;
    Alcotest.(check (option (float 1e-12)))
      "round-trip sell price"
      None
      loaded.A.last_sell_fill_price;
    (* Absent key loads defaults. *)
    let missing = A.load ~key:"Grid:MISSING:kraken" in
    Alcotest.(check (float 1e-12)) "missing loads default" 0.0 missing.A.reserved_base)
;;

let test_sell_levels_round_trip_and_adopt () =
  with_hermetic_dir (fun _dir ->
    let key = S.key_of ~strategy:"Grid" ~symbol:"QQQ" ~venue:"alpaca" in
    S.save_async ~key [ { S.price = 149.0; qty = 0.25 }; { S.price = 150.0; qty = 0.25 } ];
    (* save_async is drained by a background domain; poll briefly. *)
    let rec wait n =
      if n = 0
      then failwith "async save never landed"
      else if List.length (S.load ~key) = 2
      then ()
      else (
        Thread.delay 0.01;
        wait (n - 1))
    in
    wait 500;
    let loaded = S.load ~key in
    Alcotest.(check int) "two levels" 2 (List.length loaded);
    Alcotest.(check (float 1e-12)) "sorted desc head" 150.0 (List.hd loaded).S.price;
    (* Adoption deduplicates by price tolerance. *)
    S.adopt_exchange_order ~key { S.price = 149.0; qty = 0.25 };
    Alcotest.(check int) "duplicate adoption ignored" 2 (List.length (S.load ~key));
    S.adopt_exchange_order ~key { S.price = 151.0; qty = 0.25 };
    Alcotest.(check int) "new level adopted" 3 (List.length (S.load ~key));
    (* Removal drops matching levels only. *)
    S.remove_levels ~key ~levels:[ { S.price = 151.0; qty = 0.25 } ];
    Alcotest.(check int) "level removed" 2 (List.length (S.load ~key)))
;;

(* -- Corrupt-file handling & legacy migration --------------------------- *)

let test_corrupt_file_backed_up () =
  with_hermetic_dir (fun dir ->
     let path = Filename.concat dir "accumulation_state.json" in
     let oc = open_out path in
     output_string oc "{ this is not json";
     close_out oc;
     (* load triggers the lazy read; the corrupt file must be backed up, not
        discarded, and the store must start fresh. *)
     let t = A.load ~key:"Grid:X:kraken" in
     let entries = Array.to_list (Sys.readdir dir) in
     let backup_exists =
       List.exists (fun f -> String.starts_with ~prefix:"accumulation_state.json.corrupt." f)
         entries
     in
     Alcotest.(check bool) "corrupt file backed up" true backup_exists;
     Alcotest.(check (float 1e-12)) "store starts fresh" 0.0 t.A.reserved_base)
;;

let test_legacy_migration_split () =
  with_hermetic_dir (fun dir ->
    let legacy_path = Filename.concat dir "accumulated_state.json" in
    let legacy =
      {|{
  "SPCX": {
    "reserved_base": 0.016,
    "accumulated_profit": 6.63,
    "last_fill_oid": "d0ac",
    "last_buy_fill_price": 148.258,
    "last_buy_fill_qty": 0.25,
    "sell_levels": [
      [149.0, 0.2497],
      [150.0, 0.2497]
    ]
  }
}|}
    in
    let oc = open_out legacy_path in
    output_string oc legacy;
    close_out oc;
    (* Exactly one configured strategy matches the symbol -> auto-mapped to
        the full strategy key. *)
    Dio_persistence.Persistence_orchestrator.register_configured_strategies
      [ "Grid", "SPCX", "alpaca", true, true ];
    Dio_persistence.Persistence_orchestrator.migrate_if_legacy ();
    Alcotest.(check bool)
      "legacy file renamed away"
      (not (Sys.file_exists legacy_path))
      true;
    let renamed =
      Array.find_opt
        (fun f -> String.starts_with ~prefix:"accumulated_state.json.migrated." f)
        (Sys.readdir dir)
    in
    Alcotest.(check bool) "renamed file retained" (renamed <> None) true;
    let acc_key = A.key_of ~strategy:"Grid" ~symbol:"SPCX" ~venue:"alpaca" in
    let acc = A.load ~key:acc_key in
    Alcotest.(check (float 1e-12)) "migrated reserved_base" 0.016 acc.A.reserved_base;
    Alcotest.(check (float 1e-12))
      "migrated accumulated_profit"
      6.63
      acc.A.accumulated_profit;
    let levels = S.load ~key:acc_key in
    Alcotest.(check int) "migrated sell levels" 2 (List.length levels);
    Alcotest.(check (float 1e-12))
      "migrated level sorted desc"
      150.0
      (List.hd levels).S.price)
;;

let () =
  Random.self_init ();
  Alcotest.run
    "persistence_stores"
    [ ( "base_accumulation"
      , [ "apply_buy_fill", `Quick, test_apply_buy_fill
        ; ( "apply_sell_fill profit+reserve"
          , `Quick
          , test_apply_sell_fill_profit_and_reserve )
        ; "apply_sell_fill below buffer", `Quick, test_apply_sell_fill_below_buffer
        ; "apply_sell_fill unprofitable", `Quick, test_apply_sell_fill_unprofitable
        ] )
    ; ( "round_trip"
      , [ "accumulation", `Quick, test_accumulation_round_trip
        ; "sell_levels adopt/remove", `Quick, test_sell_levels_round_trip_and_adopt
        ] )
    ; "corruption", [ "backed up", `Quick, test_corrupt_file_backed_up ]
    ; "migration", [ "legacy split", `Quick, test_legacy_migration_split ]
    ]
;;
