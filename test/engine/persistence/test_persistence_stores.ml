(* Tests for the split persistence stores: base_accumulation_store and sell_levels_store
   (pure decision logic, JSON round-trips, corrupt-file handling, legacy migration).
   Hermetic via DIO_DATA_DIR. *)

(* Test-only [Unix.putenv] to point stores at a temp dir; single-threaded test process.
   OxCaml [unsafe_multidomain] alert acknowledged. *)
[@@@alert "-unsafe_multidomain"]

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
      (* Best-effort cleanup: background writers may leave .tmp artifacts, so sweep every
         file before removing the dir. *)
      (try
         Array.iter
           (fun f ->
             try Sys.remove (Filename.concat dir f) with
             | _ -> ())
           (Sys.readdir dir)
       with
       | _ -> ());
      try Unix.rmdir dir with
      | _ -> ())
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
  (* Profitable sell: profit = (110 - 100) * 0.5 = 5.0. oracle_qty = 0.1, sell_mult = 0.8
     -> accrued_base = 0.1 * (1.0 - 0.8) = 0.02. base_cost = 0.02 * 100.0 = 2.0. With
     buffer 2.0, threshold = base_cost + buffer = 4.0. Since profit 5.0 >= 4.0 -> reserve
     0.02 base and debit profit by cost (5.0 - 2.0 = 3.0). *)
  let t =
    A.apply_sell_fill
      t
      ~price:110.0
      ~qty:0.5
      ~oid:"o2"
      ~buffer:2.0
      ~sell_mult:0.8
      ~oracle_qty:0.1
      ~fees:0.0
      ()
  in
  Alcotest.(check (float 1e-12)) "reserved_base accrued" 0.02 t.A.reserved_base;
  Alcotest.(check (float 1e-12)) "accumulated_profit debited" 3.0 t.A.accumulated_profit;
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
      ~sell_mult:0.8
      ~oracle_qty:0.1
      ~fees:0.0
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
      ~fees:0.0
      ()
  in
  Alcotest.(check (float 1e-12)) "loss not accumulated" 0.0 t.A.accumulated_profit;
  Alcotest.(check (float 1e-12)) "no reservation on loss" 0.0 t.A.reserved_base
;;

let test_apply_sell_fill_one_to_one_no_reserve () =
  (* A 1:1 venue (Hyperliquid/Alpaca) did not withhold the sell_mult slice in the order,
     so a sell fill must never accrue reserved_base: the base was sold. The profit stays
     in quote for the separate, profit-gated lot skim. *)
  let t = A.apply_buy_fill A.default ~price:100.0 ~qty:0.5 ~oid:"o1" in
  let t =
    A.apply_sell_fill
      t
      ~price:110.0
      ~qty:0.5
      ~oid:"o2"
      ~buffer:0.0
      ~sell_mult:0.8
      ~oracle_qty:0.1
      ~sell_uses_mult:false
      ~fees:0.0
      ()
  in
  Alcotest.(check (float 1e-12)) "1:1 sell accrues no base" 0.0 t.A.reserved_base;
  Alcotest.(check (float 1e-12)) "profit kept in quote" 5.0 t.A.accumulated_profit
;;

let test_apply_sell_fill_fees_reduce_profit () =
  (* 0.5 all-in fees reduce profit to 4.5; the reserve accrual then takes 2.0. *)
  let t = A.apply_buy_fill A.default ~price:100.0 ~qty:0.5 ~oid:"o1" in
  let t =
    A.apply_sell_fill
      t
      ~price:110.0
      ~qty:0.5
      ~oid:"o2"
      ~buffer:2.0
      ~sell_mult:0.8
      ~oracle_qty:0.1
      ~fees:0.5
      ()
  in
  Alcotest.(check (float 1e-12))
    "fees reduce accumulated profit"
    2.5
    t.A.accumulated_profit;
  Alcotest.(check (float 1e-12)) "reserve still accrues after fees" 0.02 t.A.reserved_base
;;

let ladder_sell t price oid =
  A.apply_sell_fill
    t
    ~price
    ~qty:0.5
    ~oid
    ~buffer:0.0
    ~sell_mult:1.0
    ~oracle_qty:0.0
    ~sell_uses_mult:false
    ~fees:0.0
    ()
;;

let test_ladder_queue_one_cycle_per_buy () =
  (* A ladder can fill several buys before a sell does. Each sell consumes its own lot:
     pairing every sell in the run against the deepest fill would credit the ladder step
     once per sell too many (0.03 here instead of 0.02). *)
  let t = A.apply_buy_fill A.default ~price:10.00 ~qty:0.5 ~oid:"b1" in
  let t = A.apply_buy_fill t ~price:9.98 ~qty:0.5 ~oid:"b2" in
  let t = ladder_sell t 10.00 "s1" in
  Alcotest.(check (float 1e-9)) "first cycle" 0.01 t.A.accumulated_profit;
  let t = ladder_sell t 10.02 "s2" in
  Alcotest.(check (float 1e-9)) "second cycle" 0.02 t.A.accumulated_profit;
  Alcotest.(check int) "no unspent lots" 0 (List.length t.A.buy_lots);
  (* A sell with no unspent buy behind it is a sale of inventory the ledger did not buy as
     a cycle: it must credit nothing. *)
  let t = ladder_sell t 10.04 "s3" in
  Alcotest.(check (float 1e-9)) "no phantom cycle" 0.02 t.A.accumulated_profit
;;

let test_partial_sell_keeps_lot () =
  let t = A.apply_buy_fill A.default ~price:100.0 ~qty:0.5 ~oid:"b1" in
  let t =
    A.apply_sell_fill
      t
      ~price:110.0
      ~qty:0.2
      ~oid:"s1"
      ~buffer:0.0
      ~sell_mult:1.0
      ~oracle_qty:0.0
      ~sell_uses_mult:false
      ~fees:0.0
      ()
  in
  match t.A.buy_lots with
  | [ (p, q) ] ->
    Alcotest.(check (float 1e-12)) "residual lot price" 100.0 p;
    Alcotest.(check (float 1e-12)) "residual lot qty" 0.3 q
  | lots -> Alcotest.failf "expected one residual lot, got %d" (List.length lots)
;;

let test_withheld_slice_leaves_the_lot () =
  (* Kraken withholds (1 - sell_mult) of the oracle qty inside the order. That base was
     never sold, so it must leave the lot too - otherwise it funds a second cycle. *)
  let t = A.apply_buy_fill A.default ~price:100.0 ~qty:0.5 ~oid:"b1" in
  let t =
    A.apply_sell_fill
      t
      ~price:140.0
      ~qty:0.4
      ~oid:"s1"
      ~buffer:0.0
      ~sell_mult:0.8
      ~oracle_qty:0.5
      ~fees:0.0
      ()
  in
  Alcotest.(check (float 1e-12)) "withheld slice reserved" 0.1 t.A.reserved_base;
  Alcotest.(check int) "lot fully consumed" 0 (List.length t.A.buy_lots)
;;

let test_lots_survive_serialization () =
  (* A pre-queue entry resumes with its last buy fill as the only open lot; an entry that
     carries an explicit (even empty) queue is taken at its word, so a spent anchor is
     never resurrected by a restart. *)
  let legacy =
    A.of_json
      (Yojson.Basic.from_string
         {|{"accumulated_profit":1.5,"last_buy_fill_price":100.0,"last_buy_fill_qty":0.2}|})
  in
  Alcotest.(check int) "legacy anchor becomes a lot" 1 (List.length legacy.A.buy_lots);
  let spent =
    A.of_json
      (Yojson.Basic.from_string
         {|{"accumulated_profit":1.5,"last_buy_fill_price":100.0,"last_buy_fill_qty":0.2,
            "buy_lots":[]}|})
  in
  Alcotest.(check int) "empty queue preserved" 0 (List.length spent.A.buy_lots);
  (* A null entry (torn or hand-edited file) must not raise: the store's parse would then
     drop every entry in the file. *)
  let null_entry = A.of_json `Null in
  Alcotest.(check int) "null entry yields no lots" 0 (List.length null_entry.A.buy_lots);
  let two = A.apply_buy_fill legacy ~price:9.98 ~qty:0.5 ~oid:"b2" in
  let round_tripped = A.of_json (A.to_json two) in
  Alcotest.(check int) "newest lot first" 2 (List.length round_tripped.A.buy_lots);
  match round_tripped.A.buy_lots with
  | [ (p1, _); (p2, _) ] ->
    Alcotest.(check (float 1e-12)) "newest price first" 9.98 p1;
    Alcotest.(check (float 1e-12)) "oldest price last" 100.0 p2
  | _ -> Alcotest.fail "expected two lots after round trip"
;;

(* -- Round-trip serialization + persistence ---------------------------- *)

let test_credit_profit_persists_and_adds () =
  (* Carry profit is written straight to the parent strategy's entry: additive, signed,
     deduplicated by recycle sequence, and creates the entry when absent. *)
  with_hermetic_dir (fun _dir ->
    let key = "jacobs_ladder:ETH/USD:kraken" in
    A.save ~key { A.default with A.reserved_base = 0.004; A.accumulated_profit = 0.1 };
    ignore (A.credit_profit ~key ~seq:1 ~qty:0.25);
    ignore (A.credit_profit ~key ~seq:1 ~qty:0.25);
    let t = A.load ~key in
    Alcotest.(check (float 1e-12)) "same seq credited once" 0.35 t.A.accumulated_profit;
    ignore (A.credit_profit ~key ~seq:2 ~qty:0.08);
    ignore (A.credit_profit ~key ~seq:3 ~qty:(-0.03));
    let t = A.load ~key in
    Alcotest.(check (float 1e-12)) "profit and loss applied" 0.4 t.A.accumulated_profit;
    Alcotest.(check int) "highest seq recorded" 3 t.A.carry_credit_seq;
    ignore (A.credit_profit ~key ~seq:2 ~qty:1.0);
    Alcotest.(check (float 1e-12))
      "stale seq ignored"
      0.4
      (A.load ~key).A.accumulated_profit;
    Alcotest.(check (float 1e-12)) "reserved_base untouched" 0.004 t.A.reserved_base;
    let new_key = "jacobs_ladder:SOL/USD:kraken" in
    ignore (A.credit_profit ~key:new_key ~seq:1 ~qty:0.3);
    let fresh = A.load ~key:new_key in
    Alcotest.(check (float 1e-12)) "entry created" 0.3 fresh.A.accumulated_profit;
    Alcotest.(check (float 1e-12)) "fresh reserved_base" 0.0 fresh.A.reserved_base)
;;

let test_accumulation_round_trip () =
  with_hermetic_dir (fun _dir ->
    let key = A.key_of ~strategy:"Ladder" ~symbol:"RT/USDC" ~venue:"hyperliquid" in
    let t =
      { A.reserved_base = 1.25
      ; accumulated_profit = 3.75
      ; carry_credit_seq = 7
      ; last_fill_oid = Some "abc"
      ; last_buy_fill_price = Some 100.0
      ; last_buy_fill_qty = Some 0.5
      ; last_sell_fill_price = None
      ; last_sell_fill_qty = None
      ; buy_lots = [ 100.0, 0.5; 99.0, 0.25 ]
      }
    in
    A.save ~key t;
    let loaded = A.load ~key in
    Alcotest.(check (float 1e-12)) "round-trip reserved_base" 1.25 loaded.A.reserved_base;
    Alcotest.(check (float 1e-12))
      "round-trip accumulated_profit"
      3.75
      loaded.A.accumulated_profit;
    Alcotest.(check int) "round-trip carry seq" 7 loaded.A.carry_credit_seq;
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
    Alcotest.(check int) "round-trip lots" 2 (List.length loaded.A.buy_lots);
    Alcotest.(check (float 1e-12))
      "round-trip newest lot qty"
      0.5
      (match loaded.A.buy_lots with
       | (_, q) :: _ -> q
       | [] -> neg_infinity);
    (* Absent key loads defaults. *)
    let missing = A.load ~key:"Ladder:MISSING:kraken" in
    Alcotest.(check (float 1e-12)) "missing loads default" 0.0 missing.A.reserved_base)
;;

(* [put_async] publishes to the read cache before returning, so the level count is fixed
   by the time the call returns. The poll only exists to report "never landed" instead of
   a level-count mismatch. *)
let wait_for_levels ~key ~expected =
  let rec wait n =
    if List.length (S.load ~key) = expected
    then ()
    else if n = 0
    then failwith "async save never landed"
    else (
      Thread.delay 0.01;
      wait (n - 1))
  in
  wait 500
;;

let test_sell_levels_round_trip_and_adopt () =
  with_hermetic_dir (fun _dir ->
    let key = S.key_of ~strategy:"Ladder" ~symbol:"QQQ" ~venue:"alpaca" in
    S.save_async ~key [ { S.price = 149.0; qty = 0.25 }; { S.price = 150.0; qty = 0.25 } ];
    wait_for_levels ~key ~expected:2;
    let loaded = S.load ~key in
    Alcotest.(check int) "two levels" 2 (List.length loaded);
    Alcotest.(check (float 1e-12)) "sorted desc head" 150.0 (List.hd loaded).S.price;
    (* Adoption deduplicates by price tolerance. *)
    S.adopt_exchange_order ~key { S.price = 149.0; qty = 0.25 };
    Alcotest.(check int) "duplicate adoption ignored" 2 (List.length (S.load ~key));
    S.adopt_exchange_order ~key { S.price = 151.0; qty = 0.25 };
    wait_for_levels ~key ~expected:3;
    Alcotest.(check int) "new level adopted" 3 (List.length (S.load ~key));
    (* Removal drops matching levels only. *)
    S.remove_levels ~key ~levels:[ { S.price = 151.0; qty = 0.25 } ];
    wait_for_levels ~key ~expected:2;
    Alcotest.(check int) "level removed" 2 (List.length (S.load ~key)))
;;

(* The async writer used to re-apply its drained snapshot to the read cache, overwriting
   any [put_async] that landed in between, so a read could return an older snapshot than
   the last save. A round per save gives the writer the chance to interleave.

   Own key: [with_hermetic_dir] points the store at a fresh file but does not reset the
   in-memory cache, so reusing another test's key would start from its levels. *)
let test_async_save_never_regresses_reads () =
  with_hermetic_dir (fun _dir ->
    let key = S.key_of ~strategy:"Ladder" ~symbol:"REGRESS" ~venue:"alpaca" in
    let rounds = 300 in
    let regressions = ref 0 in
    for round = 1 to rounds do
      let price = 100.0 +. float_of_int round in
      S.adopt_exchange_order ~key { S.price; qty = 0.25 };
      let top = List.hd (S.load ~key) in
      if abs_float (top.S.price -. price) > 1e-4 then incr regressions
    done;
    Alcotest.(check int) "reads never regress to an older snapshot" 0 !regressions;
    (* Every round adopted a distinct price, so all of them are still there. *)
    Alcotest.(check int) "every adopted level retained" rounds (List.length (S.load ~key));
    Alcotest.(check (float 1e-12))
      "newest level is the head"
      (100.0 +. float_of_int rounds)
      (List.hd (S.load ~key)).S.price)
;;

let test_salvage_recovers_hand_edited_double_document () =
  (* The exact prod corruption: a manual XMR entry appended as a SECOND top-level document
     joined by a comma instead of being added as another key inside the single object. The
     store must salvage every intact key/object pair into memory, rewrite the live file as
     valid JSON, and quarantine the original - never silently zero the accruals. *)
  with_hermetic_dir (fun dir ->
    let path = Filename.concat dir "accumulation_state.json" in
    let oc = open_out path in
    output_string
      oc
      {|
{
  "Ladder:XMR/USD:kraken": {
   "reserved_base": 0.0,
   "accumulated_profit": 0.0,
   "last_fill_oid": "OUE37M-ABXQZ-FGK7BN",
   "last_buy_fill_price": 410.2,
   "last_buy_fill_qty": 0.04
},
{
  "Ladder:BTC/USDC:hyperliquid": {
    "reserved_base": 0.0006272989999999895,
    "accumulated_profit": 5.626475816000456,
    "last_fill_oid": "523036882052",
    "last_buy_fill_price": 76703.0,
    "last_buy_fill_qty": 0.0005,
    "last_sell_fill_price": 77278.0,
    "last_sell_fill_qty": 0.0005
  }
}
|};
    close_out oc;
    let xmr = A.load ~key:"Ladder:XMR/USD:kraken" in
    Alcotest.(check (option (float 1e-9)))
      "salvaged XMR buy fill price"
      (Some 410.2)
      xmr.A.last_buy_fill_price;
    Alcotest.(check (float 1e-12)) "salvaged XMR reserved_base" 0.0 xmr.A.reserved_base;
    let btc = A.load ~key:"Ladder:BTC/USDC:hyperliquid" in
    Alcotest.(check (float 1e-12))
      "salvaged BTC accumulated_profit"
      5.626475816000456
      btc.A.accumulated_profit;
    (* The cleaned merge was persisted: the live file parses again. *)
    ignore (Yojson.Basic.from_file path);
    let backup_exists =
      List.exists
        (fun f -> String.starts_with ~prefix:"accumulation_state.json.corrupt." f)
        (Array.to_list (Sys.readdir dir))
    in
    Alcotest.(check bool) "original quarantined for audit" true backup_exists)
;;

(* -- Corrupt-file handling & legacy migration --------------------------- *)

let test_corrupt_file_backed_up () =
  with_hermetic_dir (fun dir ->
    let path = Filename.concat dir "accumulation_state.json" in
    let oc = open_out path in
    output_string oc "{ this is not json";
    close_out oc;
    (* load triggers the lazy read; the corrupt file must be backed up, not discarded, and
       the store must start fresh. *)
    let t = A.load ~key:"Ladder:X:kraken" in
    let entries = Array.to_list (Sys.readdir dir) in
    let backup_exists =
      List.exists
        (fun f -> String.starts_with ~prefix:"accumulation_state.json.corrupt." f)
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
    (* Exactly one configured strategy matches the symbol -> auto-mapped to the full
       strategy key. *)
    Dio_persistence.Persistence_orchestrator.register_configured_strategies
      [ "Ladder", "SPCX", "alpaca", true, true ];
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
    let acc_key = A.key_of ~strategy:"Ladder" ~symbol:"SPCX" ~venue:"alpaca" in
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
        ; ( "apply_sell_fill 1:1 accrues no base"
          , `Quick
          , test_apply_sell_fill_one_to_one_no_reserve )
        ; ( "apply_sell_fill fees reduce profit"
          , `Quick
          , test_apply_sell_fill_fees_reduce_profit )
        ; "ladder lots one cycle per buy", `Quick, test_ladder_queue_one_cycle_per_buy
        ; "partial sell keeps the lot", `Quick, test_partial_sell_keeps_lot
        ; "withheld slice leaves the lot", `Quick, test_withheld_slice_leaves_the_lot
        ; "lots survive serialization", `Quick, test_lots_survive_serialization
        ; "credit_profit persists and adds", `Quick, test_credit_profit_persists_and_adds
        ] )
    ; ( "round_trip"
      , [ "accumulation", `Quick, test_accumulation_round_trip
        ; "sell_levels adopt/remove", `Quick, test_sell_levels_round_trip_and_adopt
        ; ( "async save never regresses reads"
          , `Quick
          , test_async_save_never_regresses_reads )
        ] )
    ; ( "corruption"
      , [ ( "hand-edited double document salvaged"
          , `Quick
          , test_salvage_recovers_hand_edited_double_document )
        ; "backed up", `Quick, test_corrupt_file_backed_up
        ] )
    ; "migration", [ "legacy split", `Quick, test_legacy_migration_split ]
    ]
;;
