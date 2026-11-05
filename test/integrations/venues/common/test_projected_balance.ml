let near msg a b = Alcotest.(check (float 0.000001)) msg a b

let test_compose () =
  (* Gross total minus our own holds: a cancel raises it before the venue's frame does. *)
  near "no holds" 100.0 (Projected_balance.compose ~total:100.0 ~projected:0.0 ~held:0.0);
  near
    "a resting buy is netted"
    88.13
    (Projected_balance.compose ~total:100.0 ~projected:0.0 ~held:11.87);
  near
    "a fill credit lands on top"
    90.13
    (Projected_balance.compose ~total:100.0 ~projected:2.0 ~held:11.87);
  near
    "clamped at zero"
    0.0
    (Projected_balance.compose ~total:1.0 ~projected:0.0 ~held:5.0)
;;

let test_apply_dedupes_by_id () =
  let t = Projected_balance.create ~label:"test/projected" () in
  Projected_balance.apply t ~delta:2.5 ~id:"fill-1";
  near "first application lands" 2.5 (Projected_balance.get t);
  Projected_balance.apply t ~delta:2.5 ~id:"fill-1";
  near "replay is ignored" 2.5 (Projected_balance.get t);
  Projected_balance.apply t ~delta:1.0 ~id:"fill-2";
  near "distinct ids accumulate" 3.5 (Projected_balance.get t);
  Projected_balance.apply t ~delta:1.0 ~id:"";
  near "empty id is not deduped and is not recorded" 4.5 (Projected_balance.get t)
;;

let test_frame_clears_projection () =
  let t = Projected_balance.create ~label:"test/projected-clear" () in
  Projected_balance.apply t ~delta:2.5 ~id:"fill-a";
  Projected_balance.clear t;
  near "a balance frame supersedes projected fills" 0.0 (Projected_balance.get t)
;;

let test_ttl_expires_a_projection_without_a_frame () =
  let t = Projected_balance.create ~label:"test/projected-ttl" () in
  Projected_balance.apply t ~delta:2.5 ~id:"fill-b";
  let now = Unix.time () in
  near "fresh projection" 2.5 (Projected_balance.get ~now t);
  near
    "stale projection ages out"
    0.0
    (Projected_balance.get ~now:(now +. Projected_balance.ttl_s +. 1.0) t)
;;

let test_non_finite_deltas_ignored () =
  let t = Projected_balance.create ~label:"test/projected-nan" () in
  Projected_balance.apply t ~delta:Float.nan ~id:"fill-nan";
  Projected_balance.apply t ~delta:Float.infinity ~id:"fill-inf";
  Projected_balance.apply t ~delta:0.0 ~id:"fill-zero";
  near "nothing applied" 0.0 (Projected_balance.get t)
;;

let () =
  Alcotest.run
    "Projected balance"
    [ ( "projection"
      , [ Alcotest.test_case "apply dedupes by id" `Quick test_apply_dedupes_by_id
        ; Alcotest.test_case
            "a balance frame clears it"
            `Quick
            test_frame_clears_projection
        ; Alcotest.test_case
            "ttl expires it"
            `Quick
            test_ttl_expires_a_projection_without_a_frame
        ; Alcotest.test_case
            "non-finite and zero deltas are ignored"
            `Quick
            test_non_finite_deltas_ignored
        ] )
    ; "compose", [ Alcotest.test_case "tradeable arithmetic" `Quick test_compose ]
    ]
;;
