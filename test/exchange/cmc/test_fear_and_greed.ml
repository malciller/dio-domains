let test_grid_index_and_values () =
  let open Cmc.Fear_and_greed in
  Alcotest.(check int) "very low" 0 (grid_index_of_fng 10.);
  Alcotest.(check int) "low" 1 (grid_index_of_fng 30.);
  Alcotest.(check int) "mid" 2 (grid_index_of_fng 55.);
  Alcotest.(check int) "high" 3 (grid_index_of_fng 70.);
  Alcotest.(check int) "very high" 4 (grid_index_of_fng 90.);

  let levels = compute_grid_levels (0.75, 1.5) in
  Alcotest.(check (float 0.0001)) "level0" 0.75 levels.(0);
  Alcotest.(check (float 0.0001)) "level4" 1.5 levels.(4);

  let value = grid_value_for_fng ~grid_interval:(0.75, 1.5) ~fear_and_greed:73. in
  Alcotest.(check (float 0.0001)) "value at idx3" 1.3125 value

let test_fetch_fallback_sets_cache () =
  let module F = Cmc.Fear_and_greed in
  F.clear_cache ();
  let fallback = 42.0 in
  let value = F.fetch_and_cache_sync ~fallback () in
  Alcotest.(check (float 0.0001)) "fallback used" fallback value;
  Alcotest.(check (option (float 0.0001))) "cached set" (Some fallback) (F.get_cached ())

let () =
  Alcotest.run "Fear_and_greed" [
    ("grid_computation", [ Alcotest.test_case "grid levels" `Quick test_grid_index_and_values ]);
    ("fetching", [ Alcotest.test_case "fallback cache" `Quick test_fetch_fallback_sets_cache ]);
  ]
