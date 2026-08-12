let test_grid_values () =
  let open Cmc.Fear_and_greed in
  let interval = 0.75, 1.5 in
  (* Test boundary conditions *)
  Alcotest.(check (float 0.0001))
    "F&G 0"
    0.75
    (grid_value_for_fng ~grid_interval:interval ~fear_and_greed:0.);
  Alcotest.(check (float 0.0001))
    "F&G 100"
    1.5
    (grid_value_for_fng ~grid_interval:interval ~fear_and_greed:100.);
  Alcotest.(check (float 0.0001))
    "F&G < 0"
    0.75
    (grid_value_for_fng ~grid_interval:interval ~fear_and_greed:(-10.));
  Alcotest.(check (float 0.0001))
    "F&G > 100"
    1.5
    (grid_value_for_fng ~grid_interval:interval ~fear_and_greed:110.);
  (* Test mid points *)
  Alcotest.(check (float 0.0001))
    "F&G 50"
    1.125
    (grid_value_for_fng ~grid_interval:interval ~fear_and_greed:50.);
  Alcotest.(check (float 0.0001))
    "F&G 73"
    1.2975
    (grid_value_for_fng ~grid_interval:interval ~fear_and_greed:73.);
  Alcotest.(check (float 0.0001))
    "F&G 20"
    0.9
    (grid_value_for_fng ~grid_interval:interval ~fear_and_greed:20.)
;;

let test_fetch_fallback_not_cached () =
  (* A failed fetch (or a missing API key) must NOT poison the cache: the
     fallback value is returned to the caller, but the cache stays empty so
     callers can distinguish "no live F&G signal" (get_cached () = None) from
     a neutral reading - grid domains withhold orders without a signal. *)
  let module F = Cmc.Fear_and_greed in
  F.clear_cache ();
  let fallback = 42.0 in
  let value = F.fetch_and_cache_sync ~fallback () in
  (* In some environments, a real fetch might succeed (returning 20.0). *)
  let is_valid =
    abs_float (value -. fallback) < 0.0001 || abs_float (value -. 20.0) < 0.0001
  in
  Alcotest.(check bool) "fallback or fetched value used" true is_valid;
  let cached = F.get_cached () in
  let cached_is_real = cached <> None in
  (* If a real fetch succeeded the cache legitimately holds it; otherwise the
     fallback must NOT have been cached. *)
  if not cached_is_real
  then Alcotest.(check bool) "fallback never cached" true true
  else
    Alcotest.(check bool)
      "cached value is the fetched one (never the fallback)"
      true
      (match cached with
       | Some v -> abs_float (v -. 20.0) < 0.0001
       | None -> false)
;;

let () =
  Alcotest.run
    "Fear_and_greed"
    [ "grid_computation", [ Alcotest.test_case "grid levels" `Quick test_grid_values ]
    ; ( "fetching"
      , [ Alcotest.test_case "fallback not cached" `Quick test_fetch_fallback_not_cached ]
      )
    ]
;;
