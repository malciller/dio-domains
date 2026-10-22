(* Alpaca REST submission guards: the venue rejects limit prices off the valid tick band
   with HTTP 422 ("sub-penny increment does not fulfill minimum pricing criteria"). Prices
   >= $1.00 must land on penny increments; sub-penny ($0.0001) is only valid below $1.00. *)

let near ?(eps = 1e-9) a b = Alcotest.(check (float eps)) "approx" a b

let test_rounds_above_one_dollar_to_penny () =
  (* The production failure: grid level 710.8678 must not reach the wire. *)
  near 710.87 (Alpaca.Rest.round_limit_price 710.8678);
  near 100.0 (Alpaca.Rest.round_limit_price 99.999999);
  near 2.55 (Alpaca.Rest.round_limit_price 2.5468)
;;

let test_keeps_sub_penny_below_one_dollar () =
  near 0.8678 (Alpaca.Rest.round_limit_price 0.8678);
  near 0.0001 (Alpaca.Rest.round_limit_price 0.00014);
  near 0.9999 (Alpaca.Rest.round_limit_price 0.99994)
;;

let test_exact_prices_pass_through () =
  near 710.87 (Alpaca.Rest.round_limit_price 710.87);
  near 1.0 (Alpaca.Rest.round_limit_price 1.0);
  near 133.94 (Alpaca.Rest.round_limit_price 133.94)
;;

let test_non_finite_pass_through () =
  Alcotest.(check bool)
    "nan preserved"
    (Float.is_nan (Alpaca.Rest.round_limit_price Float.nan))
    true;
  Alcotest.(check bool)
    "infinity preserved"
    (Float.is_infinite (Alpaca.Rest.round_limit_price Float.infinity))
    true
;;

(* Transport failures (timeout / connection) must be retriable; venue rejections and parse
   errors must not, so a permanent failure fails fast instead of sleeping through the
   backoff. *)
let test_retriable_classification () =
  let check name expected err =
    Alcotest.(check bool) name expected (Alpaca.Rest.is_retriable_http_exception err)
  in
  check "timeout retriable" true "Get positions HTTP exception: Lwt_unix.Timeout";
  check
    "connection retriable"
    true
    "Get open orders HTTP exception: Unix.Unix_error(ECONNRESET, \"read\", \"\")";
  check
    "connect refused retriable"
    true
    "Get account HTTP exception: Unix.Unix_error(Unix.ECONNREFUSED, \"connect\", \"\")";
  check
    "os connect timeout retriable"
    true
    "Get account HTTP exception: Unix.Unix_error(Unix.ETIMEDOUT, \"connect\", \"\")";
  check
    "host unreachable retriable"
    true
    "Get account HTTP exception: Unix.Unix_error(Unix.EHOSTUNREACH, \"connect\", \"\")";
  check "422 terminal" false "HTTP 422 getting positions: sub-penny";
  check "parse terminal" false "Failed to parse positions response: bad json"
;;

(* The retry loop must actually re-invoke [f] on a timeout. Regression guard: previously
   the lambdas re-raised transport exceptions, which bypassed [retry_with_backoff]
   entirely, so timeouts were never retried. *)
let test_retry_retries_timeout () =
  let attempts = ref 0 in
  let result =
    Lwt_main.run
      (Alpaca.Rest.retry_http_exceptions ~f:(fun () ->
         incr attempts;
         if !attempts = 1
         then Lwt.return (Error "Get positions HTTP exception: Lwt_unix.Timeout")
         else Lwt.return (Ok "recovered")))
  in
  Alcotest.(check int) "retried once" 2 !attempts;
  match result with
  | Ok s -> Alcotest.(check string) "recovered value" "recovered" s
  | Error e -> Alcotest.failf "expected Ok, got Error %s" e
;;

(* A terminal rejection must be returned after a single attempt, with no backoff sleep. *)
let test_retry_stops_on_rejection () =
  let attempts = ref 0 in
  let result =
    Lwt_main.run
      (Alpaca.Rest.retry_http_exceptions ~f:(fun () ->
         incr attempts;
         Lwt.return (Error "HTTP 422 getting positions: sub-penny")))
  in
  Alcotest.(check int) "attempted once" 1 !attempts;
  match result with
  | Ok _ -> Alcotest.fail "expected Error"
  | Error _ -> ()
;;

(* A repeated DELETE against an order that is already cancelling is Alpaca's
   [{"code":42210000,"message":"order pending cancel"}]. The first cancel was accepted, so
   the retry is an idempotent success; treating it as an error produced recurring ERROR
   alerts on every fractional-order amendment. *)
let test_cancel_pending_classification () =
  let check name expected body =
    Alcotest.(check bool) name expected (Alpaca.Rest.is_cancel_pending_response body)
  in
  check "pending cancel" true "{\"code\":42210000,\"message\":\"order pending cancel\"}";
  check "pending_cancel snake case" true "order is pending_cancel";
  check "uppercase" true "ORDER PENDING CANCEL";
  check "genuine rejection" false "{\"code\":40310000,\"message\":\"insufficient qty\"}";
  check
    "already filled"
    false
    "{\"code\":42210000,\"message\":\"order is already filled\"}"
;;

let () =
  Alcotest.run
    "alpaca_rest"
    [ ( "tick rounding"
      , [ Alcotest.test_case
            ">= $1 rounds to penny"
            `Quick
            test_rounds_above_one_dollar_to_penny
        ; Alcotest.test_case
            "< $1 keeps sub-penny"
            `Quick
            test_keeps_sub_penny_below_one_dollar
        ; Alcotest.test_case "on-tick pass-through" `Quick test_exact_prices_pass_through
        ; Alcotest.test_case "non-finite pass-through" `Quick test_non_finite_pass_through
        ] )
    ; ( "cancel"
      , [ Alcotest.test_case
            "pending cancel is idempotent"
            `Quick
            test_cancel_pending_classification
        ] )
    ; ( "retry policy"
      , [ Alcotest.test_case "classification" `Quick test_retriable_classification
        ; Alcotest.test_case "retries timeout" `Quick test_retry_retries_timeout
        ; Alcotest.test_case "stops on rejection" `Quick test_retry_stops_on_rejection
        ] )
    ]
;;
