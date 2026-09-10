(* Raster framebuffer tests: dimensions, write/blend semantics, AA line
   coverage, and the half-block sink geometry. *)

open Dashboard_ui

let test_create_and_set () =
  let t = Raster.create 4 3 in
  Alcotest.(check int) "width" 4 (Raster.width t);
  Alcotest.(check int) "height" 3 (Raster.height t);
  Raster.set t 1 2 (1.0, 0.5, 0.25);
  let r, g, b = Raster.get t 1 2 in
  Alcotest.(check (float 1e-9)) "r" 1.0 r;
  Alcotest.(check (float 1e-9)) "g" 0.5 g;
  Alcotest.(check (float 1e-9)) "b" 0.25 b;
  (* Out-of-bounds writes are ignored and reads return black. *)
  Raster.set t 10 10 (1.0, 1.0, 1.0);
  let r, g, b = Raster.get t 10 10 in
  Alcotest.(check (float 1e-9)) "oob r" 0.0 r;
  Alcotest.(check (float 1e-9)) "oob g" 0.0 g;
  Alcotest.(check (float 1e-9)) "oob b" 0.0 b
;;

let test_blend () =
  let t = Raster.create 1 1 in
  Raster.set t 0 0 (0.0, 0.0, 0.0);
  Raster.blend t 0 0 (1.0, 1.0, 1.0) 0.5;
  let r, _, _ = Raster.get t 0 0 in
  Alcotest.(check (float 1e-9)) "half blend" 0.5 r;
  Raster.blend t 0 0 (1.0, 1.0, 1.0) 0.5;
  let r, _, _ = Raster.get t 0 0 in
  Alcotest.(check (float 1e-9)) "second half blend" 0.75 r
;;

let test_line_coverage () =
  let t = Raster.create 11 3 in
  Raster.line t (0, 0) (10, 0) (1.0, 1.0, 1.0);
  (* A horizontal line through y=0 must light up every pixel on that row and
     leave the other rows black. *)
  let row0 =
    List.init 11 (fun x ->
      let r, _, _ = Raster.get t x 0 in
      r >= 0.49)
  in
  Alcotest.(check bool) "row 0 fully lit" true (List.for_all (fun b -> b) row0);
  let row1 =
    List.init 11 (fun x ->
      let r, _, _ = Raster.get t x 1 in
      r > 0.001)
  in
  Alcotest.(check bool) "row 1 dark" true (List.for_all (fun b -> not b) row1)
;;

let test_to_blocks_geometry () =
  let t = Raster.create 7 5 in
  Raster.fill t (0.2, 0.4, 0.6);
  let img = Raster.to_blocks t in
  Alcotest.(check int) "cell width" 7 (Notty.I.width img);
  Alcotest.(check int) "cell height ceil(h/2)" 3 (Notty.I.height img)
;;

let () =
  Alcotest.run
    "dashboard_raster"
    [ ( "raster"
      , [ Alcotest.test_case "create and set" `Quick test_create_and_set
        ; Alcotest.test_case "blend coverage" `Quick test_blend
        ; Alcotest.test_case "line coverage" `Quick test_line_coverage
        ; Alcotest.test_case "half-block geometry" `Quick test_to_blocks_geometry
        ] )
    ]
;;
