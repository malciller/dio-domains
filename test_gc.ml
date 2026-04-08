let () =
  Gc.set { (Gc.get ()) with Gc.minor_heap_size = 33554432; Gc.space_overhead = 2000 };
  let g_main = Gc.get () in
  Printf.printf "Main Domain: minor_heap_size=%d space_overhead=%d\n" g_main.minor_heap_size g_main.space_overhead;
  let d = Domain.spawn (fun () ->
    let g = Gc.get () in
    Printf.printf "Spawned Domain: minor_heap_size=%d space_overhead=%d\n" g.minor_heap_size g.space_overhead;
  ) in
  Domain.join d
