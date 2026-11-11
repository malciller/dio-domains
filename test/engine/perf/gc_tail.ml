(* Tail-latency harness for the per-cycle allocation burst (the PREP phase). Models the
   live shape measured in production:
   - a large retained major-heap live set (default 180 MB total across domains),
   - N trading domains each allocating a ~4.6 kword burst per cycle,
   - an event-driven cycle rate (~350/s) with the domain idle between bursts.

   The metric is the wall time of one allocation burst. When a minor/major collection
   lands inside the burst it shows up as a p99/max spike, which is exactly the PREP tail
   seen in production.

   Env: DIO_TAIL_DOMAINS domains (default 6) DIO_TAIL_LIVE_MB retained live set, total MB
   (default 180) DIO_TAIL_HZ cycles/sec/domain (default 350) DIO_TAIL_SECS run seconds
   (default 30) DIO_TAIL_WORDS allocated words per burst (default 4600) DIO_TAIL_MINOR
   minor_heap_size words (default 8388608 = 64 MB) DIO_TAIL_OVERHEAD space_overhead
   (default 120) DIO_TAIL_SAFEPOINT 1 = force minor GC at cycle end when pressure high
   DIO_TAIL_SP_WORDS safepoint threshold in minor words (default 2097152 = 16 MB)
*)

[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let env_int name d =
  match Sys.getenv_opt name with
  | Some s ->
    (try int_of_string (String.trim s) with
     | _ -> d)
  | None -> d
;;

let now_ns = Mtime_clock.now_ns

(* Retained live set: many small strings in a hashtable, promoted to the major heap so the
   collector has real live data to scan. *)
let make_live_set words =
  let tbl = Hashtbl.create 65_536 in
  (* ~40 words per entry including the string payload. *)
  let n = max 1 (words / 40) in
  for i = 1 to n do
    Hashtbl.replace tbl i (Printf.sprintf "live-%d-%d" i (i * 7))
  done;
  tbl
;;

(* Allocate a transient structure of roughly [words] words, then drop it. *)
let allocate_burst words =
  let per_item = 12 in
  let n = max 1 (words / per_item) in
  let rec loop i acc =
    if i > n then acc else loop (i + 1) ((i, float_of_int i, "x") :: acc)
  in
  let l = loop 1 [] in
  ignore (List.length l)
;;

type result =
  { mutable samples : int
  ; mutable max_ns : int64
  ; mutable over_200 : int
  ; mutable over_500 : int
  ; buckets : int array (* 10us buckets up to 1ms *)
  }

let make_result () =
  { samples = 0; max_ns = 0L; over_200 = 0; over_500 = 0; buckets = Array.make 100 0 }
;;

let record r ns =
  r.samples <- r.samples + 1;
  if Int64.compare ns r.max_ns > 0 then r.max_ns <- ns;
  let us = Int64.to_int ns / 1000 in
  if us >= 200 then r.over_200 <- r.over_200 + 1;
  if us >= 500 then r.over_500 <- r.over_500 + 1;
  let b = min 99 (us / 10) in
  r.buckets.(b) <- r.buckets.(b) + 1
;;

let percentile r p =
  let target = int_of_float (float r.samples *. p) in
  let acc = ref 0 in
  let res = ref 0 in
  (try
     Array.iteri
       (fun i c ->
         acc := !acc + c;
         if !acc >= target
         then (
           res := i * 10;
           raise Exit))
       r.buckets
   with
   | Exit -> ());
  !res
;;

let worker ~hz ~words ~secs ~warmup ~live_words ~safepoint ~sp_words _id =
  let live = make_live_set live_words in
  let period_ns = Int64.of_float (1e9 /. float_of_int hz) in
  let r = make_result () in
  let start = now_ns () in
  let last = ref start in
  let last_minor = ref (Gc.minor_words ()) in
  let warm_deadline = Int64.add start (Int64.of_float (float_of_int warmup *. 1e9)) in
  let deadline = Int64.add warm_deadline (Int64.of_float (float_of_int secs *. 1e9)) in
  let stop = ref false in
  let warming = ref true in
  while not !stop do
    let t0 = now_ns () in
    allocate_burst words;
    let t1 = now_ns () in
    if not !warming then record r (Int64.sub t1 t0);
    if safepoint
    then (
      let mw = Gc.minor_words () in
      if mw -. !last_minor >= float_of_int sp_words
      then (
        Gc.minor ();
        last_minor := Gc.minor_words ()))
    else last_minor := Gc.minor_words ();
    (* Idle until the next cycle tick, like the event-driven loop. *)
    let next = Int64.add !last period_ns in
    let now = now_ns () in
    if Int64.compare now next < 0
    then (
      let sleep_s = Int64.to_float (Int64.sub next now) /. 1e9 in
      ignore (Unix.select [] [] [] sleep_s))
    else last := now;
    let now2 = now_ns () in
    if !warming && Int64.compare now2 warm_deadline >= 0 then warming := false;
    if Int64.compare now2 deadline > 0 then stop := true
  done;
  ignore (Sys.opaque_identity live);
  r
;;

let () =
  let domains = env_int "DIO_TAIL_DOMAINS" 6 in
  let live_mb = env_int "DIO_TAIL_LIVE_MB" 180 in
  let hz = env_int "DIO_TAIL_HZ" 350 in
  let secs = env_int "DIO_TAIL_SECS" 30 in
  let warmup = env_int "DIO_TAIL_WARMUP" 3 in
  let words = env_int "DIO_TAIL_WORDS" 4600 in
  let minor = env_int "DIO_TAIL_MINOR" 8_388_608 in
  let overhead = env_int "DIO_TAIL_OVERHEAD" 120 in
  let safepoint = env_int "DIO_TAIL_SAFEPOINT" 0 = 1 in
  let sp_words = env_int "DIO_TAIL_SP_WORDS" 2_097_152 in
  let gc = Gc.get () in
  (* Match production: compaction disabled, fixed 8 MB heap increments. *)
  Gc.set
    { gc with
      minor_heap_size = minor
    ; space_overhead = overhead
    ; max_overhead = 1_000_000
    ; major_heap_increment = 1_048_576
    };
  (* One live set per domain; the major heap is shared, so this is the total. *)
  let live_words = live_mb * 1024 * 1024 / 8 / domains in
  Printf.printf
    "domains=%d live_total=%dMB hz=%d words=%d minor=%d(%dMB) overhead=%d safepoint=%b \
     sp_words=%d secs=%d\n\
     %!"
    domains
    live_mb
    hz
    words
    minor
    (minor * 8 / 1_048_576)
    overhead
    safepoint
    sp_words
    secs;
  let handles =
    List.init domains (fun id ->
      Domain.spawn (fun () ->
        worker ~hz ~words ~secs ~warmup ~live_words ~safepoint ~sp_words id))
  in
  let results = List.map Domain.join handles in
  let total = List.fold_left (fun a r -> a + r.samples) 0 results in
  let max_ns = List.fold_left (fun a r -> Int64.max a r.max_ns) 0L results in
  let over_200 = List.fold_left (fun a r -> a + r.over_200) 0 results in
  let over_500 = List.fold_left (fun a r -> a + r.over_500) 0 results in
  (* Merge histograms for a combined p99/p999. *)
  let merged = Array.make 100 0 in
  List.iter
    (fun r -> Array.iteri (fun i c -> merged.(i) <- merged.(i) + c) r.buckets)
    results;
  let mr = { samples = total; max_ns; over_200; over_500; buckets = merged } in
  Printf.printf
    "RESULT samples=%d p99=%dus p999=%dus max=%Ldus over200=%d over500=%d\n%!"
    total
    (percentile mr 0.99)
    (percentile mr 0.999)
    (Int64.div max_ns 1000L)
    over_200
    over_500
;;
