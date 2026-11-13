(** OxCaml hot-path shadow feed.

    The legacy engine keeps trading. This module consumes one venue's raw top-of-book
    frames through a single-producer/single-consumer ring and runs a zero-allocation
    decode/apply on a dedicated domain. It exists to measure the replacement feed path on
    production data before the strategy and order layers move.

    Producers: the venue WebSocket read thread (single writer). Consumer: one pinned
    domain, busy-polling the ring. No locks, no condition variables, no per-frame heap
    allocation on the consumer path. *)

[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let section = "hotpath"

type slot =
  { mutable payload : string
  ; mutable recv_ns : int
  }

type ring =
  { slots : slot array
  ; mask : int
  ; head : int Atomic.t
  ; tail : int Atomic.t
  ; drops : int Atomic.t
  ; mutable current_payload : string
  ; mutable current_recv : int
  }

let create_ring capacity =
  let rec pow2 n = if n >= capacity then n else pow2 (n lsl 1) in
  let cap = pow2 1 in
  { slots = Array.init cap (fun _ -> { payload = ""; recv_ns = 0 })
  ; mask = cap - 1
  ; head = Atomic.make 0
  ; tail = Atomic.make 0
  ; drops = Atomic.make 0
  ; current_payload = ""
  ; current_recv = 0
  }
;;

let[@inline] push ring ~payload ~recv_ns =
  let tail = Atomic.get ring.tail in
  let head = Atomic.get ring.head in
  if tail - head >= Array.length ring.slots
  then (
    ignore (Atomic.fetch_and_add ring.drops 1);
    false)
  else (
    let slot = ring.slots.(tail land ring.mask) in
    slot.payload <- payload;
    slot.recv_ns <- recv_ns;
    Atomic.set ring.tail (tail + 1);
    true)
;;

(* Single consumer only: the consumer copies the fields before advancing [head], so the
   producer can reuse the slot as soon as [head] moves. *)
let[@inline] try_pop ring =
  let head = Atomic.get ring.head in
  if head = Atomic.get ring.tail
  then false
  else (
    let slot = ring.slots.(head land ring.mask) in
    ring.current_payload <- slot.payload;
    ring.current_recv <- slot.recv_ns;
    Atomic.set ring.head (head + 1);
    true)
;;

(* Manual scanners: the stdlib [_opt] variants allocate an option per call, which would
   put a block on the hot path for every frame. *)
let[@inline] find_char s c from =
  let n = String.length s in
  let rec loop i =
    if i >= n then -1 else if String.unsafe_get s i = c then i else loop (i + 1)
  in
  loop from
;;

let[@inline] starts_with_at s key i =
  let n = String.length key in
  i + n <= String.length s
  &&
  let rec loop j =
    j >= n || (String.unsafe_get s (i + j) = String.unsafe_get key j && loop (j + 1))
  in
  loop 0
;;

let find_sub s key from =
  let n = String.length s in
  let k = String.length key in
  let rec loop i =
    if i + k > n then -1 else if starts_with_at s key i then i else loop (i + 1)
  in
  loop from
;;

let coin_matches ~target s =
  let key = "\"coin\":\"" in
  match find_sub s key 0 with
  | -1 -> false
  | at ->
    let start = at + String.length key in
    let stop = find_char s '"' start in
    if stop < 0
    then false
    else (
      let len = stop - start in
      len = String.length target
      &&
      let rec eq j =
        j >= len
        || (String.unsafe_get s (start + j) = String.unsafe_get target j && eq (j + 1))
      in
      eq 0)
;;

type parser =
  { mutable ks : int
  ; mutable ke : int
  }

let create_parser () = { ks = 0; ke = 0 }

let find_key p s key from limit =
  let rec search i =
    let q = find_char s '"' i in
    if q < 0 || q >= limit
    then false
    else if starts_with_at s key q
    then (
      let vs = q + String.length key in
      let ve = find_char s '"' vs in
      if ve < 0
      then false
      else (
        p.ks <- vs;
        p.ke <- ve;
        true))
    else search (q + 1)
  in
  search from
;;

let parse_float_fast s start_idx end_idx =
  let len = end_idx - start_idx in
  if len = 0
  then 0.0
  else (
    let sign, start_idx =
      if String.unsafe_get s start_idx = '-' then -1.0, start_idx + 1 else 1.0, start_idx
    in
    let rec loop i acc dec frac_div =
      if i = end_idx
      then acc /. frac_div
      else (
        let c = String.unsafe_get s i in
        if c = '.'
        then loop (i + 1) acc true 1.0
        else if c >= '0' && c <= '9'
        then (
          let d = float_of_int (Char.code c - 48) in
          if dec
          then loop (i + 1) ((acc *. 10.0) +. d) true (frac_div *. 10.0)
          else loop (i + 1) ((acc *. 10.0) +. d) false frac_div)
        else loop (i + 1) acc dec frac_div)
    in
    sign *. loop start_idx 0.0 false 1.0)
;;

type tob =
  { mutable bid_px : float
  ; mutable bid_sz : float
  ; mutable ask_px : float
  ; mutable ask_sz : float
  ; mutable ok : bool
  }

let create_tob () = { bid_px = 0.0; bid_sz = 0.0; ask_px = 0.0; ask_sz = 0.0; ok = false }

(** Depth-1 top-of-book from a Hyperliquid l2Book frame. Layout:
    ["levels":[[{px,sz}...],[{px,sz}...]]]. Uses the parser's scratch fields, so it
    allocates nothing. *)
let parse_tob_into p t s =
  t.ok <- false;
  let levels = find_sub s "\"levels\":[[" 0 in
  if levels >= 0
  then (
    let bids_start = levels + 11 in
    let bids_end = find_char s ']' bids_start in
    if bids_end >= 0 && find_key p s "\"px\":\"" bids_start bids_end
    then (
      let bpx_s = p.ks in
      let bpx_e = p.ke in
      if find_key p s "\"sz\":\"" (bpx_e + 1) bids_end
      then (
        let bsz_s = p.ks in
        let bsz_e = p.ke in
        let asks_start = bids_end + 2 in
        let asks_end = find_char s ']' asks_start in
        if asks_end >= 0 && find_key p s "\"px\":\"" asks_start asks_end
        then (
          let apx_s = p.ks in
          let apx_e = p.ke in
          if find_key p s "\"sz\":\"" (apx_e + 1) asks_end
          then (
            t.bid_px <- parse_float_fast s bpx_s bpx_e;
            t.bid_sz <- parse_float_fast s bsz_s bsz_e;
            t.ask_px <- parse_float_fast s apx_s apx_e;
            t.ask_sz <- parse_float_fast s p.ks p.ke;
            t.ok <- true)))))
;;

type book =
  { mutable bid_px : float
  ; mutable bid_sz : float
  ; mutable ask_px : float
  ; mutable ask_sz : float
  ; mutable updates : int
  }

let create_book () =
  { bid_px = 0.0; bid_sz = 0.0; ask_px = 0.0; ask_sz = 0.0; updates = 0 }
;;

let enabled = Atomic.make false
let target_coin = ref "BTC"
let ring_ref : ring option ref = ref None
let frames = Atomic.make 0
let book = create_book ()

let e2e_prof =
  Latency_profiler.create
    ~shared:true
    ~bucket_us:1_000
    ~max_latency_us:60_000_000
    "hotpath:e2e"
;;

let decode_prof =
  Latency_profiler.create
    ~shared:true
    ~bucket_us:1
    ~max_latency_us:10_000
    "hotpath:decode"
;;

let book_prof =
  Latency_profiler.create ~shared:true ~bucket_us:1 ~max_latency_us:10_000 "hotpath:book"
;;

(** Called from the venue WebSocket read thread for every raw text frame. No-op unless the
    shadow is enabled and the frame is the target coin's l2Book. The payload string is the
    frame already allocated by the reader; the ring stores the reference, not a copy. *)
let submit ~recv_ns payload =
  if Atomic.get enabled
  then (
    match !ring_ref with
    | None -> ()
    | Some ring ->
      if coin_matches ~target:!target_coin payload
      then ignore (push ring ~payload ~recv_ns : bool))
;;

let consume ring =
  let p = create_parser () in
  let t = create_tob () in
  let rec loop idle =
    if try_pop ring
    then (
      let payload = ring.current_payload in
      let recv_ns = ring.current_recv in
      let t0 = Monotonic_clock.now_ns () in
      parse_tob_into p t payload;
      let t1 = Monotonic_clock.now_ns () in
      if t.ok
      then (
        book.bid_px <- t.bid_px;
        book.bid_sz <- t.bid_sz;
        book.ask_px <- t.ask_px;
        book.ask_sz <- t.ask_sz;
        book.updates <- book.updates + 1);
      let t2 = Monotonic_clock.now_ns () in
      Latency_profiler.record_shared_ns e2e_prof (t2 - recv_ns);
      Latency_profiler.record_shared_ns decode_prof (t1 - t0);
      Latency_profiler.record_shared_ns book_prof (t2 - t1);
      ignore (Atomic.fetch_and_add frames 1);
      loop 0)
    else if idle >= 4096
    then (
      (* A shadow feed has no latency contract: park between bursts instead of pinning a
         background core for a mirror nobody trades on. *)
      Thread.delay 0.0002;
      loop 0)
    else (
      Domain.cpu_relax ();
      loop (idle + 1))
  in
  loop 0
;;

let show_p99 name = function
  | None -> Printf.sprintf "%s=n/a" name
  | Some (s : Latency_profiler.snapshot) ->
    Printf.sprintf
      "%s p50=%s p99=%s max=%s"
      name
      (Latency_profiler.format_us s.p50)
      (Latency_profiler.format_us s.p99)
      (Latency_profiler.format_us s.max_us)
;;

let metrics_loop ring =
  while true do
    Unix.sleepf 5.0;
    Latency_profiler.rotate e2e_prof;
    Latency_profiler.rotate decode_prof;
    Latency_profiler.rotate book_prof;
    let e2e = Latency_profiler.drain e2e_prof in
    let dec = Latency_profiler.drain decode_prof in
    let bk = Latency_profiler.drain book_prof in
    Logging.info_f
      ~section
      "shadow %s frames=%d drops=%d updates=%d | %s | %s | %s"
      !target_coin
      (Atomic.get frames)
      (Atomic.get ring.drops)
      book.updates
      (show_p99 "e2e" e2e)
      (show_p99 "decode" dec)
      (show_p99 "book" bk)
  done
;;

(** Start the shadow when [DIO_HOTPATH_SHADOW=1]. Target coin is [DIO_HOTPATH_COIN]
    (default [BTC]). With the flag off this is a single log line and no domain. *)
let start () =
  match Sys.getenv_opt "DIO_HOTPATH_SHADOW" with
  | Some ("1" | "true" | "on" | "yes") ->
    (target_coin
     := match Sys.getenv_opt "DIO_HOTPATH_COIN" with
        | Some c when c <> "" -> c
        | _ -> "BTC");
    let ring = create_ring 8192 in
    ring_ref := Some ring;
    Atomic.set enabled true;
    ignore
      (Domain.spawn (fun () ->
         Thread_affinity.pin_self_background ();
         consume ring));
    ignore
      (Domain.spawn (fun () ->
         Thread_affinity.pin_self_background ();
         metrics_loop ring));
    Logging.info_f
      ~section
      "shadow enabled: coin=%s ring=%d slots (legacy engine keeps trading)"
      !target_coin
      (Array.length ring.slots)
  | _ -> Logging.info_f ~section "shadow disabled (DIO_HOTPATH_SHADOW != 1)"
;;
