(* Live repro harness: replays Kraken's real book feed through the same
   code paths production uses (parse_and_apply_levels, levels_to_array,
   calculate_checksum) and validates the CRC32 on EVERY message to bisect
   whether snapshots validate immediately or drift begins with deltas. *)

let _section = "debug_ws"

type state =
  { bids : (string, Kraken.Kraken_orderbook_feed.level) Hashtbl.t
  ; asks : (string, Kraken.Kraken_orderbook_feed.level) Hashtbl.t
  ; mutable seq : Int64.t option
  ; mutable updates_since_snapshot : int
  ; mutable validated : int
  ; mutable mismatched : int
  ; mutable snapshot_ok : bool
  ; recent_deltas : string Queue.t
    (* Raw JSON of the last deltas, dumped on first mismatch. *)
  ; mutable first_mismatch_dumped : bool
  }

let states : (string, state) Hashtbl.t = Hashtbl.create 8
let symbols = [ "BTC/USD"; "SOL/USD"; "ADA/USD" ]

let get_state symbol =
  match Hashtbl.find_opt states symbol with
  | Some s -> s
  | None ->
    let s =
      { bids = Hashtbl.create 64
      ; asks = Hashtbl.create 64
      ; seq = None
      ; updates_since_snapshot = 0
      ; validated = 0
      ; mismatched = 0
      ; snapshot_ok = false
      ; recent_deltas = Queue.create ()
      ; first_mismatch_dumped = false
      }
    in
    Hashtbl.replace states symbol s;
    s
;;

let dump_levels label arr =
  Printf.printf "    %s:\n" label;
  Array.iteri
    (fun i lvl ->
       if i < 10
       then
         Printf.printf
           "      [%d] key=%s wire=%s size=%s\n"
           i
           lvl.Kraken.Kraken_orderbook_feed.price
           lvl.Kraken.Kraken_orderbook_feed.price_wire
           lvl.Kraken.Kraken_orderbook_feed.size)
    arr
;;

let validate symbol st checksum_received =
  let depth = 10 in
  let bid_arr =
    Kraken.Kraken_orderbook_feed.levels_to_array ~sort_desc:true st.bids depth
  in
  let ask_arr =
    Kraken.Kraken_orderbook_feed.levels_to_array ~sort_desc:false st.asks depth
  in
  (* Same argument order as production: bids (desc) then asks (asc); the
     function itself hashes asks first per Kraken's spec. *)
  let calc = Kraken.Kraken_orderbook_feed.calculate_checksum symbol bid_arr ask_arr in
  st.validated <- st.validated + 1;
  if Int32.compare calc checksum_received <> 0
  then (
    st.mismatched <- st.mismatched + 1;
    Printf.printf
      "MISMATCH %s after %d updates since snapshot | received=%ld calculated=%ld\n"
      symbol
      st.updates_since_snapshot
      checksum_received
      calc;
    (* Independent recomputation straight from the wire strings, bypassing
       calculate_checksum entirely - isolates math vs state. *)
    let crc = ref 0xFFFFFFFFl in
    let feed s =
      crc
      := Kraken.Kraken_orderbook_feed.add_normalized_to_crc
           !crc
           (Kraken.Kraken_orderbook_feed.to_decimal_str ~trim_trailing:false (`String s))
    in
    let norm_repr s =
      let s' = String.concat "" (String.split_on_char '.' s) in
      let rec strip i =
        if i < String.length s' && s'.[i] = '0'
        then strip (i + 1)
        else String.sub s' i (String.length s' - i)
      in
      strip 0
    in
    let manual = Buffer.create 256 in
    Array.iter
      (fun lvl ->
         feed lvl.Kraken.Kraken_orderbook_feed.price_wire;
         feed lvl.Kraken.Kraken_orderbook_feed.size;
         Buffer.add_string manual (norm_repr lvl.Kraken.Kraken_orderbook_feed.price_wire);
         Buffer.add_string manual (norm_repr lvl.Kraken.Kraken_orderbook_feed.size))
      ask_arr;
    Array.iter
      (fun lvl ->
         feed lvl.Kraken.Kraken_orderbook_feed.price_wire;
         feed lvl.Kraken.Kraken_orderbook_feed.size;
         Buffer.add_string manual (norm_repr lvl.Kraken.Kraken_orderbook_feed.price_wire);
         Buffer.add_string manual (norm_repr lvl.Kraken.Kraken_orderbook_feed.size))
      bid_arr;
    let manual_crc = Kraken.Kraken_orderbook_feed.crc32_zlib (Buffer.contents manual) in
    Printf.printf "  independent-wire-crc=%ld manual-stream-crc=%ld\n%!" !crc manual_crc;
    (* First mismatch since snapshot: replay the recent raw deltas. *)
    if (not st.first_mismatch_dumped) && st.updates_since_snapshot > 0
    then (
      st.first_mismatch_dumped <- true;
      Printf.printf
        "  DELTA_HISTORY %s (%d entries, oldest first):\n%!"
        symbol
        (Queue.length st.recent_deltas);
      Queue.iter (fun s -> Printf.printf "    DELTA: %s\n%!" s) st.recent_deltas);
    if st.updates_since_snapshot = 0
    then
      Printf.printf
        "  RAW_SNAPSHOT %s: %s\n%!"
        symbol
        (Yojson.Safe.to_string
           (`List
               [ `List
                   (List.map
                      (fun lvl ->
                         `List
                           [ `String lvl.Kraken.Kraken_orderbook_feed.price_wire
                           ; `String lvl.Kraken.Kraken_orderbook_feed.size
                           ])
                      (Array.to_list bid_arr))
               ; `List
                   (List.map
                      (fun lvl ->
                         `List
                           [ `String lvl.Kraken.Kraken_orderbook_feed.price_wire
                           ; `String lvl.Kraken.Kraken_orderbook_feed.size
                           ])
                      (Array.to_list ask_arr))
               ]));
    dump_levels "bids (desc)" bid_arr;
    dump_levels "asks (asc)" ask_arr;
    flush stdout)
  else (
    if st.updates_since_snapshot = 0 then st.snapshot_ok <- true;
    Printf.printf
      "ok        %s seq=%s after=%d total_ok=%d\n%!"
      symbol
      (match st.seq with
       | Some s -> Int64.to_string s
       | None -> "?")
      st.updates_since_snapshot
      st.validated)
;;

let handle_message json =
  let open Yojson.Safe.Util in
  try
    let channel = member "channel" json |> to_string in
    if channel = "book"
    then (
      let typ = member "type" json |> to_string in
      let data = member "data" json |> to_list in
      List.iter
        (fun entry ->
           let symbol = member "symbol" entry |> to_string in
           let st = get_state symbol in
           (match typ with
            | "snapshot" ->
              Hashtbl.clear st.bids;
              Hashtbl.clear st.asks;
              st.updates_since_snapshot <- 0;
              Queue.clear st.recent_deltas;
              st.first_mismatch_dumped <- false;
              (* One-time raw wire dump: reveals string vs number encoding. *)
              if (not st.snapshot_ok) && st.validated = 0
              then
                Printf.printf
                  "RAW_BIDS_JSON %s: %s\n%!"
                  symbol
                  (Yojson.Safe.to_string (member "bids" entry))
            | _ ->
              (* Keep the raw delta for post-mortem on first mismatch. *)
              Queue.add (Yojson.Safe.to_string entry) st.recent_deltas;
              if Queue.length st.recent_deltas > 40
              then Queue.pop st.recent_deltas |> ignore;
              (* Sequence gap check like prod *)
              let seq_opt =
                Kraken.Kraken_orderbook_feed.int64_of_json (member "sequence" entry)
              in
              (match seq_opt with
               | Some curr ->
                 (match st.seq with
                  | Some last when Int64.compare curr (Int64.add last 1L) > 0 ->
                    Printf.printf "GAP %s current=%Ld last=%Ld\n%!" symbol curr last
                  | _ -> ())
               | None -> Printf.printf "NO_SEQ_FIELD %s (%s)\n%!" symbol typ);
              st.updates_since_snapshot <- st.updates_since_snapshot + 1);
           let bids_json = member "bids" entry in
           let asks_json = member "asks" entry in
           Kraken.Kraken_orderbook_feed.parse_and_apply_levels symbol st.bids bids_json;
           Kraken.Kraken_orderbook_feed.parse_and_apply_levels symbol st.asks asks_json;
           (* HYPOTHESIS TEST: spec says truncate to subscribed depth after
               EVERY update - out-of-scope levels are never removed via qty:0,
               so retained ghosts corrupt the computed top-10 on removals. *)
           if Hashtbl.length st.bids > 10
           then Kraken.Kraken_orderbook_feed.truncate_hashtbl st.bids true 10;
           if Hashtbl.length st.asks > 10
           then Kraken.Kraken_orderbook_feed.truncate_hashtbl st.asks false 10;
           st.seq <- Kraken.Kraken_orderbook_feed.int64_of_json (member "sequence" entry);
           match
             Yojson.Safe.Util.member "checksum" entry |> Yojson.Safe.Util.to_int_option
           with
           | Some cs -> validate symbol st (Int32.of_int cs)
           | None -> Printf.printf "no checksum field for %s (%s)\n%!" symbol typ)
        data)
    else Printf.printf "ignoring channel=%s\n" channel
  with
  | exn ->
    Printf.printf
      "parse error: %s\n%s\n%!"
      (Printexc.to_string exn)
      (Yojson.Safe.to_string json)
;;

let () =
  Random.self_init ();
  Logging.set_level Logging.INFO;
  Mirage_crypto_rng_unix.use_default ();
  (* Mirror production: fetch real pair precision before any book processing. *)
  Lwt_main.run (Kraken.Kraken_orderbook_feed.initialize symbols);
  Lwt_main.run
    (let open Lwt.Infix in
     let ctx = Kraken.Kraken_common_types.get_conduit_ctx () in
     let uri = Uri.of_string "wss://ws.kraken.com/v2" in
     Lwt_unix.getaddrinfo "ws.kraken.com" "443" [ Unix.AI_FAMILY Unix.PF_INET ]
     >>= fun addresses ->
     let ip =
       match addresses with
       | { Unix.ai_addr = Unix.ADDR_INET (addr, _); _ } :: _ ->
         Ipaddr_unix.of_inet_addr addr
       | _ -> failwith "resolve failed"
     in
     Websocket_lwt_unix.connect
       ~ctx
       (`TLS (`Hostname "ws.kraken.com", `IP ip, `Port 443))
       uri
     >>= fun conn ->
     let sub =
       `Assoc
         [ "method", `String "subscribe"
         ; ( "params"
           , `Assoc
               [ "channel", `String "book"
               ; "symbol", `List (List.map (fun s -> `String s) symbols)
               ; "depth", `Int 10
               ] )
         ]
     in
     Websocket_lwt_unix.write
       conn
       (Websocket.Frame.create ~content:(Yojson.Safe.to_string sub) ())
     >>= fun () ->
     let deadline = Unix.gettimeofday () +. 45.0 in
     let rec loop () =
       if Unix.gettimeofday () > deadline
       then Lwt.return_unit
       else
         Lwt.pick
           [ (Websocket_lwt_unix.read conn
              >>= fun frame ->
              (try
                 match Yojson.Safe.from_string frame.Websocket.Frame.content with
                 | json -> handle_message json
               with
               | _ -> ());
              loop ())
           ; (Lwt_unix.sleep 50.0 >>= fun () -> Lwt.return_unit)
           ]
     in
     loop ()
     >>= fun () ->
     Printf.printf "\n==== SUMMARY ====\n";
     Hashtbl.iter
       (fun symbol st ->
          Printf.printf
            "%s: snapshot_ok=%b validated=%d mismatched=%d\n"
            symbol
            st.snapshot_ok
            st.validated
            st.mismatched)
       states;
     Lwt.return_unit)
;;
