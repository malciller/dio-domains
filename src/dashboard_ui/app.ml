open Notty
open Theme

(** Main App loop for the dashboard UI.
    Connects to the UDS, processes the JSON stream, and loops the frame renderer. *)

let socket_path = ref ""

(** Default socket path matching the engine's fixed location. *)
let default_socket_path = "/var/run/dio/dashboard.sock"

let discover_socket_candidates () =
  let fixed =
    [ default_socket_path; "/tmp/dio/dashboard.sock" ] |> List.filter Sys.file_exists
  in
  if fixed <> []
  then fixed
  else (
    let entries =
      try Sys.readdir "/tmp" with
      | _ -> [||]
    in
    Array.to_list entries
    |> List.filter (fun f ->
      String.length f > 4
      && String.sub f 0 4 = "dio-"
      &&
      let len = String.length f in
      String.sub f (len - 5) 5 = ".sock")
    |> List.sort (fun a b -> String.compare b a)
    |> List.map (fun f -> "/tmp/" ^ f))
;;

let connect_and_watch path =
  let fd = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  try
    Unix.connect fd (Unix.ADDR_UNIX path);
    (* Non-blocking: the render loop must never block mid-payload (a blocked
       read stalls renders -> missed heartbeats -> the server prunes the
       client and the dashboard blanks out every engine cycle). *)
    Unix.set_nonblock fd;
    let _ = Unix.write_substring fd "W" 0 1 in
    fd
  with
  | exn ->
    (try Unix.close fd with
     | _ -> ());
    raise exn
;;

(** Reusable frame buffer — avoids per-frame allocation.
    Cleared and refilled on each render cycle. *)
let frame_buf = Buffer.create 65536

let render_to_stdout_buf (draw : Buffer.t -> unit) =
  Buffer.clear frame_buf;
  draw frame_buf;
  Buffer.output_buffer stdout frame_buf;
  flush stdout
;;

let stdout_alive () =
  try Unix.isatty Unix.stdout with
  | Unix.Unix_error _ -> false
;;

exception Render_timeout

let render_to_stdout_safe ~timeout_s draw =
  let old_handler =
    Sys.signal Sys.sigalrm (Sys.Signal_handle (fun _ -> raise Render_timeout))
  in
  let completed = ref false in
  (try
     ignore (Unix.alarm timeout_s);
     render_to_stdout_buf draw;
     ignore (Unix.alarm 0);
     completed := true
   with
   | Render_timeout -> ignore (Unix.alarm 0)
   | exn ->
     ignore (Unix.alarm 0);
     raise exn);
  Sys.set_signal Sys.sigalrm old_handler;
  !completed
;;

let render_wait_screen w h msg =
  let img =
    I.string A.(fg c_yellow ++ bg c_bg) msg
    |> I.hsnap ~align:`Left w
    |> I.vsnap ~align:`Top h
  in
  render_to_stdout_buf (fun buf ->
    Buffer.add_string buf "\027[?2026h";
    Buffer.add_string buf "\027[H";
    Render.to_buffer buf Cap.ansi (0, 0) (w, I.height img) img;
    Buffer.add_string buf "\027[J";
    Buffer.add_string buf "\027[?2026l")
;;

(** Incremental, non-blocking frame assembler for the UDS stream.
    The engine pushes a full state snapshot every ~500 ms; those frames can
    be large, and a blocking [read_exact] mid-payload would stall the render
    loop (no pongs -> the server prunes the client -> blank dashboard +
    reconnect flicker). The fd is non-blocking: whatever is available is
    drained into [buf], complete length-prefixed frames are extracted, and
    the loop never blocks on the socket. *)
type frame_assembler = { buf : Buffer.t }

let assem_create () = { buf = Buffer.create 65536 }

let assem_drain fd (assem : frame_assembler) : [ `Data | `Eof | `Error ] =
  let tmp = Bytes.create 8192 in
  let rec loop () =
    match Unix.read fd tmp 0 8192 with
    | 0 -> `Eof
    | n ->
      Buffer.add_subbytes assem.buf tmp 0 n;
      loop ()
    | exception Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK), _, _) -> `Data
    | exception Unix.Unix_error _ -> `Error
  in
  loop ()
;;

(** Extract one complete length-prefixed frame if available. *)
let assem_extract (assem : frame_assembler) : string option =
  let buf = assem.buf in
  let len = Buffer.length buf in
  if len < 4
  then None
  else (
    let header = Buffer.sub buf 0 4 in
    let frame_len =
      (Char.code header.[0] lsl 24)
      lor (Char.code header.[1] lsl 16)
      lor (Char.code header.[2] lsl 8)
      lor Char.code header.[3]
    in
    if frame_len > 10_000_000
    then (
      (* Corrupt/oversized frame: drop the whole buffer and resync. *)
      Buffer.clear buf;
      None)
    else if len < 4 + frame_len
    then None
    else (
      let frame = Buffer.sub buf 4 frame_len in
      (* Remove the consumed frame (keep any trailing partial bytes). *)
      let rest = Buffer.sub buf (4 + frame_len) (len - 4 - frame_len) in
      Buffer.clear buf;
      Buffer.add_string buf rest;
      Some frame))
;;

let run () =
  (* GC tuning for a lightweight single-domain render loop.
     Small minor heap enables frequent collections of short-lived
     frame data. Moderate compaction keeps the heap from fragmenting
     over multi-hour runs. *)
  Gc.set
    { (Gc.get ()) with
      minor_heap_size = 32768
    ; (* 256KB — fast minor collections *)
      space_overhead = 40
    ; (* major GC targets 1.4x live data — override engine's o=2000 *)
      major_heap_increment = 65536
    ; (* 512KB — grow major heap slowly *)
      max_overhead = 500 (* compact when free > 5x live *)
    };
  let saved_termios = Unix.tcgetattr Unix.stdin in
  let raw_termios =
    { saved_termios with
      Unix.c_icanon = false
    ; Unix.c_echo = false
    ; Unix.c_isig = false
    ; Unix.c_vmin = 0
    ; Unix.c_vtime = 0
    }
  in
  Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH raw_termios;
  Printf.printf "\027[?1049h\027[?25l%!";
  at_exit (fun () ->
    Printf.printf "\027[?25h\027[?1049l%!";
    Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH saved_termios);
  let last_json = ref (`Assoc []) in
  let has_cached_data = ref false in
  let quit = ref false in
  let input_buf = Bytes.create 64 in
  let view_mode_ref = ref `MainView in
  let selected_index_ref = ref 0 in
  let find_asset_index key assets =
    let rec aux i = function
      | [] -> None
      | (a : Holdings.selectable_asset) :: rest ->
        if a.key = key then Some i else aux (i + 1) rest
    in
    aux 0 assets
  in
  let parse_key_bytes buf n =
    let rec parse i acc =
      if i >= n
      then acc
      else (
        let ch = Bytes.get buf i in
        if ch = '\027'
        then
          if i + 2 < n && Bytes.get buf (i + 1) = '['
          then (
            match Bytes.get buf (i + 2) with
            | 'A' -> parse (i + 3) (`Key_up :: acc)
            | 'B' -> parse (i + 3) (`Key_down :: acc)
            | 'C' -> parse (i + 3) (`Key_right :: acc)
            | 'D' -> parse (i + 3) (`Key_left :: acc)
            | _ -> parse (i + 3) (`Key_back :: acc))
          else parse (i + 1) (`Key_back :: acc)
        else (
          match ch with
          | 'q' | 'Q' -> parse (i + 1) (`Key_quit :: acc)
          | 'k' | 'K' -> parse (i + 1) (`Key_up :: acc)
          | 'j' | 'J' -> parse (i + 1) (`Key_down :: acc)
          | 'h' | 'H' -> parse (i + 1) (`Key_left :: acc)
          | 'l' | 'L' -> parse (i + 1) (`Key_right :: acc)
          | '\r' | '\n' | ' ' -> parse (i + 1) (`Key_enter :: acc)
          | '=' | '+' -> parse (i + 1) (`Key_zoom_in :: acc)
          | '-' | '_' -> parse (i + 1) (`Key_zoom_out :: acc)
          | 'b' | 'B' | '\b' | '\127' -> parse (i + 1) (`Key_back :: acc)
          | _ -> parse (i + 1) acc))
    in
    List.rev (parse 0 [])
  in
  Sys.set_signal Sys.sighup (Sys.Signal_handle (fun _ -> quit := true));
  let fd_ref : Unix.file_descr option ref = ref None in
  let try_connect () =
    let candidates =
      if !socket_path <> "" && !fd_ref = None
      then [ !socket_path ]
      else discover_socket_candidates ()
    in
    let rec try_candidates = function
      | [] -> None
      | p :: rest ->
        (try
           let fd = connect_and_watch p in
           fd_ref := Some fd;
           Some fd
         with
         | Unix.Unix_error _ ->
           (try Unix.unlink p with
            | _ -> ());
           try_candidates rest)
    in
    if List.length candidates > 1
    then Printf.eprintf "Warning: multiple engine sockets found, trying newest first\n%!";
    try_candidates candidates
  in
  let disconnect fd =
    fd_ref := None;
    (* Cache the last known state: never blank the dashboard on a dropped
       connection - it keeps rendering the cached snapshot (with the engine
       status frozen) until the reconnect delivers fresh data. *)
    (try
       let _ = Unix.write_substring fd "Q" 0 1 in
       ()
     with
     | _ -> ());
    try Unix.close fd with
    | _ -> ()
  in
  (* The frame draw is shared by the live loop and the reconnect path, so
     the dashboard keeps showing the CACHED last snapshot while it waits
     for the engine - it never blanks out. *)
  let draw_frame w h =
    let draw buf =
      Buffer.add_string buf "\027[?2026h";
      Buffer.add_string buf "\027[H";
      let content_img =
        match !view_mode_ref with
        | `MainView ->
          let uncropped =
            I.vcat
              [ Kpi_cards.render_kpi_cards w !last_json
              ; Ticker_feed.render_ticker w !last_json
              ; Holdings.render_strategies
                  ~selected_index:(Some !selected_index_ref)
                  w
                  !last_json
              ; Recent_fills_feed.render_fills w !last_json
              ; Memory.render_memory w !last_json
              ; Latencies.render_latencies w !last_json
              ; Footer.render_footer w !last_json
              ]
          in
          I.hsnap ~align:`Left w uncropped
        | `DetailView asset_key ->
          let detail_img = Asset_graph.render_asset_detail w h asset_key !last_json in
          I.hsnap ~align:`Left w detail_img
      in
      let c_h = I.height content_img in
      let c_w = I.width content_img in
      let content_img =
        if c_h < h
        then I.vsnap ~align:`Middle h content_img
        else I.vsnap ~align:`Top h content_img
      in
      let content_img =
        if c_w < w
        then I.hsnap ~align:`Middle w content_img
        else I.hsnap ~align:`Left w content_img
      in
      let img = I.(content_img </> I.char A.(bg c_bg) ' ' w h) in
      Render.to_buffer buf Cap.ansi (0, 0) (w, I.height img) img;
      Buffer.add_string buf "\027[J";
      Buffer.add_string buf "\027[?2026l"
    in
    render_to_stdout_safe ~timeout_s:2 draw
  in
  (* Render throttle: full frames on changes (~2/s), a keep-alive frame
     every 2s when idle. A frame that exceeds the alarm timeout is SKIPPED,
     not fatal - the loop continues and the next frame retries (the old
     behavior killed the whole UI on a slow frame). *)
  let render_if_due ~(now : float) ~(last_render : float ref) ~(dirty : bool) =
    let interval = if dirty then 0.5 else 2.0 in
    if now -. !last_render < interval
    then `Not_due
    else (
      last_render := now;
      if not (stdout_alive ())
      then `Dead
      else (
        let w, h =
          match Notty_unix.winsize Unix.stdout with
          | Some (w, h) -> w, h
          | None -> 80, 24
        in
        if draw_frame w h then `Ok else `Skipped))
  in
  let rec wait_for_engine () =
    if !quit
    then ()
    else (
      match try_connect () with
      | Some fd -> run_event_loop fd
      | None ->
        let w, h =
          match Notty_unix.winsize Unix.stdout with
          | Some (w, h) -> w, h
          | None -> 80, 24
        in
        (* Cached state: keep the last dashboard visible (stale but real)
           while reconnecting; only a truly first run shows the wait
           screen. *)
        if !has_cached_data
        then ignore (draw_frame w h)
        else render_wait_screen w h "Waiting for engine...  (q to quit)";
        let ready, _, _ =
          try Unix.select [ Unix.stdin ] [] [] 1.0 with
          | Unix.Unix_error _ -> [], [], []
        in
        if List.mem Unix.stdin ready
        then (
          let n =
            try Unix.read Unix.stdin input_buf 0 64 with
            | _ -> 0
          in
          if n = 0
          then quit := true
          else
            for i = 0 to n - 1 do
              match Bytes.get input_buf i with
              | 'q' | 'Q' | '\027' -> quit := true
              | _ -> ()
            done);
        if not !quit then wait_for_engine ())
  and run_event_loop fd =
    let lost_connection = ref false in
    let last_render_time = ref (Unix.gettimeofday ()) in
    let last_pong_time = ref (Unix.gettimeofday ()) in
    let dirty = ref true in
    let assem = assem_create () in
    while (not !quit) && not !lost_connection do
      let now = Unix.gettimeofday () in
      (* Heartbeat on a FIXED cadence, decoupled from rendering: the server
         prunes clients that miss pongs for ~3s, and a slow frame or a big
         snapshot parse must never cost the connection (and with it the
         whole dashboard state). *)
      if now -. !last_pong_time >= 1.0
      then (
        last_pong_time := now;
        try
          let _ = Unix.write_substring fd "P" 0 1 in
          ()
        with
        | _ -> ());
      let render_interval = if !dirty then 0.5 else 2.0 in
      let next_render = render_interval -. (now -. !last_render_time) in
      let next_pong = 1.0 -. (now -. !last_pong_time) in
      let timeout = max 0.0 (Float.min next_render next_pong) in
      let ready, _, _ =
        try Unix.select [ fd; Unix.stdin ] [] [] timeout with
        | Unix.Unix_error _ -> [], [], []
      in
      if List.mem Unix.stdin ready
      then (
        let n =
          try Unix.read Unix.stdin input_buf 0 64 with
          | _ -> 0
        in
        if n = 0
        then quit := true
        else (
          let actions = parse_key_bytes input_buf n in
          let assets = Holdings.get_selectable_assets !last_json in
          let asset_count = List.length assets in
          List.iter
            (fun action ->
               match !view_mode_ref with
               | `MainView ->
                 (match action with
                  | `Key_quit -> quit := true
                  | `Key_up ->
                    if asset_count > 0
                    then selected_index_ref := max 0 (!selected_index_ref - 1)
                  | `Key_down ->
                    if asset_count > 0
                    then
                      selected_index_ref := min (asset_count - 1) (!selected_index_ref + 1)
                  | `Key_enter ->
                    if asset_count > 0
                    then (
                      let idx = min (asset_count - 1) (max 0 !selected_index_ref) in
                      let asset = List.nth assets idx in
                      view_mode_ref := `DetailView asset.key)
                  | `Key_back -> quit := true
                  | _ -> ())
               | `DetailView curr_key ->
                 (match action with
                  | `Key_quit -> quit := true
                  | `Key_back -> view_mode_ref := `MainView
                  | `Key_up | `Key_left ->
                    if asset_count > 0
                    then (
                      let curr_idx =
                        match find_asset_index curr_key assets with
                        | Some i -> i
                        | None -> 0
                      in
                      let new_idx =
                        if curr_idx > 0 then curr_idx - 1 else asset_count - 1
                      in
                      selected_index_ref := new_idx;
                      let new_asset = List.nth assets new_idx in
                      view_mode_ref := `DetailView new_asset.key)
                  | `Key_down | `Key_right ->
                    if asset_count > 0
                    then (
                      let curr_idx =
                        match find_asset_index curr_key assets with
                        | Some i -> i
                        | None -> 0
                      in
                      let new_idx =
                        if curr_idx < asset_count - 1 then curr_idx + 1 else 0
                      in
                      selected_index_ref := new_idx;
                      let new_asset = List.nth assets new_idx in
                      view_mode_ref := `DetailView new_asset.key)
                  | `Key_zoom_in -> Asset_graph.zoom_in curr_key
                  | `Key_zoom_out -> Asset_graph.zoom_out curr_key
                  | _ -> ()))
            actions;
          dirty := true));
      if List.mem fd ready && not !quit
      then (
        (* Non-blocking drain: complete frames are parsed, partial payloads
           wait in the assembler - the loop is never blocked on the socket. *)
        match assem_drain fd assem with
        | `Eof ->
          disconnect fd;
          lost_connection := true
        | `Error ->
          disconnect fd;
          lost_connection := true
        | `Data ->
          let rec take_frames () =
            match assem_extract assem with
            | None -> ()
            | Some msg ->
              (try
                 let new_json = Yojson.Basic.from_string msg in
                 if new_json <> !last_json
                 then (
                   last_json := new_json;
                   has_cached_data := true;
                   Asset_graph.record_all_prices new_json;
                   dirty := true)
               with
               | _ -> ());
              take_frames ()
          in
          take_frames ());
      if (not !quit) && not !lost_connection
      then (
        match
          render_if_due
            ~now:(Unix.gettimeofday ())
            ~last_render:last_render_time
            ~dirty:!dirty
        with
        | `Dead ->
          disconnect fd;
          quit := true
        | `Skipped -> dirty := false
        | `Ok -> dirty := false
        | `Not_due -> ())
    done;
    (match !fd_ref with
     | Some fd -> disconnect fd
     | None -> ());
    if not !quit then wait_for_engine ()
  in
  wait_for_engine ()
;;
