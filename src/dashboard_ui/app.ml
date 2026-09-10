open Notty
open Theme

(** Main loop for the dashboard UI: it connects to the engine's Unix domain
    socket, processes the JSON snapshot stream, and runs the frame renderer
    on every cycle. *)

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
    (* The socket is set to non-blocking so the render loop never stalls
       mid-payload: a blocked read delays renders, which causes missed
       heartbeats, which in turn makes the server prune the client and
       leaves the dashboard blank on every engine cycle. *)
    Unix.set_nonblock fd;
    let _ = Unix.write_substring fd "W" 0 1 in
    fd
  with
  | exn ->
    (try Unix.close fd with
     | _ -> ());
    raise exn
;;

(** Reusable frame buffer that avoids per-frame allocation.
    It is cleared and refilled on each render cycle. *)
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

(** The SIGALRM handler is installed once in [run] rather than being saved and
    restored on every frame. [Unix.alarm] is armed only around a render and
    cleared immediately after, so a single handler is sufficient. *)
let render_to_stdout_safe ~timeout_s draw =
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
  !completed
;;

let render_wait_screen w h msg =
  let t = Theme.current () in
  let img =
    I.string A.(fg t.c_yellow ++ bg t.c_bg) msg
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
      (* A corrupt or oversized frame: drop the whole buffer so the stream
         can resynchronize at the next frame boundary. *)
      Buffer.clear buf;
      None)
    else if len < 4 + frame_len
    then None
    else (
      let frame = Buffer.sub buf 4 frame_len in
      (* Remove the consumed frame, keeping any trailing partial bytes. *)
      let rest = Buffer.sub buf (4 + frame_len) (len - 4 - frame_len) in
      Buffer.clear buf;
      Buffer.add_string buf rest;
      Some frame))
;;

let run ?(config_file = "config.json") () =
  (* Load user's theme from config.json or disk if present *)
  Theme.load_saved_theme ~config_file ();
  (* Motion controls: DIO_MOTION=off honours reduced-motion; DIO_FPS caps the
     animated frame rate (default 30). *)
  (match Sys.getenv_opt "DIO_MOTION" with
   | Some s ->
     (match String.lowercase_ascii (String.trim s) with
      | "off" | "0" | "false" | "no" -> Anim.reduced_motion := true
      | _ -> ())
   | None -> ());
  (match Sys.getenv_opt "DIO_FPS" with
   | Some s ->
     (match float_of_string_opt (String.trim s) with
      | Some f when f > 0.0 -> Anim.target_fps := f
      | _ -> ())
   | None -> ());
  (* GC tuning for a lightweight single-domain render loop.
     Small minor heap enables frequent collections of short-lived
     frame data. Moderate compaction keeps the heap from fragmenting
     over multi-hour runs. *)
  Gc.set
    { (Gc.get ()) with
      minor_heap_size = 4_194_304
    ; (* 32MB: the render loop allocates heavily per frame (image trees,
         gradients, formatted cells); a large minor heap cuts minor-GC
         frequency. Measured with test/engine/dashboard/bench_dashboard.exe:
         detail render ~2.4ms at 256KB vs ~1.6ms at 32MB. *)
      space_overhead = 40
    ; (* major GC targets 1.4x live data, overriding the engine's o=2000 *)
      major_heap_increment = 65536
    ; (* 512KB: grow the major heap slowly *)
      max_overhead = 500 (* compact when free space exceeds 5x live data *)
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
  let snapshot = ref (Snapshot.of_json (`Assoc [])) in
  let last_raw = ref "" in
  let has_cached_data = ref false in
  let quit = ref false in
  let input_buf = Bytes.create 64 in
  let view_mode_ref = ref `MainView in
  let selected_index_ref = ref 0 in
  let theme_modal_open = ref false in
  let theme_cursor_idx = ref 0 in
  let original_theme_id = ref "" in
  let find_asset_index key assets =
    let rec aux i = function
      | [] -> None
      | (a : Snapshot.selectable_asset) :: rest ->
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
          | 't' | 'T' -> parse (i + 1) (`Key_theme :: acc)
          | 'f' | 'F' -> parse (i + 1) (`Key_focus :: acc)
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
  (* Frame-render alarm handler, installed once for the whole session. *)
  Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ -> raise Render_timeout));
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
    (* Cache the last known state so the dashboard never blanks on a dropped
       connection: it keeps rendering the cached snapshot, with the engine
       status frozen, until a reconnect delivers fresh data. *)
    (try
       let _ = Unix.write_substring fd "Q" 0 1 in
       ()
     with
     | _ -> ());
    try Unix.close fd with
    | _ -> ()
  in
  (* Line-level frame diffing. We render the whole frame each cycle (cheap:
     ~1ms), split the ANSI stream into per-row strings, and re-emit only the
     rows whose bytes changed, addressed absolutely. Because every row is
     cropped to the terminal height and written with an explicit cursor
     position, content taller than the screen can never scroll the terminal
     (the old section-level approach wrote past the bottom edge and caused the
     seizure). It also makes localized animation cheap over SSH: only the rows
     that actually move are transmitted. *)
  let split_nel s =
    let parts = ref [] in
    let buf = Buffer.create 128 in
    let n = String.length s in
    let i = ref 0 in
    while !i < n do
      if
        !i + 1 < n
        && String.unsafe_get s !i = '\x1b'
        && String.unsafe_get s (!i + 1) = 'E'
      then (
        parts := Buffer.contents buf :: !parts;
        Buffer.clear buf;
        i := !i + 2)
      else (
        Buffer.add_char buf (String.unsafe_get s !i);
        incr i)
    done;
    parts := Buffer.contents buf :: !parts;
    List.rev !parts
  in
  let prev_lines : string array option ref = ref None in
  (* Escape hatch: DIO_DAMAGE=off forces full-frame redraws if a terminal ever
     renders the incremental updates wrong. *)
  let damage_enabled =
    match Sys.getenv_opt "DIO_DAMAGE" with
    | Some s ->
      (match String.lowercase_ascii (String.trim s) with
       | "off" | "0" | "false" | "no" -> false
       | _ -> true)
    | None -> true
  in
  let draw_frame w h =
    Anim.reset_frame ();
    let t = Theme.current () in
    let draw buf =
      Buffer.add_string buf "\027[?2026h";
      let content_img =
        match !view_mode_ref with
        | `MainView ->
          I.vcat
            [ Kpi_cards.render_kpi_cards w !snapshot
            ; Ticker_feed.render_ticker w !snapshot
            ; Holdings.render_strategies
                ~selected_index:(Some !selected_index_ref)
                w
                !snapshot
            ; Recent_fills_feed.render_fills w !snapshot
            ; Memory.render_memory w !snapshot
            ; Latencies.render_latencies w !snapshot
            ; Footer.render_footer w !snapshot
            ]
          |> I.hsnap ~align:`Left w
        | `DetailView asset_key ->
          Asset_graph.render_asset_detail w h asset_key !snapshot
          |> I.hsnap ~align:`Left w
        | `FocusView asset_key ->
          Focus_chart.render w h asset_key !snapshot |> I.hsnap ~align:`Left w
      in
      let content_img =
        if I.height content_img < h
        then I.vsnap ~align:`Middle h content_img
        else I.vsnap ~align:`Top h content_img
      in
      let content_img =
        if I.width content_img < w
        then I.hsnap ~align:`Middle w content_img
        else I.hsnap ~align:`Left w content_img
      in
      let img =
        if !theme_modal_open
        then (
          let modal_overlay =
            Theme.render_theme_modal ~target_w:w ~target_h:h ~cursor_idx:!theme_cursor_idx
          in
          I.(modal_overlay </> content_img </> I.char A.(bg t.c_bg) ' ' w h))
        else I.(content_img </> I.char A.(bg t.c_bg) ' ' w h)
      in
      let scratch = Buffer.create 65536 in
      Render.to_buffer scratch Cap.ansi (0, 0) (w, I.height img) img;
      let lines = Array.of_list (split_nel (Buffer.contents scratch)) in
      if not damage_enabled then prev_lines := None;
      (match !prev_lines with
       | Some prev when Array.length prev = Array.length lines ->
         Array.iteri
           (fun i line ->
              if line <> prev.(i)
              then (
                Buffer.add_string buf (Printf.sprintf "\027[%d;1H" (i + 1));
                Buffer.add_string buf line))
           lines
       | _ ->
         Array.iteri
           (fun i line ->
              Buffer.add_string buf (Printf.sprintf "\027[%d;1H" (i + 1));
              Buffer.add_string buf line)
           lines);
      prev_lines := Some lines;
      Buffer.add_string buf "\027[?2026l"
    in
    render_to_stdout_safe ~timeout_s:2 draw
  in
  (* Render throttling: full frames are drawn on changes, about two per
     second, plus a keep-alive frame every two seconds when idle. A frame
     that exceeds the alarm timeout is skipped rather than treated as fatal;
     the loop continues and the next frame retries. The old behavior killed
     the whole UI on a slow frame. *)
  let render_if_due ~(now : float) ~(last_render : float ref) ~(interval : float) =
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
        (* With cached state, keep the last dashboard visible, stale but
           real, while reconnecting; only a true first run shows the wait
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
    (* A fresh connection may follow a disturbed screen; force a full repaint. *)
    prev_lines := None;
    let lost_connection = ref false in
    let last_render_time = ref (Unix.gettimeofday ()) in
    let last_pong_time = ref (Unix.gettimeofday ()) in
    let dirty = ref true in
    let assem = assem_create () in
    while (not !quit) && not !lost_connection do
      let now = Unix.gettimeofday () in
      (* The heartbeat runs on a fixed cadence, decoupled from rendering:
         the server prunes clients that miss pongs for about three seconds,
         so a slow frame or a large snapshot parse must never cost the
         connection, and with it the whole dashboard state. *)
      if now -. !last_pong_time >= 1.0
      then (
        last_pong_time := now;
        try
          let _ = Unix.write_substring fd "P" 0 1 in
          ()
        with
        | _ -> ());
      let render_interval =
        if !Anim.reduced_motion
        then if !dirty then 0.5 else 2.0
        else if !dirty || Anim.pending () || Anim.active ()
        then 1.0 /. max 1.0 !Anim.target_fps
        else !Anim.idle_interval
      in
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
          let assets = !snapshot.assets in
          let asset_count = List.length assets in
          List.iter
            (fun action ->
               if !theme_modal_open
               then (
                 let num_themes = Theme.theme_count () in
                 match action with
                 | `Key_up ->
                   theme_cursor_idx := max 0 (!theme_cursor_idx - 1);
                   Theme.set_theme_by_index !theme_cursor_idx
                 | `Key_down ->
                   theme_cursor_idx := min (num_themes - 1) (!theme_cursor_idx + 1);
                   Theme.set_theme_by_index !theme_cursor_idx
                 | `Key_enter ->
                   Theme.set_theme_by_index !theme_cursor_idx;
                   Theme.save_theme (Theme.current ()).id;
                   theme_modal_open := false
                 | `Key_theme | `Key_back ->
                   (* Cancel and revert to original theme *)
                   if !original_theme_id <> ""
                   then ignore (Theme.set_theme_by_id !original_theme_id);
                   theme_modal_open := false
                 | `Key_quit -> theme_modal_open := false
                 | _ -> ())
               else (
                 match !view_mode_ref with
                 | `MainView ->
                   (match action with
                    | `Key_theme ->
                      theme_modal_open := true;
                      theme_cursor_idx := Theme.current_theme_index ();
                      original_theme_id := (Theme.current ()).id
                    | `Key_quit -> quit := true
                    | `Key_up ->
                      if asset_count > 0
                      then selected_index_ref := max 0 (!selected_index_ref - 1)
                    | `Key_down ->
                      if asset_count > 0
                      then
                        selected_index_ref
                        := min (asset_count - 1) (!selected_index_ref + 1)
                    | `Key_enter ->
                      if asset_count > 0
                      then (
                        let idx = min (asset_count - 1) (max 0 !selected_index_ref) in
                        let asset = List.nth assets idx in
                        view_mode_ref := `DetailView asset.key)
                    | `Key_focus ->
                      if asset_count > 0
                      then (
                        let idx = min (asset_count - 1) (max 0 !selected_index_ref) in
                        let asset = List.nth assets idx in
                        view_mode_ref := `FocusView asset.key)
                    | `Key_back -> quit := true
                    | `Key_left -> Latencies.prev_page ()
                    | `Key_right -> Latencies.next_page ()
                    | _ -> ())
                 | `DetailView curr_key ->
                   (match action with
                    | `Key_theme ->
                      theme_modal_open := true;
                      theme_cursor_idx := Theme.current_theme_index ();
                      original_theme_id := (Theme.current ()).id
                    | `Key_quit -> quit := true
                    | `Key_back -> view_mode_ref := `MainView
                    | `Key_focus -> view_mode_ref := `FocusView curr_key
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
                    | _ -> ())
                 | `FocusView curr_key ->
                   (match action with
                    | `Key_theme ->
                      theme_modal_open := true;
                      theme_cursor_idx := Theme.current_theme_index ();
                      original_theme_id := (Theme.current ()).id
                    | `Key_quit -> quit := true
                    | `Key_back -> view_mode_ref := `DetailView curr_key
                    | `Key_focus -> view_mode_ref := `MainView
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
                        view_mode_ref := `FocusView new_asset.key)
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
                        view_mode_ref := `FocusView new_asset.key)
                    | _ -> ())))
            actions;
          dirty := true));
      if List.mem fd ready && not !quit
      then (
        (* Non-blocking drain: complete frames are parsed immediately,
           partial payloads wait in the assembler, and the loop is never
           blocked on the socket. *)
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
                 if msg <> !last_raw
                 then (
                   let new_snapshot = Snapshot.of_json (Yojson.Basic.from_string msg) in
                   last_raw := msg;
                   snapshot := new_snapshot;
                   has_cached_data := true;
                   Anim.note_activity ();
                   Asset_graph.record_all_prices new_snapshot;
                   Latencies.ingest new_snapshot;
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
            ~interval:render_interval
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
