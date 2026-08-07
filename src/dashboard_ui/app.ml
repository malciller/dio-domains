open Notty
open Theme

(** Main App loop for the dashboard UI.
    Connects to the UDS, processes the JSON stream, and loops the frame renderer. *)

let socket_path = ref ""

(** Default socket path matching the engine's fixed location. *)
let default_socket_path = "/var/run/dio/dashboard.sock"

let discover_socket_candidates () =
  let fixed = [default_socket_path] |> List.filter Sys.file_exists in
  if fixed <> [] then fixed
  else begin
    let entries = try Sys.readdir "/tmp" with _ -> [||] in
    Array.to_list entries
    |> List.filter (fun f ->
      String.length f > 4 && String.sub f 0 4 = "dio-" &&
      let len = String.length f in
      String.sub f (len - 5) 5 = ".sock")
    |> List.sort (fun a b -> String.compare b a)
    |> List.map (fun f -> "/tmp/" ^ f)
  end

let read_exact fd buf off len =
  let rec loop off remaining =
    if remaining = 0 then ()
    else
      let n = Unix.read fd buf off remaining in
      if n = 0 then raise End_of_file;
      loop (off + n) (remaining - n)
  in
  loop off len

let read_message fd =
  let header = Bytes.create 4 in
  read_exact fd header 0 4;
  let len =
    (Bytes.get_uint8 header 0 lsl 24) lor
    (Bytes.get_uint8 header 1 lsl 16) lor
    (Bytes.get_uint8 header 2 lsl 8) lor
    (Bytes.get_uint8 header 3)
  in
  if len > 10_000_000 then failwith "message too large";
  let payload = Bytes.create len in
  read_exact fd payload 0 len;
  Bytes.to_string payload

let connect_and_watch path =
  let fd = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
  try
    Unix.connect fd (Unix.ADDR_UNIX path);
    let _ = Unix.write_substring fd "W" 0 1 in
    fd
  with exn ->
    (try Unix.close fd with _ -> ());
    raise exn

(** Reusable frame buffer — avoids per-frame allocation.
    Cleared and refilled on each render cycle. *)
let frame_buf = Buffer.create 65536

let render_to_stdout_buf (draw : Buffer.t -> unit) =
  Buffer.clear frame_buf;
  draw frame_buf;
  Buffer.output_buffer stdout frame_buf;
  flush stdout

let stdout_alive () =
  try Unix.isatty Unix.stdout
  with Unix.Unix_error _ -> false

exception Render_timeout

let render_to_stdout_safe ~timeout_s draw =
  let old_handler = Sys.signal Sys.sigalrm
    (Sys.Signal_handle (fun _ -> raise Render_timeout)) in
  let completed = ref false in
  (try
    ignore (Unix.alarm timeout_s);
    render_to_stdout_buf draw;
    ignore (Unix.alarm 0);
    completed := true
  with
  | Render_timeout -> ignore (Unix.alarm 0)
  | exn -> ignore (Unix.alarm 0); raise exn);
  Sys.set_signal Sys.sigalrm old_handler;
  !completed

let render_wait_screen w h msg =
  let img = I.string A.(fg c_yellow ++ bg c_bg) msg
            |> I.hsnap ~align:`Left w
            |> I.vsnap ~align:`Top  h
  in
  render_to_stdout_buf (fun buf ->
    Buffer.add_string buf "\027[?2026h";
    Buffer.add_string buf "\027[H";
    Render.to_buffer buf Cap.ansi (0, 0) (w, I.height img) img;
    Buffer.add_string buf "\027[J";
    Buffer.add_string buf "\027[?2026l")

let run () =
  (* GC tuning for a lightweight single-domain render loop.
     Small minor heap enables frequent collections of short-lived
     frame data. Moderate compaction keeps the heap from fragmenting
     over multi-hour runs. *)
  Gc.set { (Gc.get ()) with
    minor_heap_size = 32768;       (* 256KB — fast minor collections *)
    space_overhead = 40;           (* major GC targets 1.4x live data — override engine's o=2000 *)
    major_heap_increment = 65536;  (* 512KB — grow major heap slowly *)
    max_overhead = 500;            (* compact when free > 5x live *)
  };

  let saved_termios = Unix.tcgetattr Unix.stdin in
  let raw_termios = { saved_termios with
    Unix.c_icanon = false;
    Unix.c_echo = false;
    Unix.c_isig = false;
    Unix.c_vmin = 0;
    Unix.c_vtime = 0;
  } in
  Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH raw_termios;

  Printf.printf "\027[?1049h\027[?25l%!";

  at_exit (fun () ->
    Printf.printf "\027[?25h\027[?1049l%!"; 
    Unix.tcsetattr Unix.stdin Unix.TCSAFLUSH saved_termios
  );

  let last_json = ref (`Assoc []) in
  let quit = ref false in
  let input_buf = Bytes.create 64 in

  let view_mode_ref = ref `MainView in
  let selected_index_ref = ref 0 in

  let find_asset_index key assets =
    let rec aux i = function
      | [] -> None
      | (a : Holdings.selectable_asset) :: rest ->
          if a.key = key then Some i else aux (i + 1) rest
    in aux 0 assets
  in

  let parse_key_bytes buf n =
    let rec parse i acc =
      if i >= n then acc
      else
        let ch = Bytes.get buf i in
        if ch = '\027' then
          if i + 2 < n && Bytes.get buf (i + 1) = '[' then
            match Bytes.get buf (i + 2) with
            | 'A' -> parse (i + 3) (`Key_up :: acc)
            | 'B' -> parse (i + 3) (`Key_down :: acc)
            | 'C' -> parse (i + 3) (`Key_right :: acc)
            | 'D' -> parse (i + 3) (`Key_left :: acc)
            | _ -> parse (i + 3) (`Key_back :: acc)
          else parse (i + 1) (`Key_back :: acc)
        else match ch with
          | 'q' | 'Q' -> parse (i + 1) (`Key_quit :: acc)
          | 'k' | 'K' -> parse (i + 1) (`Key_up :: acc)
          | 'j' | 'J' -> parse (i + 1) (`Key_down :: acc)
          | 'h' | 'H' -> parse (i + 1) (`Key_left :: acc)
          | 'l' | 'L' -> parse (i + 1) (`Key_right :: acc)
          | '\r' | '\n' | ' ' -> parse (i + 1) (`Key_enter :: acc)
          | 'b' | 'B' | '\b' | '\127' -> parse (i + 1) (`Key_back :: acc)
          | _ -> parse (i + 1) acc
    in
    List.rev (parse 0 [])
  in

  Sys.set_signal Sys.sighup (Sys.Signal_handle (fun _ -> quit := true));

  let fd_ref : Unix.file_descr option ref = ref None in

  let try_connect () =
    let candidates =
      if !socket_path <> "" && !fd_ref = None then
        [!socket_path]
      else
        discover_socket_candidates ()
    in
    let rec try_candidates = function
      | [] -> None
      | p :: rest ->
          (try
            let fd = connect_and_watch p in
            fd_ref := Some fd;
            Some fd
          with Unix.Unix_error _ ->
            (try Unix.unlink p with _ -> ());
            try_candidates rest)
    in
    if List.length candidates > 1 then
      Printf.eprintf "Warning: multiple engine sockets found, trying newest first\n%!";
    try_candidates candidates
  in

  let disconnect fd =
    fd_ref := None;
    last_json := `Assoc [];
    (try let _ = Unix.write_substring fd "Q" 0 1 in () with _ -> ());
    (try Unix.close fd with _ -> ())
  in

  let rec wait_for_engine () =
    if !quit then ()
    else
      match try_connect () with
      | Some fd -> run_event_loop fd
      | None ->
          let (w, h) = match Notty_unix.winsize Unix.stdout with
            | Some (w, h) -> (w, h) | None -> (80, 24) in
          render_wait_screen w h "Waiting for engine...  (q to quit)";
          let ready, _, _ =
            try Unix.select [Unix.stdin] [] [] 2.0
            with Unix.Unix_error _ -> ([], [], [])
          in
          if List.mem Unix.stdin ready then begin
            let n = try Unix.read Unix.stdin input_buf 0 64 with _ -> 0 in
            if n = 0 then quit := true
            else begin
              for i = 0 to n - 1 do
                match Bytes.get input_buf i with
                | 'q' | 'Q' | '\027' -> quit := true
                | _ -> ()
              done
            end
          end;
          if not !quit then wait_for_engine ()

  and run_event_loop fd =
    let lost_connection = ref false in
    let last_render_time = ref (Unix.gettimeofday ()) in
    while not !quit && not !lost_connection do
      let now = Unix.gettimeofday () in
      let time_since_render = now -. !last_render_time in
      let target_frame_time = 0.05 in (* ~20 FPS perfectly locked to scroll speed *)
      let timeout = if time_since_render >= target_frame_time then 0.0 else target_frame_time -. time_since_render in

      let ready, _, _ =
        try Unix.select [fd; Unix.stdin] [] [] timeout
        with Unix.Unix_error _ -> ([], [], [])
      in

      if List.mem Unix.stdin ready then begin
        let n = try Unix.read Unix.stdin input_buf 0 64 with _ -> 0 in
        if n = 0 then quit := true
        else begin
          let actions = parse_key_bytes input_buf n in
          let assets = Holdings.get_selectable_assets !last_json in
          let asset_count = List.length assets in
          List.iter (fun action ->
            match !view_mode_ref with
            | `MainView ->
                (match action with
                 | `Key_quit -> quit := true
                 | `Key_up ->
                     if asset_count > 0 then
                       selected_index_ref := max 0 (!selected_index_ref - 1)
                 | `Key_down ->
                     if asset_count > 0 then
                       selected_index_ref := min (asset_count - 1) (!selected_index_ref + 1)
                 | `Key_enter ->
                     if asset_count > 0 then begin
                       let idx = min (asset_count - 1) (max 0 !selected_index_ref) in
                       let asset = List.nth assets idx in
                       view_mode_ref := `DetailView asset.key
                     end
                 | `Key_back -> quit := true
                 | _ -> ())
            | `DetailView curr_key ->
                (match action with
                 | `Key_quit -> quit := true
                 | `Key_back -> view_mode_ref := `MainView
                 | `Key_up | `Key_left ->
                     if asset_count > 0 then begin
                       let curr_idx = match find_asset_index curr_key assets with
                         | Some i -> i | None -> 0 in
                       let new_idx = if curr_idx > 0 then curr_idx - 1 else asset_count - 1 in
                       selected_index_ref := new_idx;
                       let new_asset = List.nth assets new_idx in
                       view_mode_ref := `DetailView new_asset.key
                     end
                 | `Key_down | `Key_right ->
                     if asset_count > 0 then begin
                       let curr_idx = match find_asset_index curr_key assets with
                         | Some i -> i | None -> 0 in
                       let new_idx = if curr_idx < asset_count - 1 then curr_idx + 1 else 0 in
                       selected_index_ref := new_idx;
                       let new_asset = List.nth assets new_idx in
                       view_mode_ref := `DetailView new_asset.key
                     end
                 | _ -> ())
          ) actions
        end
      end;

      if List.mem fd ready && not !quit then begin
        (try
          let msg = read_message fd in
          (try 
            let new_json = Yojson.Basic.from_string msg in
            last_json := new_json;
          with _ -> ())
        with
        | End_of_file ->
            disconnect fd;
            lost_connection := true
        | Unix.Unix_error _ ->
            disconnect fd;
            lost_connection := true
        | _ -> ())
      end;

      if not !quit && not !lost_connection then begin
        let now = Unix.gettimeofday () in
        if now -. !last_render_time >= target_frame_time then begin
          last_render_time := now;
          if not (stdout_alive ()) then begin
            disconnect fd;
            quit := true
          end else begin
            let (w, h) = match Notty_unix.winsize Unix.stdout with
              | Some (w, h) -> (w, h)
              | None -> (80, 24)
            in
            
            let draw buf =
              Buffer.add_string buf "\027[?2026h";
              Buffer.add_string buf "\027[H";
              let content_img =
                match !view_mode_ref with
                | `MainView ->
                    let uncropped = I.vcat [
                      Kpi_cards.render_kpi_cards w !last_json;
                      Ticker_feed.render_ticker w !last_json;
                      Holdings.render_strategies ~selected_index:(Some !selected_index_ref) w !last_json;
                      Recent_fills_feed.render_fills w !last_json;
                      Memory.render_memory w !last_json;
                      Latencies.render_latencies w !last_json;
                      Footer.render_footer w !last_json;
                    ] in
                    I.hsnap ~align:`Left w uncropped
                | `DetailView asset_key ->
                    let detail_img = Asset_graph.render_asset_detail w h asset_key !last_json in
                    I.hsnap ~align:`Left w detail_img
              in



              let c_h = I.height content_img in
              let c_w = I.width content_img in
              let content_img = 
                if c_h < h then I.vsnap ~align:`Middle h content_img
                else I.vsnap ~align:`Top h content_img
              in
              let content_img = 
                if c_w < w then I.hsnap ~align:`Middle w content_img
                else I.hsnap ~align:`Left w content_img
              in
              let img = I.(content_img </> I.char A.(bg c_bg) ' ' w h) in
              Render.to_buffer buf Cap.ansi (0, 0) (w, I.height img) img;
              Buffer.add_string buf "\027[J";
              Buffer.add_string buf "\027[?2026l"
            in
            let rendered = render_to_stdout_safe ~timeout_s:2 draw in
            if not rendered then begin
              disconnect fd;
              quit := true
            end else
              (try let _ = Unix.write_substring fd "P" 0 1 in () with _ -> ())
          end
        end
      end
    done;
    (match !fd_ref with Some fd -> disconnect fd | None -> ());
    if not !quit then wait_for_engine ()
  in

  wait_for_engine ()
