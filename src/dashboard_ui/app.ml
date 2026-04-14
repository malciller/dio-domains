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

let render_to_stdout (draw : out_channel -> unit) =
  let tmp_path = Filename.temp_file "dio_dash_" ".ansi" in
  let oc = open_out_bin tmp_path in
  (try
     draw oc;
     close_out oc
   with exn ->
     close_out_noerr oc;
     Sys.remove tmp_path;
     raise exn);
  let ic = open_in_bin tmp_path in
  let len = in_channel_length ic in
  let frame = really_input_string ic len in
  close_in ic;
  Sys.remove tmp_path;
  let rec go off rem =
    if rem > 0 then
      let n = Unix.write_substring Unix.stdout frame off rem in
      go (off + n) (rem - n)
  in
  go 0 len

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
    render_to_stdout draw;
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
  render_to_stdout (fun oc ->
    output_string oc "\027[?2026h";
    output_string oc "\027[H";
    Notty_unix.output_image ~cap:Cap.ansi ~fd:oc img;
    output_string oc "\027[J";
    output_string oc "\027[?2026l")

let run () =
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
      let target_frame_time = 0.033 in
      let timeout = if time_since_render >= target_frame_time then 0.0 else target_frame_time -. time_since_render in

      let ready, _, _ =
        try Unix.select [fd; Unix.stdin] [] [] timeout
        with Unix.Unix_error _ -> ([], [], [])
      in

      if List.mem Unix.stdin ready then begin
        let n = try Unix.read Unix.stdin input_buf 0 64 with _ -> 0 in
        if n = 0 then quit := true
        else begin
          let rec check_bytes i =
            if i >= n then ()
            else begin
              (match Bytes.get input_buf i with
               | 'q' | 'Q' -> quit := true
               | '\027' ->
                   if i + 1 >= n then quit := true
               | _ -> ());
              check_bytes (i + 1)
            end
          in
          check_bytes 0
        end
      end;

      if List.mem fd ready && not !quit then begin
        (try
          let msg = read_message fd in
          (try 
            let new_json = Yojson.Basic.from_string msg in
            let old_json = !last_json in
            last_json := new_json;
            
            (* Check for order fills by comparing sell_count *)
            let get_sell_counts json =
              match Theme.(json |?> "strategies") with
              | `Assoc l ->
                  List.map (fun (sym, data) ->
                    let strat = Theme.(data |?> "strategy") in
                    sym, Theme.(strat |?> "sell_count" |> to_int_d 0)
                  ) l
              | _ -> []
            in
            let old_counts = get_sell_counts old_json in
            let new_counts = get_sell_counts new_json in
            let filled = List.exists (fun (sym, new_c) ->
              try let old_c = List.assoc sym old_counts in old_c <> new_c
              with Not_found -> false
            ) new_counts in
            if filled then begin
              print_char '\007';
              flush stdout
            end
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
            
            let draw oc =
              output_string oc "\027[?2026h";
              output_string oc "\027[H";
              let content_img =
                  I.vcat [
                    Ticker_feed.render_ticker w !last_json;
                    Recent_fills_feed.render_fills w !last_json;
                    Memory.render_memory w !last_json;
                    Holdings.render_strategies w !last_json;
                    Latencies.render_latencies w !last_json;
                    Header.render_header w !last_json;
                  ]
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
              Notty_unix.output_image ~cap:Cap.ansi ~fd:oc img;
              output_string oc "\027[J";
              output_string oc "\027[?2026l"
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
