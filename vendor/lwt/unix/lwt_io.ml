open Lwt.Infix

exception Channel_closed of string

type input
type output

type 'a mode =
  | Input : input mode
  | Output : output mode

let input = Input
let output = Output

type 'a channel =
  { mode : 'a mode
  ; io : Lwt_bytes.t -> int -> int -> int Lwt.t
  ; close_fn : unit -> unit Lwt.t
  ; buffer : Lwt_bytes.t
  ; lock : Lwt_mutex.t
  ; mutable closed : bool
  ; mutable pos : int
  ; mutable len : int
  }

type input_channel = input channel
type output_channel = output channel

let default_buffer_size = 65536
let buffer_size channel = Lwt_bytes.length channel.buffer

let copy_bigarray_to_bytes buffer pos len =
  let bytes = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.unsafe_set bytes i (Bigarray.Array1.unsafe_get buffer (pos + i))
  done;
  bytes
;;

let make ?buffer ?(close = fun () -> Lwt.return_unit) ~mode io =
  let buffer =
    match buffer with
    | Some buffer -> buffer
    | None -> Lwt_bytes.create default_buffer_size
  in
  { mode
  ; io
  ; close_fn = close
  ; buffer
  ; lock = Lwt_mutex.create ()
  ; closed = false
  ; pos = 0
  ; len = 0
  }
;;

let of_fd
  : type m.
    ?buffer:Lwt_bytes.t
    -> ?close:(unit -> unit Lwt.t)
    -> mode:m mode
    -> Lwt_unix.file_descr
    -> m channel
  =
  fun ?buffer ?close ~mode fd ->
  let close =
    match close with
    | Some close -> close
    | None -> fun () -> Lwt_unix.close fd
  in
  match mode with
  | Input ->
    make ?buffer ~close ~mode:Input (fun buffer pos len ->
      Lwt_bytes.read fd buffer pos len)
  | Output ->
    make ?buffer ~close ~mode:Output (fun buffer pos len ->
      Lwt_bytes.write fd buffer pos len)
;;

let with_channel channel f =
  if channel.closed
  then Lwt.fail (Channel_closed "channel is closed")
  else
    Lwt_mutex.with_lock channel.lock (fun () ->
      if channel.closed then Lwt.fail (Channel_closed "channel is closed") else f ())
;;

(* Fill the input buffer with at least one more byte. Returns the number read. *)
let fill channel =
  if channel.len = 0 then channel.pos <- 0;
  if channel.pos + channel.len > buffer_size channel
  then (
    Lwt_bytes.unsafe_blit channel.buffer channel.pos channel.buffer 0 channel.len;
    channel.pos <- 0);
  let space = buffer_size channel - (channel.pos + channel.len) in
  if space = 0
  then Lwt.return 0
  else
    channel.io channel.buffer (channel.pos + channel.len) space
    >|= fun n ->
    channel.len <- channel.len + n;
    n
;;

let read_into_locked channel bytes offset length =
  if length = 0
  then Lwt.return 0
  else (
    let copy () =
      let n = min length channel.len in
      for i = 0 to n - 1 do
        Bytes.unsafe_set
          bytes
          (offset + i)
          (Bigarray.Array1.unsafe_get channel.buffer (channel.pos + i))
      done;
      channel.pos <- channel.pos + n;
      channel.len <- channel.len - n;
      n
    in
    if channel.len > 0
    then Lwt.return (copy ())
    else fill channel >|= fun n -> if n = 0 then 0 else copy ())
;;

let read_into_exactly_locked channel bytes offset length =
  let rec loop offset remaining =
    if remaining = 0
    then Lwt.return_unit
    else
      read_into_locked channel bytes offset remaining
      >>= fun n ->
      if n = 0 then Lwt.fail End_of_file else loop (offset + n) (remaining - n)
  in
  loop offset length
;;

let strip_cr line =
  let length = String.length line in
  if length > 0 && line.[length - 1] = '\r' then String.sub line 0 (length - 1) else line
;;

let read_line_opt_locked channel =
  let accumulator = Buffer.create 64 in
  let rec loop () =
    let start = channel.pos in
    let stop = channel.pos + channel.len in
    let rec scan i =
      if i >= stop
      then None
      else if Bigarray.Array1.unsafe_get channel.buffer i = '\n'
      then Some i
      else scan (i + 1)
    in
    match scan start with
    | Some newline ->
      let segment = newline - start in
      Buffer.add_bytes accumulator (copy_bigarray_to_bytes channel.buffer start segment);
      channel.pos <- newline + 1;
      channel.len <- stop - (newline + 1);
      Lwt.return_some (strip_cr (Buffer.contents accumulator))
    | None ->
      Buffer.add_bytes
        accumulator
        (copy_bigarray_to_bytes channel.buffer start channel.len);
      channel.pos <- channel.pos + channel.len;
      channel.len <- 0;
      fill channel
      >>= fun n ->
      if n = 0
      then
        if Buffer.length accumulator = 0
        then Lwt.return_none
        else Lwt.return_some (strip_cr (Buffer.contents accumulator))
      else loop ()
  in
  loop ()
;;

let read_locked channel count =
  match count with
  | Some count ->
    let count = max 0 count in
    if count = 0
    then Lwt.return ""
    else (
      let take () =
        let n = min count channel.len in
        let bytes = copy_bigarray_to_bytes channel.buffer channel.pos n in
        channel.pos <- channel.pos + n;
        channel.len <- channel.len - n;
        Bytes.to_string bytes
      in
      if channel.len > 0
      then Lwt.return (take ())
      else fill channel >|= fun n -> if n = 0 then "" else take ())
  | None ->
    let accumulator = Buffer.create 1024 in
    let rec loop () =
      if channel.len > 0
      then (
        Buffer.add_bytes
          accumulator
          (copy_bigarray_to_bytes channel.buffer channel.pos channel.len);
        channel.pos <- channel.pos + channel.len;
        channel.len <- 0;
        loop ())
      else
        fill channel
        >>= fun n -> if n = 0 then Lwt.return (Buffer.contents accumulator) else loop ()
    in
    loop ()
;;

let flush_locked channel =
  let rec loop () =
    if channel.len = 0
    then Lwt.return_unit
    else
      channel.io channel.buffer 0 channel.len
      >>= fun n ->
      if n <= 0
      then Lwt.fail (Channel_closed "output channel closed during flush")
      else (
        if n < channel.len
        then Lwt_bytes.unsafe_blit channel.buffer n channel.buffer 0 (channel.len - n);
        channel.len <- channel.len - n;
        loop ())
  in
  loop ()
;;

let write_locked channel bytes offset length =
  let rec loop offset length =
    if length = 0
    then Lwt.return_unit
    else if channel.len = buffer_size channel
    then flush_locked channel >>= fun () -> loop offset length
    else (
      let space = buffer_size channel - channel.len in
      let n = min space length in
      for i = 0 to n - 1 do
        Bigarray.Array1.unsafe_set
          channel.buffer
          (channel.len + i)
          (Bytes.unsafe_get bytes (offset + i))
      done;
      channel.len <- channel.len + n;
      loop (offset + n) (length - n))
  in
  loop offset length
;;

let flush channel = with_channel channel (fun () -> flush_locked channel)

let write channel string =
  with_channel channel (fun () ->
    write_locked channel (Bytes.of_string string) 0 (String.length string))
;;

let write_from_exactly channel bytes offset length =
  with_channel channel (fun () -> write_locked channel bytes offset length)
;;

let read_line_opt channel = with_channel channel (fun () -> read_line_opt_locked channel)
let read ?count channel = with_channel channel (fun () -> read_locked channel count)

let read_into channel bytes offset length =
  with_channel channel (fun () -> read_into_locked channel bytes offset length)
;;

let read_into_exactly channel bytes offset length =
  with_channel channel (fun () -> read_into_exactly_locked channel bytes offset length)
;;

let close : type a. a channel -> unit Lwt.t =
  fun channel ->
  if channel.closed
  then Lwt.return_unit
  else
    Lwt_mutex.with_lock channel.lock (fun () ->
      if channel.closed
      then Lwt.return_unit
      else (
        match channel.mode with
        | Output ->
          flush_locked channel
          >>= fun () ->
          channel.closed <- true;
          channel.close_fn ()
        | Input ->
          channel.closed <- true;
          channel.close_fn ()))
;;

let abort channel =
  channel.closed <- true;
  Lwt.catch (fun () -> channel.close_fn ()) (fun _ -> Lwt.return_unit)
;;
