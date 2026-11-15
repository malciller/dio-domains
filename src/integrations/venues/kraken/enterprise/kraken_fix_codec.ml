(** FIX 4.4 wire codec: framing, field access and message construction.

    FIX is a SOH-delimited tag=value protocol. Unlike the JSON WebSocket path there is no
    document tree to build: a received frame is scanned in place and each field is decoded
    on demand, so a frame costs one buffer blit and whatever values the caller keeps.

    Nothing here owns state or does I/O; the session layer ({!Kraken_fix_client}) decides
    what a frame means. *)

let section = "kraken_fix_codec"

(** Field delimiter (ASCII SOH, 0x01). *)
let soh = '\x01'

(* -- Received frames ------------------------------------------------- *)

(** A complete FIX frame inside the read buffer. [off] points at '8' and [len] spans
    through the trailing SOH after the checksum. *)
type view =
  { buf : Bytes.t
  ; off : int
  ; len : int
  }

let view_length v = v.len
let byte v i = Bytes.unsafe_get v.buf (v.off + i)

(** Decodes a run of decimal digits. Stops at the first non-digit; callers bound the span
    first. *)
let int_of_digits buf off len =
  let rec loop i acc =
    if i >= len
    then acc
    else (
      let c = Bytes.unsafe_get buf (off + i) in
      if c >= '0' && c <= '9' then loop (i + 1) ((acc * 10) + (Char.code c - 48)) else acc)
  in
  loop 0 0
;;

let span_string v off len = Bytes.sub_string v.buf (v.off + off) len

(* Scans for [tag]=<value> and returns the value span. Field counts on the messages this
   client receives are in the tens, so a linear scan beats any lookup structure. *)
let field_span v tag =
  let stop = v.len in
  let rec loop i =
    if i >= stop
    then None
    else (
      let semi =
        match Bytes.index_from_opt v.buf (v.off + i) soh with
        | Some j -> j - v.off
        | None -> stop
      in
      (* Parse the tag digits. *)
      let rec tag_end j =
        if j >= semi
        then j
        else if Bytes.unsafe_get v.buf (v.off + j) = '='
        then j
        else tag_end (j + 1)
      in
      let eq = tag_end i in
      let parsed = if eq = i then -1 else int_of_digits v.buf (v.off + i) (eq - i) in
      if parsed = tag then Some (eq + 1, semi - eq - 1) else loop (semi + 1))
  in
  loop 0
;;

let field v tag =
  match field_span v tag with
  | Some (off, len) -> Some (span_string v off len)
  | None -> None
;;

let has_field v tag = Option.is_some (field_span v tag)

let field_float v tag =
  match field_span v tag with
  | Some (off, len) when len > 0 ->
    (try Some (float_of_string (span_string v off len)) with
     | _ -> None)
  | _ -> None
;;

let field_int v tag =
  match field_span v tag with
  | Some (off, len) when len > 0 -> Some (int_of_digits v.buf (v.off + off) len)
  | _ -> None
;;

let field_char v tag =
  match field_span v tag with
  | Some (off, 1) -> Some (Bytes.unsafe_get v.buf (v.off + off))
  | _ -> None
;;

(** [35] - the message type; every routing decision starts here. *)
let msg_type v = field v 35

(** [34] - the sender's sequence number. *)
let msg_seq_num v = field_int v 34

(** [43] - "the sender believes this may be a duplicate". *)
let poss_dup v = field_char v 43 = Some 'Y'

(** Modulo-256 checksum over every byte before the [10=] trailer. *)
let computed_checksum v =
  let stop = v.len - 7 in
  let rec loop i acc =
    if i >= stop then acc else loop (i + 1) ((acc + Char.code (byte v i)) land 0xFF)
  in
  loop 0 0
;;

let stored_checksum v = int_of_digits v.buf (v.off + v.len - 4) 3

(** Checks the trailer against the body. A mismatch on a fill report is logged and the
    frame is still applied: a corrupted-but-parseable fill is better than a silently lost
    position. *)
let checksum_ok v = computed_checksum v = stored_checksum v

(* -- Message construction -------------------------------------------- *)

(** Outgoing message under construction. Reused per domain via {!thread_builder} to keep
    the order path allocation-free apart from the finished message string. *)
type builder = { body : Buffer.t }

let create () = { body = Buffer.create 512 }
let reset b = Buffer.clear b.body

let write_tag b tag =
  Buffer.add_string b.body (string_of_int tag);
  Buffer.add_char b.body '='
;;

let str b tag value =
  write_tag b tag;
  Buffer.add_string b.body value;
  Buffer.add_char b.body soh
;;

let char b tag value =
  write_tag b tag;
  Buffer.add_char b.body value;
  Buffer.add_char b.body soh
;;

let int b tag value =
  write_tag b tag;
  Buffer.add_string b.body (string_of_int value);
  Buffer.add_char b.body soh
;;

(** Formats a float for FIX without exponent notation. [precision] caps the fraction;
    trailing zeros are dropped so 0.0010 becomes 0.001 and 84000.00000000 becomes 84000,
    matching the decimal form Kraken documents in its message examples. *)
let format_float ?(precision = 8) value =
  let s = Printf.sprintf "%.*f" (max 0 precision) value in
  match String.index_opt s '.' with
  | None -> s
  | Some dot ->
    let rec last_nonzero i =
      if i <= dot then None else if s.[i] = '0' then last_nonzero (i - 1) else Some i
    in
    (match last_nonzero (String.length s - 1) with
     | None -> String.sub s 0 dot
     | Some i -> String.sub s 0 (i + 1))
;;

let float b ?precision tag_value value =
  write_tag b tag_value;
  Buffer.add_string b.body (format_float ?precision value);
  Buffer.add_char b.body soh
;;

let bool b tag_value value = char b tag_value (if value then 'Y' else 'N')
let buf_int b value = Buffer.add_string b.body (string_of_int value)

let buf_float ?(precision = 8) b value =
  Buffer.add_string b.body (format_float ~precision value)
;;

(** Completes the frame: prepends the standard header, appends [10], and returns the bytes
    to write.

    [9 BodyLength] counts the bytes after its own SOH field terminator up to and including
    the SOH that precedes [10] - that is, the header and body fields only, never the
    trailer. Kraken's published sample confirms it: the Limit order example there states
    [9=148] for a 148-byte header+body block and the trailer is not part of the count.

    [poss_dup] and [orig_sending_time] are only set when replaying a journalled frame in
    answer to a [ResendRequest]: [43=Y] tells the gateway not to treat the reused sequence
    number as a protocol error, and [122] carries the original send time. *)
let finish
  b
  ~msg_type
  ~seq
  ~sender
  ~target
  ~sending_time
  ?(poss_dup = false)
  ?orig_sending_time
  ()
  =
  let body = Buffer.create (Buffer.length b.body + 64) in
  Buffer.add_string body "35=";
  Buffer.add_char body msg_type;
  Buffer.add_char body soh;
  Buffer.add_string body "34=";
  Buffer.add_string body (string_of_int seq);
  Buffer.add_char body soh;
  Buffer.add_string body "49=";
  Buffer.add_string body sender;
  Buffer.add_char body soh;
  Buffer.add_string body "56=";
  Buffer.add_string body target;
  Buffer.add_char body soh;
  Buffer.add_string body "52=";
  Buffer.add_string body sending_time;
  Buffer.add_char body soh;
  if poss_dup
  then (
    Buffer.add_string body "43=Y";
    Buffer.add_char body soh;
    match orig_sending_time with
    | Some original ->
      Buffer.add_string body "122=";
      Buffer.add_string body original;
      Buffer.add_char body soh
    | None -> ());
  Buffer.add_buffer body b.body;
  let out = Buffer.create (Buffer.length body + 32) in
  Buffer.add_string out "8=FIX.4.4";
  Buffer.add_char out soh;
  Buffer.add_string out "9=";
  Buffer.add_string out (string_of_int (Buffer.length body));
  Buffer.add_char out soh;
  Buffer.add_buffer out body;
  let checksum =
    let sum = ref 0 in
    for i = 0 to Buffer.length out - 1 do
      sum := (!sum + Char.code (Buffer.nth out i)) land 0xFF
    done;
    !sum
  in
  Buffer.add_string out "10=";
  Buffer.add_string out (Printf.sprintf "%03d" checksum);
  Buffer.add_char out soh;
  Buffer.contents out
;;

(* Per-domain builder: the order path never shares one across domains. *)
[@@@alert "-unsafe_multidomain"]

let thread_builder_key = Domain.DLS.new_key (fun () -> create ())
let thread_builder () = Domain.DLS.get thread_builder_key

[@@@alert "+unsafe_multidomain"]

(* -- Timestamps ------------------------------------------------------ *)

(** Days since the Unix epoch for a proleptic Gregorian date (Hinnant's
    [days_from_civil]). FIX timestamps are UTC, and [Unix.mktime] would apply the process
    time zone. *)
let days_from_civil ~year ~month ~day =
  let y = if month <= 2 then year - 1 else year in
  let era = if y >= 0 then y / 400 else (y - 399) / 400 in
  let yoe = y - (era * 400) in
  let mp = (month + 9) mod 12 in
  let doy = (((153 * mp) + 2) / 5) + day - 1 in
  let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
  (era * 146_097) + doe - 719_468
;;

(** [YYYYMMDD-HH:MM:SS.uuu] as used by SendingTime, TransactTime and the execution
    reports. *)
let utc_timestamp_of_unix t =
  (* Round to whole milliseconds first: a float second carries the fractional part as a
     binary fraction, so truncating it loses a millisecond about half the time. *)
  let total_ms = Int64.of_float (Float.round (t *. 1000.0)) in
  let tm = Unix.gmtime (Int64.to_float (Int64.div total_ms 1000L)) in
  let millis = Int64.to_int (Int64.rem total_ms 1000L) in
  Printf.sprintf
    "%04d%02d%02d-%02d:%02d:%02d.%03d"
    (tm.Unix.tm_year + 1900)
    (tm.Unix.tm_mon + 1)
    tm.Unix.tm_mday
    tm.Unix.tm_hour
    tm.Unix.tm_min
    tm.Unix.tm_sec
    millis
;;

(** Accepts [YYYYMMDD-HH:MM:SS] with an optional fraction. Returns [None] rather than
    raising: a malformed venue timestamp must not abort an execution report. *)
let unix_of_utc_timestamp s =
  let len = String.length s in
  if len < 17
  then None
  else (
    let num off n = int_of_string_opt (String.sub s off n) in
    match num 0 4, num 4 2, num 6 2, num 9 2, num 12 2, num 15 2 with
    | Some year, Some month, Some day, Some hour, Some minute, Some second ->
      let fraction =
        if len > 17 && s.[17] = '.'
        then float_of_string_opt ("0." ^ String.sub s 18 (len - 18))
        else None
      in
      let days = days_from_civil ~year ~month ~day in
      Some
        (float_of_int (days * 86_400)
         +. float_of_int ((((hour * 60) + minute) * 60) + second)
         +. Option.value fraction ~default:0.0)
    | _ -> None)
;;

(* -- Framing --------------------------------------------------------- *)

(** Accumulates raw socket bytes and hands out one frame at a time. The buffer is
    compacted on [feed] rather than on [next], so a burst of small frames never shifts the
    tail more than once. *)
type framer =
  { mutable frame_buf : Bytes.t
  ; mutable frame_len : int
  ; mutable frame_pos : int
  }

let framer_create ~capacity =
  { frame_buf = Bytes.create (max capacity 4096); frame_len = 0; frame_pos = 0 }
;;

let framer_pending f = f.frame_len - f.frame_pos

let framer_reset f =
  f.frame_pos <- 0;
  f.frame_len <- 0
;;

(** Appends [count] bytes. Raises nothing: the buffer grows geometrically. *)
let framer_feed f (src : Bytes.t) ~off ~count =
  if f.frame_pos > 0
  then (
    let live = f.frame_len - f.frame_pos in
    if live > 0 then Bytes.blit f.frame_buf f.frame_pos f.frame_buf 0 live;
    f.frame_len <- live;
    f.frame_pos <- 0);
  let needed = f.frame_len + count in
  if needed > Bytes.length f.frame_buf
  then (
    let rec grow n = if n >= needed then n else grow (n * 2) in
    let bigger = Bytes.create (grow (Bytes.length f.frame_buf)) in
    Bytes.blit f.frame_buf 0 bigger 0 f.frame_len;
    f.frame_buf <- bigger);
  Bytes.blit src off f.frame_buf f.frame_len count;
  f.frame_len <- f.frame_len + count
;;

(* Locates the next complete frame. The trailer is [10=ddd<SOH>], and no field value in
   the messages Kraken sends can contain a raw SOH, so the first "10=" preceded by a SOH
   is the trailer. *)
let framer_next f : view option =
  let buf = f.frame_buf in
  let len = f.frame_len in
  let rec seek i =
    if i >= len
    then None
    else if Bytes.unsafe_get buf i = '8'
            && i + 1 < len
            && Bytes.unsafe_get buf (i + 1) = '='
    then scan i i
    else seek (i + 1)
  and scan start i =
    if i >= len
    then (
      (* Incomplete: keep the bytes from [start] and wait for the rest. *)
      f.frame_pos <- start;
      None)
    else if Bytes.unsafe_get buf i = soh
            && i + 4 <= len
            && Bytes.unsafe_get buf (i + 1) = '1'
            && Bytes.unsafe_get buf (i + 2) = '0'
            && Bytes.unsafe_get buf (i + 3) = '='
    then
      if i + 8 > len
      then (
        f.frame_pos <- start;
        None)
      else (
        let close = i + 7 in
        if Bytes.unsafe_get buf close <> soh
        then scan start (i + 1)
        else (
          f.frame_pos <- close + 1;
          Some { buf; off = start; len = close + 1 - start }))
    else scan start (i + 1)
  in
  seek f.frame_pos
;;

let framer_available f = f.frame_len - f.frame_pos
