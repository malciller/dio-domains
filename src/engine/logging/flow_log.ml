(** Decision-flow telemetry: a throttled, structured narrative of the trading loop.

    Where {!Logging} owns the *transport* (async drain, columns, colors, the [┆] gutter),
    this module owns the *content* of the high-level "what is the engine deciding right
    now" stream. It renders a multi-line block — a headline plus aligned [label value]
    rows — and hands it to {!Logging.info}, which draws the gutter and section color.

    Discipline (the difference between a showcase and noise):
    - {!digest} deduplicates: an identical decision signature for the same key is dropped
      unless [heartbeat_s] has elapsed, and never fires more often than [min_interval_s].
      Repeated steady-state cycles collapse instead of repeating.
    - {!event} is for genuinely discrete occurrences (an order was sent, a fill landed):
      it always fires, because those are the moments worth showing.
    - The whole layer is opt-in ([config.enabled], or [DIO_FLOW_LOG=1]); when disabled the
      per-cycle cost is a single boolean read.

    Colors are embedded in the message body only when the transport has them enabled, so
    piped/non-TTY output stays clean and aligned. *)

(* ---- Categories ---------------------------------------------------------- *)

(** The kind of decision step a block represents. Drives the glyph, the tag, and the
    accent color so a scan down the column separates signals from sends from fills. *)
type category =
  | Quote
  | Signal
  | Gate
  | Send
  | Amend
  | Cancel
  | Fill
  | Risk
  | Pulse

let category_glyph = function
  | Quote -> "↯"
  | Signal -> "◢"
  | Gate -> "⚑"
  | Send -> "▸"
  | Amend -> "↻"
  | Cancel -> "✕"
  | Fill -> "▲"
  | Risk -> "⚠"
  | Pulse -> "◈"
;;

let category_tag = function
  | Quote -> "TICK"
  | Signal -> "SIGNAL"
  | Gate -> "GATE"
  | Send -> "SEND"
  | Amend -> "AMEND"
  | Cancel -> "CANCEL"
  | Fill -> "FILL"
  | Risk -> "RISK"
  | Pulse -> "PULSE"
;;

(* Accent hues avoid the severity colors (31 red, 32 green, 33 yellow) used by the level
   column, except Cancel/Risk which intentionally borrow red/yellow for semantics. *)
let category_color = function
  | Quote -> 94 (* bright blue *)
  | Signal -> 96 (* bright cyan *)
  | Gate -> 93 (* bright yellow *)
  | Send -> 92 (* bright green *)
  | Amend -> 96
  | Cancel -> 91 (* bright red *)
  | Fill -> 92
  | Risk -> 93
  | Pulse -> 95 (* bright magenta *)
;;

(* ---- Configuration ------------------------------------------------------- *)

type config =
  { enabled : bool
  ; min_interval_s : float
  (** Floor on how often one key may emit a digest, regardless of change. *)
  ; heartbeat_s : float
  (** Emit one digest per key at least this often even when nothing changed, so a quiet
      but live symbol still shows motion. *)
  ; pulse_s : float (** Cross-symbol pulse cadence; [<= 0.] disables it. *)
  ; price_ticks : bool
  (** Stream every top-of-book update as a TICK block. High volume by design: this is the
      raw market-data firehose, not a deduplicated digest. *)
  }

let default_config =
  { enabled = false
  ; min_interval_s = 0.2
  ; heartbeat_s = 5.0
  ; pulse_s = 5.0
  ; price_ticks = true
  }
;;

(* Read once at startup (before domains spawn) and read-only thereafter; a plain ref is
   the cheapest possible hot-path check. *)
let config = ref default_config
let configure c = config := c
let enabled () = !config.enabled
let get_config () = !config

(* ---- Throttle / dedup state ---------------------------------------------- *)

(* key -> (last_emit_wall, last_signature). Guarded because digests are emitted from many
   domains concurrently. Only touched while enabled. *)
let last_emit : (string, float * string) Hashtbl.t = Hashtbl.create 32
let last_emit_mutex = Mutex.create ()

(** [true] when [key] may emit now: either the signature changed since the last emit (and
    the [min_interval_s] floor has passed) or the [heartbeat_s] window elapsed. Records
    the emit so the next call throttles against it. *)
let should_emit ~key ~signature =
  let c = !config in
  let now = Unix.gettimeofday () in
  Mutex.lock last_emit_mutex;
  let decision =
    match Hashtbl.find_opt last_emit key with
    | None -> true
    | Some (last_t, last_sig) ->
      let elapsed = now -. last_t in
      let changed = not (String.equal last_sig signature) in
      elapsed >= c.min_interval_s && (changed || elapsed >= c.heartbeat_s)
  in
  if decision then Hashtbl.replace last_emit key (now, signature);
  Mutex.unlock last_emit_mutex;
  decision
;;

(* ---- Rendering ----------------------------------------------------------- *)

let pad_to n s =
  let len = String.length s in
  if len >= n then s else s ^ String.make (n - len) ' '
;;

(* Wrap [s] in an ANSI color when the transport has colors on, else return it unchanged. *)
let colorize enabled code s =
  if enabled then Printf.sprintf "\027[%dm%s\027[0m" code s else s
;;

let dim = colorize

(** Render a block: a colored [glyph TAG  headline] header followed by one aligned
    [label value] row per field. The transport converts the [\n]-separated continuation
    lines into [┆]-gutter rows, so this returns plain newlines and no leading indent. *)
let render ~category ~headline ~fields =
  let colors = Logging.colors_enabled () in
  let accent = category_color category in
  let label_w = List.fold_left (fun acc (l, _) -> max acc (String.length l)) 0 fields in
  let head =
    Printf.sprintf
      "%s %s  %s"
      (colorize colors accent (category_glyph category))
      (colorize colors accent (category_tag category))
      headline
  in
  let rows =
    List.map
      (fun (label, value) ->
        Printf.sprintf "%s %s" (colorize colors 90 (pad_to label_w label)) value)
      fields
  in
  String.concat "\n" (head :: rows)
;;

(** Emit the block to the INFO stream. The dashboard's log panel captures every log line
    at the transport layer ({!Logging}), so no separate flow ring is kept here. *)
let output ~section ~category ~headline ~fields =
  if enabled () then Logging.info ~section (render ~category ~headline ~fields)
;;

(** A discrete, always-shown event (order sent, fill landed). *)
let event = output

(** Like {!output} but throttled: skip when [signature] is unchanged since the last emit
    for [key] and the heartbeat window has not elapsed. [fields] is a thunk so the row
    strings are only built on the cycle that actually emits. *)
let digest ~section ~key ~signature ~category ~headline ~fields =
  if enabled () && should_emit ~key ~signature
  then output ~section ~category ~headline ~fields:(fields ())
;;

(** Cross-symbol heartbeat. *)
let pulse ~headline ~fields = output ~section:"pulse" ~category:Pulse ~headline ~fields

(** Stream one top-of-book update (the raw market-data firehose). Unthrottled: every tick
    the domain saw is shown. Gated by [price_ticks] so the decision digests can stay on
    while the tick stream is silenced. *)
let tick ~section ~headline ~fields =
  if !config.price_ticks then output ~section ~category:Quote ~headline ~fields
;;

(** Clear throttle state. Used by tests. *)
let reset () =
  Mutex.lock last_emit_mutex;
  Hashtbl.reset last_emit;
  Mutex.unlock last_emit_mutex
;;
