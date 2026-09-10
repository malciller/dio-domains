(** Frame-clock animation toolkit.

    Provides a monotonic clock, easing functions, per-key numeric tweening,
    and decaying event flashes. The render loop uses [motion_pending] to decide
    whether another high-rate frame is worth drawing: setters mark motion while
    a value is still moving, and the loop resets the flag at the start of each
    frame. *)

(** Monotonic seconds since program start. Never jumps backwards on NTP
    adjustment, unlike [Unix.gettimeofday]. *)
let now () = Int64.to_float (Mtime_clock.elapsed_ns ()) /. 1_000_000_000.0

(** Global motion controls, wired from config/CLI. *)
let reduced_motion = ref false

let target_fps = ref 30.0
let idle_interval = ref 2.0

(** Set by [tween]/[flash] while anything is still moving; reset by the loop
    before each draw so it reflects that frame's remaining motion. *)
let motion_pending = ref false

let clamp01 x = if x < 0.0 then 0.0 else if x > 1.0 then 1.0 else x
let lerp a b t = a +. ((b -. a) *. t)

let ease_in_out t =
  let t = clamp01 t in
  t *. t *. (3.0 -. (2.0 *. t))
;;

let ease_out t =
  let t = clamp01 t in
  1.0 -. ((1.0 -. t) ** 3.0)
;;

(** Exponential approach toward [target] with time constant [tau] seconds. *)
let approach ~tau v target dt =
  if tau <= 0.0 then target else target +. ((v -. target) *. exp (-.dt /. tau))
;;

(** Per-key smoothed values, indexed by a caller-chosen key. *)
let tweens : (string, float) Hashtbl.t = Hashtbl.create 64

(** Seconds since the previous frame, clamped to a sane range. Set by
    [reset_frame] so every [tween] in a frame shares one delta time. *)
let current_dt = ref (1.0 /. 30.0)

let last_tick = ref 0.0

let tween ~key ~target ~tau =
  if !reduced_motion
  then (
    Hashtbl.replace tweens key target;
    target)
  else (
    let dt = !current_dt in
    let v =
      match Hashtbl.find_opt tweens key with
      | Some v -> v
      | None -> target
    in
    let v' = approach ~tau v target dt in
    Hashtbl.replace tweens key v';
    if abs_float (v' -. target) > 1e-3 then motion_pending := true;
    v')
;;

(** Decaying event flash: [1.0] while [active], then exponential decay with
    time constant [tau] once it goes inactive. *)
let flashes : (string, float) Hashtbl.t = Hashtbl.create 64

let flash ~key ~active ~tau =
  if !reduced_motion
  then if active then 1.0 else 0.0
  else (
    let t = now () in
    if active then Hashtbl.replace flashes key t;
    match Hashtbl.find_opt flashes key with
    | None -> 0.0
    | Some t0 ->
      let age = t -. t0 in
      if tau <= 0.0 || age < 0.0
      then 0.0
      else (
        let v = exp (-.age /. tau) in
        if v > 0.01 then motion_pending := true;
        v))
;;

(** Zero-argument convenience: is anything animating this frame? *)
let pending () = !motion_pending

(** Called once at the start of each frame: clears the motion flag used by the
    loop and advances the shared delta time. *)
let reset_frame () =
  motion_pending := false;
  let t = now () in
  current_dt
  := if !last_tick = 0.0
     then 1.0 /. max 1.0 !target_fps
     else max 0.0 (min 0.1 (t -. !last_tick));
  last_tick := t
;;

(** Last time fresh data arrived. Ambient motion (breathing fills, live
    badges) runs only for a short window after a snapshot, so the UI does not
    animate forever while the stream is idle or the session is detached. *)
let last_activity = ref 0.0

let note_activity () = last_activity := now ()
let active ?(window = 1.5) () = now () -. !last_activity < window

(** Smooth 0..1 breathing value for ambient effects. *)
let pulse () = 0.5 +. (0.5 *. sin (now () *. 3.0))
