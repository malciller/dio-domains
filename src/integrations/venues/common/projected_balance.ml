(** Event-sourced projection of a venue's tradeable figure.

    A fill moves the figure before the venue's balance frame reports it. The frame is
    absolute truth, so it clears the projection; a TTL bounds a delta whose frame never
    arrives. Ids dedupe a replayed fill.

    Holds are the other half: a venue figure netted by holds is only current once the
    venue releases them, so callers derive holds from their own open-orders store. *)

type t =
  { projected : float Atomic.t
  ; projected_at : float Atomic.t
  ; applied_ids : unit Concurrency.Fifo_index.t
  ; mutex : Mutex.t
  }

let ttl_s = 5.0

let create ~label () =
  { projected = Atomic.make 0.0
  ; projected_at = Atomic.make 0.0
  ; applied_ids = Concurrency.Fifo_index.create ~label ~cap:512 ()
  ; mutex = Mutex.create ()
  }
;;

(** Fills applied since the last balance frame. Zero once the projection outlives [ttl_s]:
    the frame should have landed by then, and a projection kept past it would double count
    a fill the frame already carries. *)
let get ?(now = Unix.time ()) t =
  let p = Atomic.get t.projected in
  if p = 0.0
  then 0.0
  else (
    let at = Atomic.get t.projected_at in
    if at > 0.0 && now -. at <= ttl_s then p else 0.0)
;;

(** The venue's frame supersedes every projected delta. *)
let clear t =
  Atomic.set t.projected 0.0;
  Atomic.set t.projected_at 0.0
;;

let apply t ~delta ~id =
  if Float.is_finite delta && delta <> 0.0
  then (
    Mutex.lock t.mutex;
    let seen =
      (not (String.equal id ""))
      && Concurrency.Fifo_index.find_opt t.applied_ids id <> None
    in
    if not seen
    then (
      if not (String.equal id "")
      then Concurrency.Fifo_index.set t.applied_ids ~key:id ~value:();
      Atomic.set t.projected (Atomic.get t.projected +. delta);
      Atomic.set t.projected_at (Unix.time ()));
    Mutex.unlock t.mutex;
    if not seen then Concurrency.Oracle_wakeup.notify_balance ())
;;

(** Tradeable quote from the venue's gross total, the projected fills, and the quote tied
    in our own resting buys. A gross total minus our holds is event-ordered: a cancel
    raises it on the ack, not on the venue's next balance frame. *)
let compose ~(total : float) ~(projected : float) ~(held : float) : float =
  Float.max 0.0 (total +. projected -. held)
;;
