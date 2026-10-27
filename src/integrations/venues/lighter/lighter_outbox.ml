(** Ordered submission queue for signed transactions.

    Lighter rejects any tx whose nonce is not strictly greater than the last one it
    accepted, so nonces must reach the exchange in allocation order. Submitters enqueue
    and wait only for their own tx to be written; one writer signs and sends in FIFO
    order, so allocation order equals wire order and a burst leaves back-to-back instead
    of N round-trips apart. It drains up to [max_batch] payloads per [send] call, turning
    a ladder burst into one transport frame. [send] resolves on write; execution is
    confirmed from the account order stream. *)

open Lwt.Infix

type 'a request =
  { payload : 'a
  ; reply : (string, string) result Lwt.u
  }

type 'a t =
  { queue : 'a request Queue.t
  ; cond : unit Lwt_condition.t
  ; max_batch : int
  ; send : 'a list -> (string, string) result list Lwt.t
  }

let take t n =
  let rec go acc n =
    if n <= 0 || Queue.is_empty t.queue
    then List.rev acc
    else go (Queue.pop t.queue :: acc) (n - 1)
  in
  go [] n
;;

let rec writer_loop t =
  if Queue.is_empty t.queue
  then Lwt_condition.wait t.cond >>= fun () -> writer_loop t
  else (
    let batch = take t t.max_batch in
    let payloads = List.map (fun request -> request.payload) batch in
    Lwt.catch
      (fun () -> t.send payloads)
      (fun exn ->
        let err = Printexc.to_string exn in
        Lwt.return (List.map (fun _ -> Error err) payloads))
    >>= fun results ->
    List.iteri
      (fun i { reply; _ } ->
        let result =
          match List.nth_opt results i with
          | Some r -> r
          | None -> Error "outbox: send returned fewer results than payloads"
        in
        Lwt.wakeup_later reply result)
      batch;
    writer_loop t)
;;

(** [send] is called by the writer, never concurrently with itself, and must return one
    result per payload in input order. A raising [send] fails its whole batch and the
    queue keeps draining. *)
let create ?(max_batch = 1) ~send () =
  let t = { queue = Queue.create (); cond = Lwt_condition.create (); max_batch; send } in
  Lwt.async (fun () -> writer_loop t);
  t
;;

(** Enqueues [payload]; resolves once the writer has attempted it. *)
let submit t payload =
  let reply, resolver = Lwt.wait () in
  Queue.push { payload; reply = resolver } t.queue;
  Lwt_condition.broadcast t.cond ();
  reply
;;
