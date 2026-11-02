(* Out-of-process persistence helper.

   Owns the state-file JSON, fsync and rename so that work never lands in the trading
   process's OCaml 5 runtime (whose stop-the-world pauses also stop pinned trading
   domains). Started as a sidecar; the engine degrades to in-process writes when this is
   not running. See [Persist_ipc] for the wire format and the liveness contract. *)

let () = Dio_persistence.Persist_ipc.serve ()
