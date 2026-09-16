(** Process-wide GC-config applier.

    config.json is the single source of truth for GC tuning, but the libraries that spawn
    domains ([concurrency], [persistence], [latency_profiling]) sit below the engine and
    cannot depend on [Config]. [Config.apply_gc_config] installs itself here at module
    initialization; every domain-spawning call site invokes {!apply} at the top of the
    domain so no domain silently inherits the runtime default. Previously the Dockerfile's
    [OCAMLRUNPARAM=s=33554432] leaked a 256MB minor heap into the parse and persistence
    domains (which never called [Config.apply_gc_config]) and caused multi-ms pauses.

    Before installation the hook is a no-op; the engine installs it during startup, well
    before any of these domains spawn. *)

let applier : (unit -> unit) ref = ref (fun () -> ())

(** [install f] registers the engine's GC applier. Idempotent. *)
let install f = applier := f

(** [apply ()] applies the configured GC parameters to the calling domain. No-op if the
    engine has not installed an applier yet. *)
let apply () = !applier ()
