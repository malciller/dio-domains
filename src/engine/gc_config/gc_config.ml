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

(* OxCaml marks [Gc.create_alarm] and [Domain.DLS] as unsafe across domains. The alarm
   callback only bumps an atomic, so it is safe; the alarm is installed once per domain. *)
[@@@alert "-unsafe_multidomain"]
[@@@alert "-do_not_spawn_domains"]

let applier : (unit -> unit) ref = ref (fun () -> ())

(** Global count of completed major collections across every domain. A major collection
    pauses the process, so a canary on another domain can attribute a long gap by this
    delta. A per-domain [Gc.stat] cannot see another domain's collection. *)
let major_gc_epoch_cell = Atomic.make 0

let major_gc_epoch () = Atomic.get major_gc_epoch_cell
let alarm_installed = Domain.DLS.new_key (fun () -> false)
let note_major_gc () = ignore (Atomic.fetch_and_add major_gc_epoch_cell 1)

(** Install the major-GC alarm once per domain. GC-alarm context forbids locks and
    allocation, which is why the callback only touches an atomic. *)
let install_alarm () =
  if not (Domain.DLS.get alarm_installed)
  then (
    Domain.DLS.set alarm_installed true;
    ignore (Gc.create_alarm note_major_gc))
;;

(** [install f] registers the engine's GC applier. Idempotent. *)
let install f = applier := f

(** [apply ()] applies the configured GC parameters to the calling domain. No-op if the
    engine has not installed an applier yet. Always arms the major-GC alarm: background
    domains that never call the engine applier still need their collections counted. *)
let apply () =
  !applier ();
  install_alarm ()
;;
