(** Where dio writes persistent data.

    One definition, so the latency journal, the oracle history cache and the per-store
    writers cannot disagree about where "data/" is. The Docker image mounts the host data
    directory at [/app/data] and runs with a read-only root filesystem, so a writer that
    resolves this wrong fails instead of silently writing somewhere that evaporates with
    the container.

    This lives in the bottom-of-the-graph library because both [dio.oracle] and
    [dio.latency_profiling] need it, and neither can depend on [dio.engine] (where the
    config module lives). *)

(** Base data directory: [/app/data] in the Docker image, [./data] elsewhere. *)
let data_dir () = if Sys.file_exists "/app" then "/app/data" else "data"

(** [subdir "latency"] is [<data_dir>/latency]. *)
let subdir name = Filename.concat (data_dir ()) name
