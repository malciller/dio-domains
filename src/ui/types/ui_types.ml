(** Common UI types shared across UI modules *)

open Telemetry

(** Cached telemetry snapshot for UI consumption *)
type telemetry_snapshot = {
  uptime: float;
  metrics: metric list;
  categories: (string * metric list) list;
  timestamp: float;
}

(** System statistics snapshot *)
type system_stats = {
  cpu_usage: float;
  core_usages: float list;  (* Per-core CPU usage percentages *)
  cpu_cores: int option;    (* Number of CPU cores *)
  cpu_vendor: string option;  (* CPU vendor as string *)
  cpu_model: string option;   (* CPU model as string *)
  memory_total: int;  (* KB *)
  memory_used: int;   (* KB *)
  memory_free: int;   (* KB *)
  swap_total: int;    (* KB *)
  swap_used: int;     (* KB *)
  load_avg_1: float;
  load_avg_5: float;
  load_avg_15: float;
  processes: int;
}
