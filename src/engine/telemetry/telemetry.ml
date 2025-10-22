(** Lightweight Telemetry System for Performance Tracking *)

let section = "telemetry"

(** Metric types *)
type metric_type = 
  | Counter of int ref
  | Gauge of float ref
  | Histogram of float list ref

type metric = {
  name: string;
  metric_type: metric_type;
  labels: (string * string) list;
  created_at: float;
  mutable last_updated: float;
}

(** Global metrics store *)
let metrics : (string, metric) Hashtbl.t = Hashtbl.create 128
let metrics_mutex = Mutex.create ()
let metrics_changed_condition = Lwt_condition.create ()

(** Helper to generate metric key *)
let metric_key name labels =
  let label_str = 
    labels 
    |> List.sort (fun (k1, _) (k2, _) -> String.compare k1 k2)
    |> List.map (fun (k, v) -> k ^ "=" ^ v)
    |> String.concat ","
  in
  if label_str = "" then name else name ^ "{" ^ label_str ^ "}"

(** Counter - incrementing metric *)
let counter name ?(labels=[]) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  let metric = 
    match Hashtbl.find_opt metrics key with
    | Some m -> m
    | None ->
        let m = {
          name;
          metric_type = Counter (ref 0);
          labels;
          created_at = Unix.time ();
          last_updated = Unix.time ();
        } in
        Hashtbl.add metrics key m;
        m
  in
  Mutex.unlock metrics_mutex;
  metric

let inc_counter metric ?(value=1) () =
  match metric.metric_type with
  | Counter r ->
      Mutex.lock metrics_mutex;
      r := !r + value;
      metric.last_updated <- Unix.time ();
      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

let get_counter metric =
  match metric.metric_type with
  | Counter r -> !r
  | _ -> 0

(** Gauge - arbitrary value that can go up or down *)
let gauge name ?(labels=[]) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  let metric = 
    match Hashtbl.find_opt metrics key with
    | Some m -> m
    | None ->
        let m = {
          name;
          metric_type = Gauge (ref 0.0);
          labels;
          created_at = Unix.time ();
          last_updated = Unix.time ();
        } in
        Hashtbl.add metrics key m;
        m
  in
  Mutex.unlock metrics_mutex;
  metric

let set_gauge metric value =
  match metric.metric_type with
  | Gauge r ->
      Mutex.lock metrics_mutex;
      r := value;
      metric.last_updated <- Unix.time ();
      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

let inc_gauge metric value =
  match metric.metric_type with
  | Gauge r ->
      Mutex.lock metrics_mutex;
      r := !r +. value;
      metric.last_updated <- Unix.time ();
      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

let get_gauge metric =
  match metric.metric_type with
  | Gauge r -> !r
  | _ -> 0.0

(** Histogram - track distribution of values *)
let histogram name ?(labels=[]) () =
  let key = metric_key name labels in
  Mutex.lock metrics_mutex;
  let metric = 
    match Hashtbl.find_opt metrics key with
    | Some m -> m
    | None ->
        let m = {
          name;
          metric_type = Histogram (ref []);
          labels;
          created_at = Unix.time ();
          last_updated = Unix.time ();
        } in
        Hashtbl.add metrics key m;
        m
  in
  Mutex.unlock metrics_mutex;
  metric

let observe_histogram metric value =
  match metric.metric_type with
  | Histogram r ->
      Mutex.lock metrics_mutex;
      r := value :: !r;
      (* Keep only last 1000 samples to avoid memory bloat *)
      if List.length !r > 1000 then
        r := List.filteri (fun i _ -> i < 1000) !r;
      metric.last_updated <- Unix.time ();
      Mutex.unlock metrics_mutex;
      Lwt_condition.broadcast metrics_changed_condition ()
  | _ -> ()

let histogram_stats metric =
  match metric.metric_type with
  | Histogram r ->
      let values = !r in
      if values = [] then (0.0, 0.0, 0.0, 0.0, 0)
      else
        let sorted = List.sort Float.compare values in
        let len = List.length sorted in
        let sum = List.fold_left (+.) 0.0 sorted in
        let mean = sum /. float_of_int len in
        (* For median: average of two middle elements for even length *)
        let p50 = if len mod 2 = 0 then
          (List.nth sorted (len / 2 - 1) +. List.nth sorted (len / 2)) /. 2.0
        else
          List.nth sorted (len / 2) in
        let p95_idx = min (len - 1) (len * 95 / 100) in
        let p95 = List.nth sorted p95_idx in
        let p99_idx = min (len - 1) (len * 99 / 100) in
        let p99 = List.nth sorted p99_idx in
        (mean, p50, p95, p99, len)
  | _ -> (0.0, 0.0, 0.0, 0.0, 0)

(** Timing utilities *)
let start_timer () = Unix.gettimeofday ()

let record_duration metric start_time =
  let duration = Unix.gettimeofday () -. start_time in
  observe_histogram metric duration;
  duration

(** Global start time *)
let start_time = ref (Unix.time ())

let () = start_time := Unix.time ()

(** Initialize basic system metrics *)
let () =
  (* Create some initial metrics to ensure the dashboard has data *)
  ignore (counter "system_startups" ());
  ignore (gauge "uptime_seconds" ());
  ignore (counter "dashboard_views" ())

(** Group metrics by category for better organization *)
let categorize_metric metric =
  let name = metric.name in
  if String.starts_with ~prefix:"connection_" name ||
     String.starts_with ~prefix:"circuit_breaker" name then "Connections"
  else if String.starts_with ~prefix:"order" name ||
          String.starts_with ~prefix:"api_" name ||
          name = "orders_placed" || name = "orders_failed" || name = "orders_amended" then "Trading"
  else if String.starts_with ~prefix:"domain_" name ||
          String.starts_with ~prefix:"strategy_" name then "Domains"
  else if String.starts_with ~prefix:"ticker" name ||
          String.starts_with ~prefix:"orderbook" name ||
          String.starts_with ~prefix:"balance" name ||
          String.starts_with ~prefix:"execution" name then "Market Data"
  else if String.starts_with ~prefix:"open_" name then "Positions"
  else "Other"

(** Format metric value for display *)
let format_metric_value metric =
  match metric.metric_type with
  | Counter r -> Printf.sprintf "%d" !r
  | Gauge r -> Printf.sprintf "%.1f" !r
  | Histogram _ ->
      let (_, p50, _, _, count) = histogram_stats metric in
      if count > 0 then
        Printf.sprintf "%d samples, p50=%.1fµs" count (p50 *. 1_000_000.0)
      else "no samples"

(** Report all metrics organized by category *)
let report_metrics () =
  Mutex.lock metrics_mutex;
  let now = Unix.time () in
  let uptime = now -. !start_time in

  (* Collect all metrics by category *)
  let categories = Hashtbl.create 16 in
  Hashtbl.iter (fun _key metric ->
    let category = categorize_metric metric in
    let existing = Hashtbl.find_opt categories category |> Option.value ~default:[] in
    Hashtbl.replace categories category (metric :: existing)
  ) metrics;

  Logging.info ~section "=== Telemetry Report ===";
  Logging.info_f ~section "System uptime: %.0f seconds" uptime;
  Logging.info ~section "";

  (* Define category order *)
  let category_order = ["Connections"; "Trading"; "Positions"; "Domains"; "Market Data"; "Other"] in

  List.iter (fun category ->
    match Hashtbl.find_opt categories category with
    | Some metrics_list when metrics_list <> [] ->
        Logging.info_f ~section "[%s]" category;
        let separator = String.make (String.length category + 3) '-' in
        Logging.info_f ~section "%s" separator;

        (* Sort metrics within category by name *)
        let sorted_metrics = List.sort (fun m1 m2 -> String.compare m1.name m2.name) metrics_list in

        List.iter (fun metric ->
          let label_str =
            if metric.labels = [] then ""
            else " (" ^ (metric.labels |> List.map (fun (k, v) -> k ^ "=" ^ v) |> String.concat ", ") ^ ")"
          in

          let value_str = format_metric_value metric in
          Logging.info_f ~section "  %-25s %s%s" (metric.name ^ ":") value_str label_str;

          (* Special handling for domain_cycles rate *)
          match metric.metric_type, metric.name with
          | Counter r, "domain_cycles" when uptime > 0.0 ->
              let rate = float_of_int !r /. uptime in
              Logging.info_f ~section "    └─ %.0f cycles/sec" rate
          | _ -> ()
        ) sorted_metrics;
        Logging.info ~section ""
    | _ -> ()
  ) category_order;

  Logging.info ~section "=== End Report ===";
  Mutex.unlock metrics_mutex

(** Reset all metrics *)
let reset_metrics () =
  Mutex.lock metrics_mutex;
  Hashtbl.clear metrics;
  Mutex.unlock metrics_mutex;
  Logging.info ~section "All metrics reset"

(** Common metrics *)
module Common = struct
  let api_requests = counter "api_requests" ()
  let api_errors = counter "api_errors" ()
  let api_duration = histogram "api_duration_seconds" ()
  
  let orders_placed = counter "orders_placed" ()
  let orders_failed = counter "orders_failed" ()

  
  let domain_cycles = counter "domain_cycles" ()
  let domain_errors = counter "domain_errors" ()
end

(** Per-asset metrics helper *)
let asset_counter name asset = counter name ~labels:["asset", asset] ()
let asset_gauge name asset = gauge name ~labels:["asset", asset] ()
let asset_histogram name asset = histogram name ~labels:["asset", asset] ()

(** Update system metrics *)
let update_system_metrics () =
  let uptime_metric = gauge "uptime_seconds" () in
  let uptime = Unix.time () -. !start_time in
  set_gauge uptime_metric uptime

(** Wait for metric changes (event-driven) *)
let wait_for_metric_changes () = Lwt_condition.wait metrics_changed_condition

