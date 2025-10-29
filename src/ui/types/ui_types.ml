(** Common UI types and responsive utilities shared across UI modules *)

open Telemetry

(** Screen size classification based on terminal dimensions *)
type screen_size =
  | Mobile    (* < 60 cols or < 20 rows - ultra-compact content *)
  | Small     (* 60-80 cols or 20-30 rows - reduced content *)
  | Medium    (* 80-120 cols or 30-40 rows - moderate content *)
  | Large     (* 120+ cols or 40+ rows - full content *)

(** Classify screen size based on width and height *)
let classify_screen_size width height =
  if width < 50 || height < 15 then Mobile  (* Stricter mobile detection for 45x15 *)
  else if width < 80 || height < 30 then Small
  else if width < 120 || height < 40 then Medium
  else Large

(** Truncate string to maximum length with ellipsis *)
let truncate_string max_len str =
  if String.length str <= max_len then str
  else String.sub str 0 (max_len - 3) ^ "..."

(** Get adaptive progress bar width based on screen size *)
let adaptive_progress_bar_width = function
  | Mobile -> 8   (* Very narrow bars for phones *)
  | Small -> 10   (* Short bars for small screens *)
  | Medium -> 15  (* Medium bars for tablets/laptops *)
  | Large -> 20   (* Full width bars for large monitors *)

(** Get adaptive decimal places for numeric display based on screen size *)
let adaptive_decimal_places = function
  | Mobile -> 2   (* Minimal precision for small screens *)
  | Small -> 4    (* Moderate precision *)
  | Medium -> 6   (* Good precision *)
  | Large -> 8    (* Full precision *)

(** Get adaptive column width ratios for balance display *)
let adaptive_balance_column_widths screen_size =
  match screen_size with
  | Mobile -> (6, 8, 8, 0, 0)  (* Asset, Total, Current only *)
  | Small -> (8, 10, 10, 0, 0)  (* Asset, Total, Current only *)
  | Medium -> (8, 12, 12, 12, 12)  (* All columns but narrower *)
  | Large -> (8, 15, 12, 15, 12)  (* Full width columns *)

(** Get adaptive order column width ratios *)
let adaptive_order_column_widths screen_size =
  match screen_size with
  | Mobile -> (0, 0, 0, 0, 0, 0, 0)  (* No order details, just summary *)
  | Small -> (4, 6, 8, 0, 0, 6, 8)   (* Simplified columns *)
  | Medium -> (6, 8, 10, 8, 10, 6, 11) (* Most columns *)
  | Large -> (6, 10, 10, 6, 10, 6, 11) (* Full columns *)

(** Calculate maximum visible entries based on available height and screen size *)
let max_visible_entries screen_size available_height header_footer_height =
  let base_entries = match screen_size with
    | Mobile -> 5   (* Very limited for phones *)
    | Small -> 8    (* Limited for small screens *)
    | Medium -> 15  (* Moderate for tablets *)
    | Large -> 25   (* Many for large screens *)
  in
  max 1 (available_height - header_footer_height - base_entries)

(** Get adaptive metric limit per category based on screen size *)
let max_metrics_per_category = function
  | Mobile -> 5   (* Very few metrics for phones *)
  | Small -> 10   (* Limited metrics *)
  | Medium -> 20  (* Moderate metrics *)
  | Large -> 50   (* Full metrics *)

(** Get adaptive category limit based on screen size and available height *)
let max_visible_categories screen_size available_height header_footer_height =
  let base_categories = match screen_size with
    | Mobile -> 2   (* Very few categories *)
    | Small -> 3    (* Limited categories *)
    | Medium -> 5   (* Moderate categories *)
    | Large -> 8    (* Many categories *)
  in
  max 1 (available_height - header_footer_height - base_categories)

(** Get adaptive log view height based on screen size and available space *)
let adaptive_log_view_height screen_size available_height header_footer_height =
  let base_height = match screen_size with
    | Mobile -> 5   (* Very small log area *)
    | Small -> 8    (* Small log area *)
    | Medium -> 15  (* Moderate log area *)
    | Large -> 30   (* Large log area *)
  in
  max 3 (available_height - header_footer_height - base_height)

(** Check if temperature section should be shown *)
let should_show_temperature screen_size =
  match screen_size with
  | Mobile -> false  (* No temperature on phones *)
  | Small -> false   (* No temperature on small screens *)
  | Medium -> true   (* Show temperature on tablets *)
  | Large -> true    (* Show temperature on large screens *)

(** Get adaptive timestamp format based on screen size *)
let adaptive_timestamp_format screen_size =
  match screen_size with
  | Mobile -> "%M:%S"      (* Minutes:Seconds only *)
  | Small -> "%H:%M:%S"    (* Hours:Minutes:Seconds *)
  | Medium -> "%H:%M:%S"   (* Full timestamp *)
  | Large -> "%Y-%m-%d %H:%M:%S"  (* Full date and time *)

(** Get adaptive CPU model display width *)
let adaptive_cpu_model_width screen_size =
  match screen_size with
  | Mobile -> 15   (* Very short model name *)
  | Small -> 20    (* Short model name *)
  | Medium -> 30   (* Medium model name *)
  | Large -> 50    (* Full model name *)

(** Check if per-core CPU display should be shown *)
let should_show_cpu_cores screen_size =
  match screen_size with
  | Mobile -> false  (* No per-core display on phones *)
  | Small -> false   (* No per-core display on small screens *)
  | Medium -> true   (* Show cores on tablets *)
  | Large -> true    (* Show cores on large screens *)

(** Minimum dimensions per module *)
let min_module_width = 40  (* chars *)
let min_module_height = 15 (* lines *)

(** Reserve space for UI chrome *)
let chrome_height = 4  (* status bar + bottom bar + separators *)
let hamburger_height = 1  (* if any modules collapsed *)

(** Calculate available space for modules after chrome *)
let calculate_available_space terminal_width terminal_height collapsed_count =
  let chrome_h = chrome_height + (if collapsed_count > 0 then hamburger_height else 0) in
  let available_w = terminal_width in
  let available_h = max 1 (terminal_height - chrome_h) in
  (available_w, available_h)

(** Determine if viewport can support multiple modules simultaneously *)
let can_support_multiple_modules available_width available_height =
  available_width >= (min_module_width * 2) && available_height >= (min_module_height * 2)

(** Get maximum number of modules that can fit in available space *)
let max_modules_in_viewport available_width available_height =
  let modules_wide = available_width / min_module_width in
  let modules_tall = available_height / min_module_height in
  modules_wide * modules_tall

(** Mobile-specific sizing constants (45x15 viewport) *)
let mobile_min_width = 45  (* Minimum width for mobile view *)
let mobile_min_height = 15 (* Minimum height for mobile view *)
let mobile_max_lines_per_view = 6  (* Maximum lines per view on mobile *)
let mobile_max_entries_per_view = 5  (* Maximum entries/items per view on mobile *)

(** Truncate string to fit mobile display (aggressive truncation) *)
let truncate_mobile str =
  if String.length str <= 15 then str
  else String.sub str 0 12 ^ "..."

(** Truncate string to fit small mobile display (ultra-aggressive) *)
let truncate_tiny str =
  if String.length str <= 8 then str
  else String.sub str 0 5 ^ "..."

(** Truncate string to exact width (no ellipsis) *)
let truncate_exact width str =
  if String.length str <= width then str
  else String.sub str 0 width

(** Format percentage with mobile-appropriate precision *)
let format_mobile_percentage value =
  Printf.sprintf "%.0f%%" value

(** Format currency value for mobile (compact) *)
let format_mobile_currency value =
  if abs_float value >= 1000.0 then
    Printf.sprintf "$%.0f" value
  else if abs_float value >= 100.0 then
    Printf.sprintf "$%.1f" value
  else
    Printf.sprintf "$%.2f" value

(** Format number for mobile (compact) *)
let format_mobile_number value =
  if abs_float value >= 1000.0 then
    Printf.sprintf "%.0f" value
  else if abs_float value >= 100.0 then
    Printf.sprintf "%.1f" value
  else
    Printf.sprintf "%.2f" value

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
  cpu_temp: float option;     (* CPU temperature in Celsius *)
  gpu_temp: float option;     (* GPU temperature in Celsius *)
  temps: (string * float) list;  (* Additional temperature sensors (name, temp in C) *)
}
