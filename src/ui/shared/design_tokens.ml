(** Design Tokens Module

    Centralized design system for the Bloomberg-style terminal UI.
    Defines colors, typography, spacing, and visual constants.
*)

open Notty

(** Color Palette - ANSI colors mapped to semantic roles *)
module Colors = struct
  (** Background colors *)
  let background = A.black
  let background_dark = A.gray 0

  (** Primary text colors *)
  let primary_text = A.(fg white)
  let bright_primary = A.(st bold ++ fg white)

  (** Secondary text colors *)
  let secondary_text = A.(fg (gray 2))
  let dimmed_text = A.(fg (gray 3))

  (** Accent and focus colors *)
  let accent = A.cyan
  let focus_highlight = A.(st bold ++ st reverse ++ bg cyan ++ fg black)

  (** Status colors *)
  let success = A.(fg green)
  let success_bright = A.(st bold ++ fg green)
  let warning = A.(fg yellow)
  let warning_bright = A.(st bold ++ fg yellow)
  let error = A.(fg red)
  let error_bright = A.(st bold ++ fg red)
  let info = A.(fg blue)
  let info_bright = A.(st bold ++ fg blue)

  (** Border colors *)
  let border = A.(fg (gray 3))
  let border_active = A.(fg cyan)
  let border_inactive = A.(fg (gray 4))
end

(** Typography Hierarchy *)
module Typography = struct
  (** Heading/Titles - Bold white text *)
  let heading = Colors.bright_primary

  (** Labels - Bold white or cyan text *)
  let label = Colors.bright_primary
  let label_accent = A.(st bold ++ fg cyan)

  (** Focus/Highlight - Reverse cyan background *)
  let focus_highlight = Colors.focus_highlight

  (** Body Text - Normal white text *)
  let body = Colors.primary_text

  (** Secondary Text - Dim gray *)
  let secondary = Colors.secondary_text

  (** Dimmed/Inactive - Very dim gray *)
  let dimmed = Colors.dimmed_text

  (** Status text helpers *)
  let success_text = Colors.success
  let warning_text = Colors.warning
  let error_text = Colors.error
  let info_text = Colors.info
end

(** Spacing Constants *)
module Spacing = struct
  (** Panel padding: 1 space left/right, 0 lines top/bottom content *)
  let panel_padding_x = 1
  let panel_padding_y = 0

  (** Gutter between panels: 1 space *)
  let panel_gutter = 1

  (** Section spacing: 1 empty line *)
  let section_spacing = 1

  (** Border width: 1 char *)
  let border_width = 1

  (** Minimum margins for readability *)
  let min_margin = 1
end

(** Border Styles *)
module Borders = struct
  (** Box-drawing characters for Bloomberg aesthetic *)
  let horizontal = "─"
  let vertical = "│"
  let top_left = "┌"
  let top_right = "┐"
  let bottom_left = "└"
  let bottom_right = "┘"
  let t_left = "├"
  let t_right = "┤"
  let t_top = "┬"
  let t_bottom = "┴"

  (** ASCII fallback for compatibility *)
  let horizontal_ascii = "-"
  let vertical_ascii = "|"
  let corner_ascii = "+"

  (** Border style configuration *)
  type border_style = {
    top_left: string;
    top_right: string;
    bottom_left: string;
    bottom_right: string;
    horizontal: string;
    vertical: string;
  }

  (** Default box-drawing border style *)
  let box_drawing = {
    top_left;
    top_right;
    bottom_left;
    bottom_right;
    horizontal;
    vertical;
  }

  (** ASCII fallback border style *)
  let ascii_fallback = {
    top_left = corner_ascii;
    top_right = corner_ascii;
    bottom_left = corner_ascii;
    bottom_right = corner_ascii;
    horizontal = horizontal_ascii;
    vertical = vertical_ascii;
  }

  (** Get border characters based on terminal capabilities *)
  let get_border_style () = box_drawing  (* Default to box-drawing, can be made adaptive later *)
end
