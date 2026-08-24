(** Terminal dashboard for the Dio trading engine.

    Runs as an out-of-process binary for crash isolation. Connects to the
    engine over a Unix domain socket using a length-prefixed JSON protocol
    in watch mode, where the engine pushes snapshot frames on each tick.


    Usage: ./dio-dashboard [--socket /tmp/dio-<pid>.sock] [--config config.json] [--theme <id>]
*)

let () =
  let theme_override = ref "" in
  let config_file = ref "config.json" in
  let speclist =
    [ ( "--socket"
      , Arg.Set_string Dashboard_ui.App.socket_path
      , " Path to engine UDS (auto-discovers if not set)" )
    ; ( "--config"
      , Arg.Set_string config_file
      , " Path to engine config.json (default: config.json)" )
    ; ( "--theme"
      , Arg.String
          (fun s ->
             let s_clean = String.trim (String.lowercase_ascii s) in
             if s_clean = "list" || s_clean = "help"
             then (
               Printf.printf "Available dashboard themes:\n";
               List.iter
                 (fun (t : Dashboard_ui.Theme.theme_palette) ->
                    Printf.printf "  %-16s %s\n" t.id t.name)
                 (Dashboard_ui.Theme.all_themes ());
               exit 0)
             else theme_override := s_clean)
      , " Select UI theme (e.g. tokyo-night, cyberpunk, nord, catppuccin, gruvbox, matrix, monokai, solarized, emerald, dracula, rose-pine, kanagawa, synthwave84, abyss, or 'list')" )
    ]
  in
  Arg.parse speclist (fun _ -> ()) "dio-dashboard [--socket /tmp/dio-<pid>.sock] [--config config.json] [--theme <id>]";
  (* First load theme from config.json (or ~/.dio_theme) *)
  Dashboard_ui.Theme.load_saved_theme ~config_file:!config_file ();
  (* If explicit CLI --theme is passed, override and save *)
  if !theme_override <> ""
  then (
    if not (Dashboard_ui.Theme.set_theme_by_id !theme_override)
    then (
      Printf.eprintf "Unknown theme '%s'. Run with '--theme list' to see available themes.\n%!" !theme_override;
      exit 1)
    else
      Dashboard_ui.Theme.save_theme !theme_override);
  Dashboard_ui.App.run ~config_file:!config_file ()
;;
