(** Terminal dashboard for the Dio trading engine.

    Out-of-process binary (crash isolation). Connects to the engine over a Unix domain
    socket using a length-prefixed JSON protocol in watch mode: the engine pushes snapshot
    frames on each tick.

    Usage: ./dio-dashboard [--socket /tmp/dio-<pid>.sock] [--config config.json]
    [--theme <id>] *)

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
      , " Select UI theme (e.g. tokyo-night, cyberpunk, gruvbox, ember, amber-crt, \
         paper, classic-term, or 'list' for all)" )
    ]
  in
  Arg.parse
    speclist
    (fun _ -> ())
    "dio-dashboard [--socket /tmp/dio-<pid>.sock] [--config config.json] [--theme <id>]";
  Dashboard_ui.App.run ~config_file:!config_file ~theme_override:!theme_override ()
;;
