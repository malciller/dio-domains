//! Zed extension for Dio strategy scripts.
//!
//! The language itself (grammar, highlighting, brackets) lives in
//! `extension.toml` + `languages/strategy/`. This file only supplies the
//! command Zed should spawn for `dio strategy lsp`.

use zed_extension_api as zed;

struct DioStrategyExtension;

impl zed::Extension for DioStrategyExtension {
    fn new() -> Self {
        Self
    }

    fn language_server_command(
        &mut self,
        _language_server_id: &zed::LanguageServerId,
        worktree: &zed::Worktree,
    ) -> zed::Result<zed::Command> {
        // Prefer `dio` on PATH, then the in-repo build so this checkout needs no
        // PATH setup. A missing binary surfaces in Zed's LSP error notification.
        let command = worktree.which("dio").unwrap_or_else(|| {
            format!(
                "{}/_build/default/bin/main.exe",
                worktree.root_path().trim_end_matches('/')
            )
        });

        Ok(zed::Command {
            command,
            args: vec!["strategy".to_string(), "lsp".to_string()],
            env: Default::default(),
        })
    }
}

zed::register_extension!(DioStrategyExtension);
