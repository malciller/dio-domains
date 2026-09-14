# Contributing

Send a PR if you want to fix something. A few notes so it goes smoothly.

## Ground rules

- dio trades real money. If your change touches sizing, execution, or hedging,
  explain how in an issue first.
- No new dependencies unless there's a reason. New opam packages have to build
  clean under both toolchains (the `build-test` CI job and the OxCaml image
  build).
- No compiler alerts. The OxCaml test run fails on any `Alert` output.
- Never commit `.env` or credentials. CI checks for this and fails the build.

## Local build

The CI workflow (`.github/workflows/ci.yml`) shows the full process. Locally
that means:

- OCaml 5.2.0 with dune
- `dune build @all` builds `dio`, `dio-dashboard`, `dio-oracle`
- `dune runtest` runs the tests

The docs pages cover how the pieces fit:
<https://diophantsolutions.com/dio/DEPLOYMENT/> and
<https://diophantsolutions.com/dio/CONFIGURATION/>.

## Submitting changes

1. Open an issue with the problem or feature and why.
2. Branch from `main`. Keep the change small. Add tests that fail without it.
3. Open a PR that references the issue. The `build-test` CI job has to pass.
4. The maintainer cuts releases. A merged PR shows up in the next `v*` tag.

Questions go through the issue tracker:
<https://github.com/malciller/dio-domains/issues>. Never paste API keys,
wallet seeds, or other secrets into an issue.