# Contributing

Send a PR if you want to fix something. A few notes so it goes smoothly.

## Ground rules

- dio trades real money. If your change touches sizing, execution, or hedging,
  explain how in an issue first.
- No new dependencies unless there's a reason. New opam packages have to build
  clean under OxCaml (the `build-test-oxcaml` CI job). The engine is OxCaml-only;
  there is no stock-OCaml build.
- No compiler alerts. The OxCaml test run fails on any `Alert` output.
- Never commit `.env` or credentials. CI checks for this and fails the build.

## Local build

The CI workflow (`.github/workflows/ci.yml`) shows the full process. Locally
that means:

- OxCaml (flambda2). The CI images in `Dockerfile.base`/`Dockerfile` are the
  reference toolchain; build that way locally to match CI
- `dune build @all` builds `dio`, `dio-dashboard`, `dio-oracle`
- `dune runtest` runs the tests

The docs pages cover how the pieces fit:
<https://diophantsolutions.com/dio/DEPLOYMENT/> and
<https://diophantsolutions.com/dio/CONFIGURATION/>.

## Submitting changes

1. Open an issue with the problem or feature and why.
2. Branch from `main`. Keep the change small. Add tests that fail without it.
3. Open a PR that references the issue. The `build-test-oxcaml` CI job has to
   pass.
4. The maintainer cuts releases. A merged PR shows up in the next `v*` tag.

Questions go through the issue tracker:
<https://github.com/malciller/dio-domains/issues>. Never paste API keys,
wallet seeds, or other secrets into an issue.