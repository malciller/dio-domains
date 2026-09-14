# Contributing

Thanks for considering a contribution to dio. The project is small and
operationally focused — a few pointers so your PR lands cleanly.

## Ground rules

- dio **trades real money**. Behavioral changes to the engine are assessed
  against the [specification](https://diophantsolutions.com/dio/SPEC/) and the
  risk policy. If your change alters sizing, execution, or hedging, say exactly
  how and discuss it in an issue first.
- No surprise dependencies. New opam packages or changes to the production
  dependency closure need a clear justification and must build cleanly under
  both the classic-flambda toolchain (CI's `build-test`) and OxCaml/flambda2
  (the image build).
- Zero compiler alerts. The OxCaml test run treats `Alert` output as a failure.
- Never commit `.env` or any credentials — CI enforces this and will fail the
  build.

## Development loop

The reference build process lives in the CI workflow
(`.github/workflows/ci.yml`); locally that means:

- `opam switch` on OCaml 5.2.0 with `dune`
- `dune build @all` to compile `dio`, `dio-dashboard`, `dio-oracle`
- `dune runtest` for the test suite

See the [deployment](https://diophantsolutions.com/dio/DEPLOYMENT/) and
[configuration](https://diophantsolutions.com/dio/CONFIGURATION/) pages for how
the pieces fit together.

## Submitting changes

1. Open an issue describing the problem or the feature and the motivation.
2. Branch from `main`, keep the change focused, and add tests that fail without
   the change.
3. Open a PR referencing the issue. The `build-test` CI job must pass; keep the
   main build green.
4. Releases are cut by the maintainer. A merged PR will be included in the next
   `v*` tag automatically.

Questions and bug reports go through the issue tracker:
<https://github.com/malciller/dio-domains/issues>. Never paste API keys, wallet
seeds, or other secrets into an issue.