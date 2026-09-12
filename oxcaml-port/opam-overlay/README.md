# OxCaml patch overlay

Local opam repository overlay used by the OxCaml Docker build. It carries
patched versions of third-party libraries that do not compile under OxCaml.

Layout follows the standard opam repository format:

```
oxcaml-port/opam-overlay/
  repo                      # opam-version marker
  packages/<pkg>/<pkg>.<ver>/opam
```

Each patched package is named `<pkg>.<version>+dioN` and carries an
`oxcaml.patch` that is applied during build. See `OXCAML_PORT.md` §5.

Wiring (see the OxCaml switch block in `OXCAML_PORT.md` §8): add this directory
as an extra opam repository *after* the OxCaml repo, e.g.

```sh
opam switch create 5.2.0+ox ocaml-variants.5.2.0+ox \
  --repos ox=git+https://github.com/oxcaml/opam-repository.git,\
dio-ox=file:///app/oxcaml-port/opam-overlay,default
```

Do not use this overlay for classic-flambda builds.
