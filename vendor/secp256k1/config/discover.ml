module C = Configurator.V1

let () =
  C.main ~name:"secp256k1" (fun c ->
    let default : C.Pkg_config.package_conf = { libs = [ "-lsecp256k1" ]; cflags = [] } in
    let conf =
      match C.Pkg_config.get c with
      | None -> default
      | Some pc ->
        (match C.Pkg_config.query pc ~package:"libsecp256k1" with
         | Some conf -> conf
         | None -> default)
    in
    C.Flags.write_sexp "c_flags.sexp" conf.cflags;
    C.Flags.write_sexp "c_library_flags.sexp" conf.libs)
;;
