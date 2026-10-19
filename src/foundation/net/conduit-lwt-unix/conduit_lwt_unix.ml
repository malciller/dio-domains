(** In-tree replacement for [conduit-lwt-unix].

    Only the client-side surface used by the engine is implemented: the polymorphic [`TLS]
    and [`TCP] clients, the default OpenSSL context, and the
    [(flow * input_channel * output_channel)] result. TLS itself remains the vendored
    [ssl]/[lwt_ssl] bindings to OpenSSL. *)

open Lwt.Infix

let () = Ssl.init ()

type tcp_flow =
  { fd : Lwt_unix.file_descr
  ; ip : Ipaddr.t
  ; port : int
  }

type domain_flow =
  { fd : Lwt_unix.file_descr
  ; path : string
  }

type flow =
  | TCP of tcp_flow
  | Domain_socket of domain_flow

type client_tls_config = [ `Hostname of string ] * [ `IP of Ipaddr.t ] * [ `Port of int ]

type client =
  [ `TLS of client_tls_config
  | `TLS_native of client_tls_config
  | `OpenSSL of client_tls_config
  | `TCP of [ `IP of Ipaddr.t ] * [ `Port of int ]
  | `Unix_domain_socket of [ `File of string ]
  ]

type ctx = Ssl.context

let create_ctx () =
  let ctx = Ssl.create_context Ssl.TLSv1_2 Ssl.Client_context in
  Ssl.set_max_protocol_version ctx Ssl.TLSv1_3;
  ignore (Ssl.set_default_verify_paths ctx);
  Ssl.set_verify ctx [ Ssl.Verify_peer ] None;
  ctx
;;

let default_ctx = create_ctx ()

let chans_of_ssl sock =
  let is_open = ref true in
  let shutdown () = if !is_open then Lwt_ssl.ssl_shutdown sock else Lwt.return_unit in
  let close () =
    is_open := false;
    Lwt_ssl.close sock
  in
  let oc = Lwt_io.make ~mode:Lwt_io.output ~close:shutdown (Lwt_ssl.write_bytes sock) in
  let ic = Lwt_io.make ~mode:Lwt_io.input ~close (Lwt_ssl.read_bytes sock) in
  Lwt_ssl.get_fd sock, ic, oc
;;

let tcp_connect ip port =
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let addr = Unix.ADDR_INET (Unix.inet_addr_of_string (Ipaddr.to_string ip), port) in
  Lwt_unix.connect fd addr >|= fun () -> fd
;;

let tls_connect ctx fd hostname =
  let uninit = Lwt_ssl.embed_uninitialized_socket fd ctx in
  let ssl = Lwt_ssl.ssl_socket_of_uninitialized_socket uninit in
  Ssl.set_client_SNI_hostname ssl hostname;
  Ssl.set_hostflags ssl [ Ssl.No_partial_wildcards ];
  Ssl.set_host ssl hostname;
  Lwt_ssl.ssl_perform_handshake uninit
;;

let connect ~ctx client =
  match client with
  | `TCP (`IP ip, `Port port) ->
    tcp_connect ip port
    >|= fun fd ->
    let ic = Lwt_io.of_fd ~mode:Lwt_io.Input fd in
    let oc = Lwt_io.of_fd ~mode:Lwt_io.Output fd in
    TCP { fd; ip; port }, ic, oc
  | `Unix_domain_socket (`File path) ->
    let fd = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
    Lwt_unix.connect fd (Unix.ADDR_UNIX path)
    >|= fun () ->
    let ic = Lwt_io.of_fd ~mode:Lwt_io.Input fd in
    let oc = Lwt_io.of_fd ~mode:Lwt_io.Output fd in
    Domain_socket { fd; path }, ic, oc
  | `TLS (`Hostname host, `IP ip, `Port port)
  | `TLS_native (`Hostname host, `IP ip, `Port port)
  | `OpenSSL (`Hostname host, `IP ip, `Port port) ->
    tcp_connect ip port
    >>= fun fd ->
    tls_connect ctx fd host
    >|= fun sock ->
    let _, ic, oc = chans_of_ssl sock in
    TCP { fd; ip; port }, ic, oc
;;
