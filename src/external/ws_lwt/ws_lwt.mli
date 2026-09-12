(** Internal, client-only WebSocket client over Conduit + Cohttp-Lwt-Unix.

    Drop-in replacement for the subset of the lwt websocket wrapper that the
    engine uses, without the deprecated [lwt_log] dependency. Framing is
    provided by [Websocket.Make (Cohttp_lwt_unix.IO)], so bytes on the wire
    are identical to the reference implementation. [TCP_NODELAY] is set on
    the underlying socket when connecting. *)

(** An established client connection. *)
type conn

(** Resolves the default Conduit context.

    [Conduit_lwt_unix.default_ctx] changed type across Conduit releases: it is a
    [ctx Lazy.t] on Conduit >= 3 (classic-flambda builds) and a plain [ctx] on
    Conduit < 3 (the OxCaml bundle). This returns the context either way, so
    call sites do not depend on which toolchain they are compiled under. *)
val resolve_ctx : unit -> Conduit_lwt_unix.ctx

val connect :
  ?extra_headers:Cohttp.Header.t ->
  ?random_string:(int -> string) ->
  ?ctx:Conduit_lwt_unix.ctx ->
  ?buf:Buffer.t ->
  Conduit_lwt_unix.client ->
  Uri.t ->
  conn Lwt.t

val read : conn -> Websocket.Frame.t Lwt.t
val write : conn -> Websocket.Frame.t -> unit Lwt.t

val close_transport : conn -> unit Lwt.t
(** Closes the underlying transport. Connection state (close frames, etc.)
    remains the caller's responsibility. *)
