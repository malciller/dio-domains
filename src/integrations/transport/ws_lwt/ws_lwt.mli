(** Client-side WebSocket connections for the engine.

    The transport is the in-tree Conduit/Cohttp/Lwt stack, and frame parsing and
    serialisation come from the in-tree [Websocket] library. This module owns connection
    establishment, the RFC 6455 opening handshake, and the lifetime of the channels behind
    a connection. [TCP_NODELAY] is enabled on TCP transports.

    Only client connections are implemented; the engine never acts as a WebSocket server. *)

(** An established client connection. *)
type conn

(** Resolves the default Conduit context.

    [Conduit_lwt_unix.default_ctx] changed type across Conduit releases: a [ctx Lazy.t] on
    Conduit >= 3 (classic-flambda builds) and a plain [ctx] on Conduit < 3 (the OxCaml
    bundle). This returns the context either way, keeping call sites
    toolchain-independent. *)
val resolve_ctx : unit -> Conduit_lwt_unix.ctx

val connect
  :  ?extra_headers:Cohttp.Header.t
  -> ?random_string:(int -> string)
  -> ?ctx:Conduit_lwt_unix.ctx
  -> ?buf:Buffer.t
  -> Conduit_lwt_unix.client
  -> Uri.t
  -> conn Lwt.t

val read : conn -> Websocket.Frame.t Lwt.t
val write : conn -> Websocket.Frame.t -> unit Lwt.t

(** Closes the underlying transport. Connection state (close frames, etc.) remains the
    caller's responsibility. *)
val close_transport : conn -> unit Lwt.t
