(** Root namespace for the Kraken Unified FIX 4.4 adapter, and the place where it plugs
    into the venue.

    Everything FIX-specific lives in this library: the wire codec, logon authentication,
    the session state machine, execution-report translation, and order entry. The spot
    WebSocket adapter ([dio.kraken]) knows nothing about it; it exposes one seam
    ({!Kraken_order_transport}) that a transport registers into, and this module is what
    registers the FIX implementation. Linking [dio.kraken_fix] plus
    [KRAKEN_ACCOUNT_TIER=enterprise] is therefore the whole switch.

    What stays on the existing transport even for an enterprise account: market data
    (order book, instruments), balances, and the REST open-orders bootstrap. Those are
    account channels, not order-entry ones, and FIX carries no equivalent; the FIX session
    feeds the same order cache the WebSocket executions channel would have fed, so there
    is exactly one source of truth for order state. *)

(** FIX 4.4 framing, field access, message construction and timestamps. *)
module Kraken_fix_codec = Kraken_fix_codec

(** Logon password derivation (HMAC-SHA512 over SHA-256, base64 secret). *)
module Kraken_fix_auth = Kraken_fix_auth

(** Session configuration, read from the environment. *)
module Kraken_fix_config = Kraken_fix_config

(** ClOrdID allocation and the local order-userref table. *)
module Kraken_fix_order_ids = Kraken_fix_order_ids

(** Execution reports translated into the engine's order cache. *)
module Kraken_fix_execution_reports = Kraken_fix_execution_reports

(** Session lifecycle, sequence numbers, heartbeats, resends and request correlation. *)
module Kraken_fix_client = Kraken_fix_client

(** NewOrderSingle, OrderCancelReplaceRequest and OrderCancelRequest. *)
module Kraken_fix_actions = Kraken_fix_actions

(** The FIX transport as the spot adapter's seam describes it.

    The WebSocket connection is still opened, because balances and the health probe live
    on it, and the supervisor keeps supervising the pair as one logical "Kraken
    authenticated connection": if either half dies the pair is re-established. Order entry
    and execution reports are answered by the FIX session. *)
let transport : Kraken_order_transport.t =
  { name = "kraken-fix"
  ; execution_source = Kraken_order_transport.Execution_from_fix
  ; place_order = Kraken_fix_actions.place_order
  ; amend_order = Kraken_fix_actions.amend_order
  ; cancel_orders = Kraken_fix_actions.cancel_orders
  ; is_connected = Kraken_fix_client.is_connected
  ; send_ping = Kraken_fix_client.send_ping
  ; connect_and_monitor =
      (fun ~token ~on_failure ~on_connected ->
        let websocket_session =
          match Kraken_order_transport.websocket () with
          | Some ws ->
            (* The WebSocket half never signals readiness: orders cannot go out until the
               FIX session has logged on. *)
            ws.Kraken_order_transport.connect_and_monitor
              ~token
              ~on_failure
              ~on_connected:(fun () -> ())
          | None -> Lwt.return_unit
        in
        let fix_session =
          Kraken_fix_client.connect_and_monitor ~token ~on_failure ~on_connected
        in
        (* Each half reports its own failure and the supervisor restarts the pair, so
           waiting for both keeps this promise alive while either is usable. *)
        Lwt.join [ websocket_session; fix_session ])
  }
;;

let () = Kraken_order_transport.register_fix transport
