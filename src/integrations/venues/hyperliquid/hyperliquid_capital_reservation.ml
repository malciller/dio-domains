(** Hyperliquid implementation of the optional [Exchange_intf.Capital_reservation]
    capability. *)

module Exchange = Dio_exchange.Exchange_intf

module Impl : Exchange.Capital_reservation.S = struct
  let name = "hyperliquid"

  let set ~owner ~amount =
    Dio_exchange.Capital_reservation.set ~exchange:name ~owner ~amount
  ;;

  let release ~owner = Dio_exchange.Capital_reservation.release ~exchange:name ~owner
  let get_total () = Dio_exchange.Capital_reservation.get_total ~exchange:name
  let available ~quote = Dio_exchange.Capital_reservation.available ~exchange:name ~quote
end
