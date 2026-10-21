(** Hyperliquid implementation of the optional [Exchange_intf.Market_context] capability.

    The feed writers publish into {!Dio_exchange.Market_context}; this module is the
    engine-facing, venue-keyed capability so strategy code never reaches into the global
    store directly. *)

module Exchange = Dio_exchange.Exchange_intf

module Impl : Exchange.Market_context.S = struct
  let name = "hyperliquid"
  let get_funding ~coin = Dio_exchange.Market_context.get_funding ~exchange:name ~coin

  let get_positions_updated_at () =
    Dio_exchange.Market_context.get_positions_updated_at ~exchange:name
  ;;

  let get_position ~coin = Dio_exchange.Market_context.get_position ~exchange:name ~coin
  let get_margin () = Dio_exchange.Market_context.get_margin ~exchange:name
  let get_margin_ratio () = Dio_exchange.Market_context.get_margin_ratio ~exchange:name
end
