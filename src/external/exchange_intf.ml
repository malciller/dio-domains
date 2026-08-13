(** Exchange interface definition.

    Defines the canonical module signature [S] that all exchange backends must
    implement, a shared [Types] module for order lifecycle and market data
    records, and a [Registry] for dynamic exchange lookup at runtime. *)

(** Shared types for order management, market data events, and retry
    configuration. Used uniformly across all exchange implementations. *)
module Types = struct
  (** Supported exchanges for type-safe dispatch. *)
  type exchange_id =
    | Hyperliquid
    | Kraken
    | Lighter
    | Ibkr
    | Alpaca
    | Custom of string

  let exchange_of_string = function
    | "hyperliquid" -> Hyperliquid
    | "kraken" -> Kraken
    | "lighter" -> Lighter
    | "ibkr" -> Ibkr
    | "alpaca" -> Alpaca
    | s -> Custom s
  ;;

  let string_of_exchange = function
    | Hyperliquid -> "hyperliquid"
    | Kraken -> "kraken"
    | Lighter -> "lighter"
    | Ibkr -> "ibkr"
    | Alpaca -> "alpaca"
    | Custom s -> s
  ;;

  (** Supported order types. [Other s] captures exchange-specific types
      not covered by the standard variants. *)
  type order_type =
    | Limit
    | Market
    | StopLoss
    | TakeProfit
    | StopLossLimit
    | TakeProfitLimit
    | SettlPosition
    | Other of string

  (** Direction of an order. *)
  type order_side =
    | Buy
    | Sell

  (** Time-in-force policy governing order lifetime.
      - [GTC]: Good-til-canceled.
      - [IOC]: Immediate-or-cancel; unfilled portion is canceled.
      - [FOK]: Fill-or-kill; entire quantity must fill or order is rejected. *)
  type time_in_force =
    | GTC
    | IOC
    | FOK

  (** Acknowledgment returned after successful order placement. *)
  type add_order_result =
    { order_id : string (** Exchange-assigned order identifier. *)
    ; cl_ord_id : string option (** Client-supplied order identifier, if provided. *)
    ; order_userref : int option (** User reference integer, if provided. *)
    }

  (** Lifecycle states of an order on the exchange. [Unknown s] captures
      any status string not mapped to a known variant. *)
  type order_status =
    | Pending
    | New
    | PartiallyFilled
    | Filled
    | Canceled
    | Expired
    | Rejected
    | Unknown of string

  (** Snapshot of a single open order, including fill progress and
      optional client identifiers. *)
  type open_order =
    { order_id : string (** Exchange-assigned order identifier. *)
    ; symbol : string (** Trading pair symbol. *)
    ; side : order_side (** Buy or sell. *)
    ; qty : float (** Original order quantity. *)
    ; cum_qty : float (** Cumulative filled quantity. *)
    ; remaining_qty : float (** Quantity remaining to be filled. *)
    ; limit_price : float option (** Limit price, if applicable. *)
    ; status : order_status (** Current order lifecycle status. *)
    ; user_ref : int option (** User reference integer, if set. *)
    ; cl_ord_id : string option (** Client order identifier, if set. *)
    }

  (** Acknowledgment returned after successful order amendment. *)
  type amend_order_result =
    { original_order_id : string (** Identifier of the amended order. *)
    ; new_order_id : string (** Identifier assigned to the replacement order. *)
    ; amend_id : string option (** Exchange-assigned amendment identifier, if any. *)
    ; cl_ord_id : string option (** Client order identifier, if provided. *)
    }

  (** Acknowledgment returned after successful order cancellation. *)
  type cancel_order_result =
    { order_id : string (** Identifier of the canceled order. *)
    ; cl_ord_id : string option (** Client order identifier, if provided. *)
    }

  (** Orderbook depth snapshot with arrays of (price, size) levels. *)
  type orderbook_event =
    { bids : (float * float) array (** Bid levels: (price, size). *)
    ; asks : (float * float) array (** Ask levels: (price, size). *)
    ; timestamp : float (** Unix timestamp of the snapshot. *)
    }

  (** Execution report describing a state change on an order. *)
  type execution_event =
    { order_id : string (** Exchange-assigned order identifier. *)
    ; order_status : order_status (** Updated order status. *)
    ; limit_price : float option (** Limit price, if applicable. *)
    ; side : order_side (** Buy or sell. *)
    ; remaining_qty : float (** Quantity remaining after this event. *)
    ; filled_qty : float (** Cumulative filled quantity. *)
    ; avg_price : float (** Volume-weighted average fill price. *)
    ; timestamp : float (** Unix timestamp of the execution report. *)
    ; is_amended : bool
      (** True when this event is an in-place amendment
                                       confirmation (Kraken exec_type=amended), not a
                                       genuine new-order acknowledgment. Domain workers
                                       must skip handle_order_acknowledged for these. *)
    ; cl_ord_id : string option
      (** Client order id when the venue provides one
                                       (Hyperliquid cloid, Lighter client_order_id). *)
    }

  (** Parameters controlling exponential backoff retry behavior.
      Canonical definition lives in [Error_handling]; re-exported here
      so exchange module signatures can reference [Types.retry_config]
      without depending on [Error_handling] directly. *)
  type retry_config = Error_handling.retry_config =
    { max_attempts : int (** Maximum number of attempts (including the initial). *)
    ; base_delay_ms : float (** Initial delay between retries, in milliseconds. *)
    ; max_delay_ms : float (** Upper bound on delay between retries, in milliseconds. *)
    ; backoff_factor : float
      (** Multiplicative factor applied to the delay after each attempt. *)
    }

  (* ---- Oracle data-layer types ----
     Shared with the capital oracle via [Oracle]; defined here so venue
     libraries can implement [Oracle.S] without depending on the oracle
     library. [Oracle_types] aliases these (see oracle_types.ml). *)

  (** Historical daily OHLC bar (the oracle's data layer). One bar per
      session/date. *)
  type bar =
    { date : string (** ISO date (YYYY-MM-DD). *)
    ; open_ : float
    ; high : float
    ; low : float
    ; close : float
    ; volume : float
    }

  (** Calendar semantics of a venue for session accounting.
      - [Crypto]: 24/7 markets; a session is a calendar day.
      - [Equity]: market sessions only (weekdays minus holidays). *)
  type calendar_kind =
    | Crypto
    | Equity
end

(** Module signature that every exchange backend must satisfy.

    Covers order lifecycle operations (place, amend, cancel), synchronous
    market data accessors backed by ring buffers, position-based event feed
    consumption, balance queries, instrument metadata, and fee retrieval. *)
module type S = sig
  (** Human-readable exchange name used as the registry key. *)
  val name : string

  (** Submit a new order to the exchange.

      Required parameters: [token] (auth), [order_type], [side], [qty],
      [symbol]. Optional parameters control limit pricing, time-in-force,
      post-only and reduce-only flags, user reference, client order id,
      trigger price, iceberg display quantity, and retry behavior.

      Returns [Ok add_order_result] on acceptance or [Error msg] on failure. *)
  val place_order
    :  token:string
    -> order_type:Types.order_type
    -> side:Types.order_side
    -> qty:float
    -> symbol:string
    -> ?limit_price:float
    -> ?time_in_force:Types.time_in_force
    -> ?post_only:bool
    -> ?reduce_only:bool
    -> ?order_userref:int
    -> ?cl_ord_id:string
    -> ?trigger_price:float
    -> ?display_qty:float
    -> ?retry_config:Types.retry_config
    -> unit
    -> (Types.add_order_result, string) result Lwt.t

  (** Amend an existing order (price, quantity, trigger, display qty).

      Requires [token] and [order_id]. All mutable order fields are optional.
      Returns [Ok amend_order_result] on acceptance or [Error msg] on failure. *)
  val amend_order
    :  token:string
    -> order_id:string
    -> ?cl_ord_id:string
    -> ?qty:float
    -> ?limit_price:float
    -> ?post_only:bool
    -> ?trigger_price:float
    -> ?display_qty:float
    -> ?symbol:string
    -> ?retry_config:Types.retry_config
    -> unit
    -> (Types.amend_order_result, string) result Lwt.t

  (** Cancel one or more orders identified by order id, client order id,
      or user reference. At least one identifier list should be non-empty.

      Returns [Ok cancel_order_result list] or [Error msg]. *)
  val cancel_orders
    :  token:string
    -> ?order_ids:string list
    -> ?cl_ord_ids:string list
    -> ?order_userrefs:int list
    -> ?symbol:string
    -> ?retry_config:Types.retry_config
    -> unit
    -> (Types.cancel_order_result list, string) result Lwt.t

  (* ---- Market data accessors ---- *)

  (** Return top-of-book as [(bid_price, bid_size, ask_price, ask_size)],
      or [None] if orderbook data is unavailable. *)
  val get_top_of_book : symbol:string -> (float * float * float * float) option

  (** Return the current tradeable balance for [asset]. Returns [0.0] if unknown. *)
  val get_tradeable_balance : asset:string -> float

  (** Return a fast path closure for fetching live tradeable balance of [asset] without lock acquisition overhead. *)
  val get_tradeable_balance_fast : asset:string -> unit -> float

  (** Return a fast path closure yielding the age (seconds) of the cached
      [asset] balance snapshot, or [None] when the exchange does not track
      freshness. A fresh snapshot is authoritative: strategies that cannot
      fund a buy against it skip placement. A stale (or unknown-age) snapshot
      may be wrong, so strategies may still attempt and let the exchange
      decide. *)
  val get_balance_age_fast : asset:string -> unit -> float option

  (** Return the total balance for [asset] including staked/earn/vault balances. Returns [0.0] if unknown. *)
  val get_total_balance : asset:string -> float

  (** Return all cached asset balances as [(asset_name, balance)] pairs. *)
  val get_all_balances : unit -> (string * float) list

  (** Look up a specific open order by [symbol] and [order_id].
      Returns [None] if not found. *)
  val get_open_order : symbol:string -> order_id:string -> Types.open_order option

  (** Return all open orders for [symbol]. *)
  val get_open_orders : symbol:string -> Types.open_order list

  (** Return all open orders across all symbol stores whose symbol
      starts with [asset ^ "/"]. Used by the dashboard to surface orders
      for non-strategy balance assets that may be stored under a different
      symbol key than the one the dashboard constructs. *)
  val get_all_orders_for_asset : asset:string -> Types.open_order list

  (* ---- Dynamic Subscription ---- *)

  (** Dynamically subscribe additional symbols to the real-time orderbook feed.
      Used to pipe top-of-book data into the central feed for non-active balance assets. *)
  val subscribe_orderbook : symbols:string list -> unit Lwt.t

  (* ---- Ring buffer event feed consumption ---- *)

  (** Return the current write position of the orderbook ring buffer for
      [symbol]. Used as the starting cursor for [read_orderbook_events]. *)
  val get_orderbook_position : symbol:string -> int

  (** Read orderbook events from [start_pos] up to the current write
      position. Returns a newly allocated list of events. *)
  val read_orderbook_events : symbol:string -> start_pos:int -> Types.orderbook_event list

  (** Iterate over orderbook events from [start_pos] without allocating
      an intermediate list. Returns the new read position. *)
  val iter_orderbook_events
    :  symbol:string
    -> start_pos:int
    -> (Types.orderbook_event -> unit)
    -> int

  (** Iterate over orderbook events from [start_pos], extracting only
      top-of-book (best bid, best ask) without allocating converted arrays.
      Callback receives [(bid_price, bid_size, ask_price, ask_size)].
      Returns the new read position. *)
  val iter_top_of_book_events
    :  symbol:string
    -> start_pos:int
    -> (float -> float -> float -> float -> unit)
    -> int

  (** Return the current write position of the execution feed ring buffer
      for [symbol]. Used as the starting cursor for [read_execution_events]. *)
  val get_execution_feed_position : symbol:string -> int

  (** Return [true] if the execution feed has received its initial data
      snapshot for [symbol]. Used by domain workers to gate strategy
      execution until open order state is populated. *)
  val has_execution_data : symbol:string -> bool

  (** Read execution events from [start_pos] up to the current write
      position. Returns a newly allocated list of events. *)
  val read_execution_events : symbol:string -> start_pos:int -> Types.execution_event list

  (** Iterate over execution events from [start_pos] without allocating
      an intermediate list. Returns the new read position. *)
  val iter_execution_events
    :  symbol:string
    -> start_pos:int
    -> (Types.execution_event -> unit)
    -> int

  (** Fold over open orders for [symbol] without allocating an intermediate
      list. Applies [f] to each open order, threading the accumulator. *)
  val fold_open_orders
    :  symbol:string
    -> init:'a
    -> f:('a -> Types.open_order -> 'a)
    -> 'a

  (** Fast path iterator that avoids creating Types.open_order intermediate records.
      Yields primitive order values to the provided closure callback directly. *)
  val iter_open_orders_fast
    :  symbol:string
    -> (string -> float -> float -> string -> int option -> unit)
    -> unit

  (** Return a fast path closure for fetching the current orderbook position without lock acquisition/hash lookup overhead. *)
  val get_orderbook_position_fast : symbol:string -> unit -> int

  (** Return a fast path closure for fetching top-of-book data without hash lookup overhead. *)
  val get_top_of_book_fast
    :  symbol:string
    -> unit
    -> (float * float * float * float) option

  (** Return a fast path closure for fetching the current execution feed position without hash lookup overhead. *)
  val get_execution_feed_position_fast : symbol:string -> unit -> int

  (** Return a fast path closure for checking if the execution feed has initial data without hash lookup overhead. *)
  val has_execution_data_fast : symbol:string -> unit -> bool

  (* ---- Instrument metadata ---- *)

  (** Return the minimum price increment (tick size) for [symbol],
      or [None] if instrument metadata is unavailable. *)
  val get_price_increment : symbol:string -> float option

  (** Return the minimum quantity increment (lot step) for [symbol],
      or [None] if instrument metadata is unavailable. *)
  val get_qty_increment : symbol:string -> float option

  (** Return the minimum order quantity for [symbol], or [None] if
      instrument metadata is unavailable. *)
  val get_qty_min : symbol:string -> float option

  (** Round [price] to the exchange's valid precision for [symbol].
      Hyperliquid: 5 significant figures, capped at
      (MAX_DECIMALS - szDecimals) decimal places.
      Kraken: nearest price_increment tick. *)
  val round_price : symbol:string -> price:float -> float

  (** Return cached (maker_fee, taker_fee) for [symbol]. Each component
      is [None] if the fee has not been fetched. *)
  val get_fees : symbol:string -> float option * float option
end

(** Dynamic registry mapping exchange names to their [(module S)]
    implementations. Backed by a [Hashtbl] for O(1) lookup. *)
module Registry = struct
  (** Internal hash table storing registered exchange modules, keyed by name. *)
  let _exchanges : (string, (module S)) Hashtbl.t = Hashtbl.create 4

  (** Register an exchange module. Replaces any existing entry with the
      same [Exchange.name]. *)
  let register (module Exchange : S) =
    Hashtbl.replace _exchanges Exchange.name (module Exchange)
  ;;

  (** Look up a registered exchange module by name. Returns [None] if
      no module has been registered under that name. *)
  let get name = Hashtbl.find_opt _exchanges name

  (** Return all registered exchange names. Used by the dashboard to
      enumerate balances across all exchanges, not just those with
      configured trading strategies. *)
  let get_all_names () =
    Hashtbl.fold (fun name _ acc -> name :: acc) _exchanges []
    |> List.sort_uniq String.compare
  ;;
end

(** Oracle data-venue interface: the contract the capital oracle needs from
    an exchange beyond live trading - historical daily bars, session
    calendars, fees, account balances and instrument metadata.

    Independent of the live-trading signature [S] above (which has no
    historical-bars or calendar concept); a venue implements BOTH, or only
    [S] if it does not participate in oracle modeling. The runtime registry
    lives in [Oracle.Registry], mirroring [Registry] above.

    Raw-bar contract: implementations return RAW source rows (any order);
    the oracle sorts, de-duplicates and normalizes centrally
    ([Oracle_calendar.normalize_bars] on every read, cache and direct fetch
    alike), so a corrected normalization rule self-heals without a refetch
    (same philosophy as the v2 history cache). Implementations may still
    pre-clean (normalize is idempotent) when they need source-level logging. *)
module Oracle = struct
  module type S = sig
    (** Human-readable venue name used as the registry key (e.g. "kraken"). *)
    val name : string

    (** Session calendar semantics: [Crypto] (session = day) or [Equity]
        (market sessions). Drives gap detection and horizon labels. *)
    val calendar_kind : Types.calendar_kind

    (** Fetch historical daily bars for [symbol] starting at [from] (ISO date
        of the first day to include; [None] = full history from the venue's
        earliest available data). [feed] and [end_date] are Alpaca-only knobs
        (IEX/SIP feed, request window end); other venues ignore them. Returns
        raw source bars; the oracle normalizes centrally. *)
    val fetch_bars
      :  ?feed:string
      -> ?end_date:string
      -> from:string option
      -> symbol:string
      -> unit
      -> Types.bar list Lwt.t

    (** Fetch expected session dates over [start_date]..[end_date]. Equity
        venues build their session model from this; crypto venues return []. *)
    val fetch_calendar : start_date:string -> end_date:string -> string list Lwt.t

    (** Fetch authoritative (maker, taker) fees for [symbol] from the venue.
        The oracle falls back to [default_fees] on failure. *)
    val fetch_fees : testnet:bool -> symbol:string -> (float * float) Lwt.t

    (** Venue default (maker, taker) fees, used offline and as the failed-fetch
        fallback. [symbol]-parameterized (Hyperliquid spot vs perp differ). *)
    val default_fees : symbol:string -> float * float

    (** One-shot account balance fetch returning already-normalized
        (asset, available, total) triples (e.g. XXBT -> BTC, UBTC -> BTC).
        The oracle wraps them into its own balance snapshot record. *)
    val fetch_balances
      :  testnet:bool
      -> ((string * float * float) list, string) result Lwt.t

    (** Populate instrument metadata (price tick / lot size) for [symbols] so
        the live registry's [get_price_increment] / [get_qty_increment]
        answer. No-op for venues with static ticks. *)
    val init_instruments : testnet:bool -> symbols:string list -> unit Lwt.t
  end

  (** Dynamic registry mapping venue names to their [(module S)]
      implementations, mirroring [Registry] above. A venue registers here
      (typically from its own library at module load, see the
      force-registration pattern in the oracle's [Oracle_venues]) to
      participate in oracle modeling. [Hashtbl.replace] semantics: a later
      registration overrides an earlier one under the same name. *)
  module Registry = struct
    (** Internal hash table storing registered oracle-venue modules, keyed by
        [Oracle.S.name]. *)
    let _venues : (string, (module S)) Hashtbl.t = Hashtbl.create 4

    (** Register an oracle-venue module. Replaces any existing entry with the
        same [name]. *)
    let register (module V : S) = Hashtbl.replace _venues V.name (module V)

    (** Look up a registered oracle-venue module by name. Returns [None] when
        no module has been registered under that name. *)
    let get name = Hashtbl.find_opt _venues name

    (** Return all registered venue names, sorted. *)
    let get_all_names () =
      Hashtbl.fold (fun name _ acc -> name :: acc) _venues []
      |> List.sort_uniq String.compare
    ;;
  end
end
