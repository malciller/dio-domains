(** TWS API protocol codec.

    Encodes outbound messages as length-prefixed, null-delimited field
    sequences and decodes inbound messages from the same format. Pure
    functions — no IO, no state. 

    Wire format:
    {v
      ┌──────────────┬──────────┬──────────┬──────────┬───┐
      │ Length (4 BE) │ Field 1  │ \x00     │ Field 2  │...│
      └──────────────┴──────────┴──────────┴──────────┴───┘
    v}

    Each field is a UTF-8 string terminated by a null byte. The 4-byte
    big-endian length prefix covers everything after itself. *)

(* ---- Encoding (outbound) ---- *)

(** [encode_fields fields] joins [fields] with null delimiters and
    prepends a 4-byte big-endian length prefix. Returns a [Bytes.t]
    ready to write to the socket. *)
let encode_fields (fields : string list) : Bytes.t =
  let payload = String.concat "\x00" fields ^ "\x00" in
  let len = String.length payload in
  let buf = Bytes.create (4 + len) in
  Bytes.set_uint8 buf 0 ((len lsr 24) land 0xFF);
  Bytes.set_uint8 buf 1 ((len lsr 16) land 0xFF);
  Bytes.set_uint8 buf 2 ((len lsr 8) land 0xFF);
  Bytes.set_uint8 buf 3 (len land 0xFF);
  Bytes.blit_string payload 0 buf 4 len;
  buf

(** [encode_handshake min_ver max_ver] builds the initial API handshake
    message: ["API\x00"] followed by a length-prefixed version range string. *)
let encode_handshake ~min_ver ~max_ver : Bytes.t =
  let prefix = "API\x00" in
  let version_str = Printf.sprintf "v%d..%d" min_ver max_ver in
  let len = String.length version_str in
  let buf = Bytes.create (String.length prefix + 4 + len) in
  Bytes.blit_string prefix 0 buf 0 (String.length prefix);
  let off = String.length prefix in
  Bytes.set_uint8 buf (off + 0) ((len lsr 24) land 0xFF);
  Bytes.set_uint8 buf (off + 1) ((len lsr 16) land 0xFF);
  Bytes.set_uint8 buf (off + 2) ((len lsr 8) land 0xFF);
  Bytes.set_uint8 buf (off + 3) (len land 0xFF);
  Bytes.blit_string version_str 0 buf (off + 4) len;
  buf

(* ---- Decoding (inbound) ---- *)

(** [decode_fields payload] splits a null-delimited payload into a list
    of string fields. Drops trailing empty fields produced by the
    terminating null. *)
let decode_fields (payload : string) : string list =
  let parts = String.split_on_char '\x00' payload in
  (* Drop the last element if it's empty (trailing null) *)
  match List.rev parts with
  | "" :: rest -> List.rev rest
  | _ -> parts

(** Cursor-based field reader. Avoids allocation on each read by
    operating on the raw field list via pattern matching. *)

(** [read_string fields] pops the next field as a string.
    Returns [(value, remaining_fields)]. *)
let read_string = function
  | s :: rest -> (s, rest)
  | [] -> ("", [])

(** [read_int fields] pops the next field as an int.
    Returns [(value, remaining_fields)]. Defaults to 0 on parse failure. *)
let read_int = function
  | s :: rest ->
      let v = try int_of_string s with _ -> 0 in
      (v, rest)
  | [] -> (0, [])

(** [read_float fields] pops the next field as a float.
    Handles TWS's "1.7976931348623157E308" (Double.MAX_VALUE) as 0.0.
    Returns [(value, remaining_fields)]. *)
let read_float = function
  | s :: rest ->
      let v =
        if s = "" || s = "1.7976931348623157E308" then 0.0
        else try float_of_string s with _ -> 0.0
      in
      (v, rest)
  | [] -> (0.0, [])

(** [read_bool fields] pops the next field as a bool.
    "1" or "true" -> true, anything else -> false.
    Returns [(value, remaining_fields)]. *)
let read_bool = function
  | s :: rest ->
      let v = (s = "1" || s = "true") in
      (v, rest)
  | [] -> (false, [])

(** [skip n fields] drops the next [n] fields. *)
let rec skip n fields =
  if n <= 0 then fields
  else match fields with
    | _ :: rest -> skip (n - 1) rest
    | [] -> []

(** [read_int_max fields] pops the next field as an int, treating
    empty string or MAX_VALUE as [max_int]. *)
let read_int_max = function
  | s :: rest ->
      let v =
        if s = "" || s = "2147483647" then max_int
        else try int_of_string s with _ -> 0
      in
      (v, rest)
  | [] -> (max_int, [])

(* ---- Contract encoding ---- *)

(** [encode_contract c] serializes a contract into the field sequence
    expected by TWS for reqContractDetails / reqMktData / placeOrder.
    The exact field order is dictated by the TWS API protocol version. *)
let encode_contract (c : Ibkr_types.contract) : string list =
  [
    string_of_int c.con_id;
    c.symbol;
    c.sec_type;
    "";     (* lastTradeDateOrContractMonth *)
    "0.0";  (* strike *)
    "";     (* right *)
    c.multiplier;
    c.exchange;
    "";     (* primaryExchange *)
    c.currency;
    c.local_symbol;
    c.trading_class;
    "0";    (* includeExpired *)
  ]

(** [encode_contract_short c] serializes only the minimal contract fields
    needed for reqMktData (no includeExpired/comboLegs). *)
let encode_contract_short (c : Ibkr_types.contract) : string list =
  [
    string_of_int c.con_id;
    c.symbol;
    c.sec_type;
    "";     (* lastTradeDateOrContractMonth *)
    "0.0";  (* strike *)
    "";     (* right *)
    c.multiplier;
    c.exchange;
    "";     (* primaryExchange *)
    c.currency;
    c.local_symbol;
    c.trading_class;
  ]

(* ---- Order encoding ---- *)

(** [encode_order o] serializes the initial order fields for the placeOrder message.
    These are the first set of fields after the contract + secIdType + secId. *)
let encode_order (o : Ibkr_types.order) : string list =
  [
    o.action;
    string_of_float o.total_qty;
    o.order_type;
    (if o.lmt_price = 0.0 then "" else string_of_float o.lmt_price);
    "";     (* auxPrice - for stop orders *)
    o.tif;
    "";     (* ocaGroup *)
    "";     (* account - use default *)
    "";     (* openClose *)
    "0";    (* origin - customer *)
    "";     (* orderRef *)
    "1";    (* transmit *)
    "0";    (* parentId *)
    "0";    (* blockOrder *)
    "0";    (* sweepToFill *)
    "0";    (* displaySize *)
    "0";    (* triggerMethod *)
    "0";    (* outsideRth *)
    "0";    (* hidden *)
  ]

(** [encode_order_tail ()] emits the remaining ~60 fields required by placeOrder.
    All set to empty/default values for simple STK limit/market orders.
    Field order matches ib_insync client.py placeOrder exactly. *)
let encode_order_tail () : string list =
  (* After the basic order fields above, placeOrder expects: *)
  [
    (* --- no comboLegs for non-BAG --- *)
    "";     (* deprecated sharesAllocation / empty string *)
    "0.0";  (* discretionaryAmt *)
    "";     (* goodAfterTime *)
    "";     (* goodTillDate *)
    "";     (* faGroup *)
    "";     (* faMethod *)
    "";     (* faPercentage *)
    (* faProfile removed in v177, but we're v176 so include it *)
    "";     (* faProfile *)
    "";     (* modelCode *)
    "0";    (* shortSaleSlot *)
    "";     (* designatedLocation *)
    "-1";   (* exemptCode *)
    "0";    (* ocaType *)
    "";     (* rule80A *)
    "";     (* settlingFirm *)
    "0";    (* allOrNone *)
    "";     (* minQty *)
    "";     (* percentOffset *)
    "0";    (* eTradeOnly - deprecated but still sent *)
    "0";    (* firmQuoteOnly - deprecated but still sent *)
    "";     (* nbboPriceCap - deprecated but still sent *)
    "0";    (* auctionStrategy *)
    "";     (* startingPrice *)
    "";     (* stockRefPrice *)
    "";     (* delta *)
    "";     (* stockRangeLower *)
    "";     (* stockRangeUpper *)
    "0";    (* overridePercentageConstraints *)
    "";     (* volatility *)
    "";     (* volatilityType *)
    "";     (* deltaNeutralOrderType *)
    "";     (* deltaNeutralAuxPrice *)
    (* no deltaNeutralOrderType so skip deltaNeutral block *)
    "0";    (* continuousUpdate *)
    "";     (* referencePriceType *)
    "";     (* trailStopPrice *)
    "";     (* trailingPercent *)
    "";     (* scaleInitLevelSize *)
    "";     (* scaleSubsLevelSize *)
    "";     (* scalePriceIncrement *)
    (* scalePriceIncrement empty so skip scale block *)
    "";     (* scaleTable *)
    "";     (* activeStartTime *)
    "";     (* activeStopTime *)
    "";     (* hedgeType *)
    (* no hedgeType so skip hedgeParam *)
    "0";    (* optOutSmartRouting *)
    "";     (* clearingAccount *)
    "";     (* clearingIntent *)
    "0";    (* notHeld *)
    "0";    (* deltaNeutralContract = false *)
    "";     (* algoStrategy *)
    (* no algoStrategy so skip algo block *)
    "";     (* algoId *)
    "0";    (* whatIf *)
    "";     (* orderMiscOptions *)
    "0";    (* solicited *)
    "0";    (* randomizeSize *)
    "0";    (* randomizePrice *)
    (* not PEG BENCH, skip peg bench fields *)
    "0";    (* conditions count *)
    (* no conditions so skip conditionsIgnoreRth/conditionsCancelOrder *)
    "";     (* adjustedOrderType *)
    "";     (* triggerPrice *)
    "";     (* lmtPriceOffset *)
    "";     (* adjustedStopPrice *)
    "";     (* adjustedStopLimitPrice *)
    "";     (* adjustedTrailingAmount *)
    "0";    (* adjustableTrailingUnit *)
    "";     (* extOperator *)
    "";     (* softDollarTier.name *)
    "";     (* softDollarTier.val *)
    "";     (* cashQty *)
    "";     (* mifid2DecisionMaker *)
    "";     (* mifid2DecisionAlgo *)
    "";     (* mifid2ExecutionTrader *)
    "";     (* mifid2ExecutionAlgo *)
    "0";    (* dontUseAutoPriceForHedge *)
    "0";    (* isOmsContainer *)
    "0";    (* discretionaryUpToLimitPrice *)
    "0";    (* usePriceMgmtAlgo *)
    "";     (* duration - v158+ *)
    "";     (* postToAts - v160+ *)
    "";     (* autoCancelParent - v162+ *)
    "";     (* advancedErrorOverride - v166+ *)
    "";     (* manualOrderTime - v169+ *)
    (* v170+: skip IBKRATS and PEG BEST/MID fields since not used *)
  ]
