(* Oracle types - the shared data vocabulary of the capital oracle. *)

type calendar_kind = Dio_exchange.Exchange_intf.Types.calendar_kind =
  | Crypto
  | Equity

type bar = Dio_exchange.Exchange_intf.Types.bar =
  { date : string
  ; open_ : float
  ; high : float
  ; low : float
  ; close : float
  ; volume : float
  }

(** A run of missing sessions. [after]/[before] are the ISO dates bounding the
    gap; [missing_days] is the number of expected sessions skipped (calendar
    days for crypto). *)
type gap =
  { after : string
  ; before : string
  ; missing_days : int
  }

(** One asset's daily price history as fetched and merged by Oracle_fetch:
    an ordered bar array plus any gaps detected against the asset's session
    calendar (informational - there is no gap tolerance and no forward
    filling; what the sources provide is what consumers see). *)
type series =
  { symbol : string
  ; calendar_kind : calendar_kind
  ; bars : bar array
  ; gaps : gap list
  }
