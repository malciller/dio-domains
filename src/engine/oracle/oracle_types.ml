(* Oracle_types - shared data types for the capital oracle. *)

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

(** One asset's daily merged price history. [gaps] is informational only: no
    gap tolerance, no forward filling. *)
type series =
  { symbol : string
  ; calendar_kind : calendar_kind
  ; bars : bar array
  ; gaps : gap list
  }
