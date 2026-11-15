(** Kraken account tier selection.

    Kraken exposes two order-entry transports for the same account: the v2 WebSocket (what
    the engine has always used) and the Unified FIX 4.4 API. FIX is only provisioned for
    institutional accounts, so the transport is a property of the account, not of the
    strategy: it is read once from [KRAKEN_ACCOUNT_TIER] and every Kraken module asks this
    module instead of hard-coding a transport.

    The default is {!Standard}. An [enterprise] tier without complete FIX credentials is
    refused rather than honoured, so a typo in the env file downgrades to the working
    WebSocket path instead of leaving the account unable to place orders. *)

let section = "kraken_account_tier"

(** Kraken account tier. [Standard] uses the WebSocket API, [Enterprise] uses FIX. *)
type t =
  | Standard
  | Enterprise

let to_string = function
  | Standard -> "standard"
  | Enterprise -> "enterprise"
;;

(** Parses the configured tier name. Accepts the documented spellings plus the synonyms
    operators reach for; anything else is an error the caller reports with the raw value. *)
let of_string s =
  match String.lowercase_ascii (String.trim s) with
  | "" | "standard" | "normal" | "retail" -> Ok Standard
  | "enterprise" | "institutional" | "fix" -> Ok Enterprise
  | other -> Error other
;;

let tier_env_var = "KRAKEN_ACCOUNT_TIER"

let non_empty_env var =
  match Sys.getenv_opt var with
  | Some v -> String.trim v <> ""
  | None -> false
;;

(** True when every credential a FIX logon needs is present. The tier alone cannot select
    FIX: without the CompID pair and the FIX key there is no session to open. *)
let fix_credentials_present () =
  non_empty_env "KRAKEN_FIX_API_KEY"
  && non_empty_env "KRAKEN_FIX_API_SECRET"
  && non_empty_env "KRAKEN_FIX_SENDER_COMP_ID"
;;

(** Resolves the tier from the raw configuration value and credential availability. Split
    out from {!detect} so the decision is testable without touching the process
    environment. *)
let resolve ~requested ~has_fix_credentials =
  match of_string requested with
  | Error other ->
    Logging.warn_f
      ~section
      "%s=%S is not a known Kraken account tier (expected \"standard\" or \
       \"enterprise\"); using \"standard\""
      tier_env_var
      other;
    Standard
  | Ok Standard -> Standard
  | Ok Enterprise ->
    if has_fix_credentials
    then Enterprise
    else (
      Logging.error_f
        ~section
        "%s=enterprise requires KRAKEN_FIX_API_KEY, KRAKEN_FIX_API_SECRET and \
         KRAKEN_FIX_SENDER_COMP_ID; falling back to the WebSocket transport"
        tier_env_var;
      Standard)
;;

let detect () =
  resolve
    ~requested:(Option.value (Sys.getenv_opt tier_env_var) ~default:"standard")
    ~has_fix_credentials:(fix_credentials_present ())
;;

(* Resolved once: the env file is read at startup and the tier cannot change while the
   process runs. Cached behind an atomic so hot callers pay one load, not a getenv. *)
let cached : t option Atomic.t = Atomic.make None

(** The configured tier, resolved on first use. *)
let current () =
  match Atomic.get cached with
  | Some tier -> tier
  | None ->
    let tier = detect () in
    let rec publish () =
      match Atomic.get cached with
      | Some existing -> existing
      | None ->
        if Atomic.compare_and_set cached None (Some tier) then tier else publish ()
    in
    publish ()
;;

(** True when order entry and execution reports must go over FIX. *)
let is_fix () = current () = Enterprise
