(** FIX logon authentication.

    Kraken signs the logon with a password derived from the API secret:

    {v
base64( HMAC-SHA512( base64decode(secret),
                            SHA256( "35=A<SOH>34=<seq><SOH>49=<sender><SOH>"
                                    ^ "56=<target><SOH>553=<api_key><SOH>"
                                    ^ nonce ) ) )
    v}

    The same nonce must be carried in [5025] and inside the signed string, and it must be
    within five seconds of Kraken's clock, so it is generated once per logon attempt and
    never cached. *)

let section = "kraken_fix_auth"

(** Milliseconds since the epoch, as FIX tag [5025] wants it. *)
let nonce () = Int64.to_string (Int64.of_float (Unix.gettimeofday () *. 1000.0))

(** Field separator, repeated here so the signed string can be assembled without taking a
    dependency on the codec's mutable builders. *)
let soh = '\x01'

(** The exact byte string Kraken expects to have signed, without the nonce. Exposed for
    tests, which pin the documented construction. *)
let message_input ~msg_seq_num ~sender_comp_id ~target_comp_id ~api_key =
  Printf.sprintf
    "35=A%c34=%s%c49=%s%c56=%s%c553=%s%c"
    soh
    msg_seq_num
    soh
    sender_comp_id
    soh
    target_comp_id
    soh
    api_key
    soh
;;

(** Computes tag [554]. The API secret is base64, and it is the decoded bytes that key the
    HMAC; using the encoded text silently produces a wrong password that Kraken reports as
    a plain logout. *)
let password ~msg_seq_num ~sender_comp_id ~target_comp_id ~api_key ~api_secret ~nonce =
  let input = message_input ~msg_seq_num ~sender_comp_id ~target_comp_id ~api_key in
  let digest = Digestif.SHA256.(digest_string (input ^ nonce) |> to_raw_string) in
  let key = Kraken_common_types.decode_secret_base64 api_secret in
  let signature = Digestif.SHA512.(hmac_string ~key digest |> to_raw_string) in
  match Base64.encode ~pad:true signature with
  | Ok encoded -> encoded
  | Error (`Msg msg) ->
    Logging.error_f ~section "Failed to base64-encode the logon password: %s" msg;
    failwith ("Kraken FIX logon password encoding failed: " ^ msg)
;;
