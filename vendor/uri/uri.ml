(** In-tree URI implementation for the subset the engine uses.

    The application only needs absolute HTTPS/WSS URLs, percent-encoding, query
    construction, and the host/port accessors. This module keeps the same names as the
    [uri] package so existing call sites do not change. *)

type t =
  { scheme : string option
  ; userinfo : string option
  ; host : string option
  ; port : int option
  ; path : string
  ; query : (string * string list) list
  ; fragment : string option
  }

let empty =
  { scheme = None
  ; userinfo = None
  ; host = None
  ; port = None
  ; path = "/"
  ; query = []
  ; fragment = None
  }
;;

let is_unreserved c =
  match c with
  | 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '-' | '.' | '_' | '~' -> true
  | _ -> false
;;

let hex = "0123456789ABCDEF"

let pct_encode_char b c =
  let v = Char.code c in
  Buffer.add_char b '%';
  Buffer.add_char b hex.[v lsr 4];
  Buffer.add_char b hex.[v land 0xf]
;;

let pct_encode ?scheme:_ ?component:_ s =
  let b = Buffer.create (String.length s) in
  String.iter
    (fun c -> if is_unreserved c then Buffer.add_char b c else pct_encode_char b c)
    s;
  Buffer.contents b
;;

let hex_value c =
  match c with
  | '0' .. '9' -> Char.code c - Char.code '0'
  | 'a' .. 'f' -> Char.code c - Char.code 'a' + 10
  | 'A' .. 'F' -> Char.code c - Char.code 'A' + 10
  | _ -> -1
;;

let pct_decode s =
  let b = Buffer.create (String.length s) in
  let n = String.length s in
  let rec loop i =
    if i >= n
    then ()
    else if s.[i] = '%' && i + 2 < n
    then (
      let hi = hex_value s.[i + 1] in
      let lo = hex_value s.[i + 2] in
      if hi >= 0 && lo >= 0
      then (
        Buffer.add_char b (Char.chr ((hi lsl 4) lor lo));
        loop (i + 3))
      else (
        Buffer.add_char b s.[i];
        loop (i + 1)))
    else (
      Buffer.add_char b s.[i];
      loop (i + 1))
  in
  loop 0;
  Buffer.contents b
;;

let split_first c s =
  match String.index_opt s c with
  | None -> s, None
  | Some i -> String.sub s 0 i, Some (String.sub s (i + 1) (String.length s - i - 1))
;;

let split_on_first_string sep s =
  let n = String.length sep in
  let rec find i =
    if i + n > String.length s
    then None
    else if String.sub s i n = sep
    then Some (String.sub s 0 i, String.sub s (i + n) (String.length s - i - n))
    else find (i + 1)
  in
  find 0
;;

let parse_query q =
  if q = ""
  then []
  else (
    let parts = String.split_on_char '&' q in
    let table = Hashtbl.create 8 in
    let order = ref [] in
    List.iter
      (fun part ->
        if part <> ""
        then (
          let k, v = split_first '=' part in
          let k = pct_decode k in
          let v =
            match v with
            | None -> ""
            | Some v -> pct_decode v
          in
          if not (Hashtbl.mem table k) then order := k :: !order;
          let vs = Option.value (Hashtbl.find_opt table k) ~default:[] in
          Hashtbl.replace table k (vs @ [ v ])))
      parts;
    List.rev_map (fun k -> k, Option.value (Hashtbl.find_opt table k) ~default:[]) !order)
;;

let parse_authority authority =
  let userinfo, hostport =
    match String.rindex_opt authority '@' with
    | None -> None, authority
    | Some i ->
      ( Some (String.sub authority 0 i)
      , String.sub authority (i + 1) (String.length authority - i - 1) )
  in
  let host, port =
    if String.length hostport > 0 && hostport.[0] = '['
    then (
      match String.index_opt hostport ']' with
      | None -> hostport, None
      | Some i ->
        let host = String.sub hostport 0 (i + 1) in
        if i + 1 < String.length hostport && hostport.[i + 1] = ':'
        then (
          let p = String.sub hostport (i + 2) (String.length hostport - i - 2) in
          host, int_of_string_opt p)
        else host, None)
    else (
      match String.rindex_opt hostport ':' with
      | Some i when i > 0 ->
        let p = String.sub hostport (i + 1) (String.length hostport - i - 1) in
        (match int_of_string_opt p with
         | Some _ as port -> String.sub hostport 0 i, port
         | None -> hostport, None)
      | _ -> hostport, None)
  in
  userinfo, (if host = "" then None else Some host), port
;;

let of_string s =
  let without_fragment, fragment = split_first '#' s in
  let base, query = split_first '?' without_fragment in
  let base, scheme =
    match split_on_first_string "://" base with
    | Some (scheme, rest) -> rest, Some (String.lowercase_ascii scheme)
    | None -> base, None
  in
  match scheme with
  | None ->
    { empty with
      path = (if base = "" then "/" else base)
    ; query = parse_query (Option.value query ~default:"")
    }
  | Some scheme ->
    let authority, path = split_first '/' base in
    let userinfo, host, port = parse_authority authority in
    let path = if path = None then "/" else "/" ^ Option.get path in
    { scheme = Some scheme
    ; userinfo
    ; host
    ; port
    ; path
    ; query = parse_query (Option.value query ~default:"")
    ; fragment
    }
;;

let encoded_query q =
  let b = Buffer.create 32 in
  let first = ref true in
  List.iter
    (fun (k, vs) ->
      let add_one v =
        if !first then first := false else Buffer.add_char b '&';
        Buffer.add_string b (pct_encode k);
        if v <> ""
        then (
          Buffer.add_char b '=';
          Buffer.add_string b (pct_encode v))
      in
      match vs with
      | [] -> add_one ""
      | _ -> List.iter add_one vs)
    q;
  Buffer.contents b
;;

let encoded_of_query ?scheme:_ q = encoded_query q

let to_string ?pct_encoder:_ t =
  let b = Buffer.create 64 in
  (match t.scheme with
   | None -> ()
   | Some s ->
     Buffer.add_string b s;
     Buffer.add_string b "://");
  (match t.userinfo with
   | None -> ()
   | Some u ->
     Buffer.add_string b u;
     Buffer.add_char b '@');
  (match t.host with
   | None -> ()
   | Some h -> Buffer.add_string b h);
  (match t.port with
   | None -> ()
   | Some p ->
     Buffer.add_char b ':';
     Buffer.add_string b (string_of_int p));
  let path = if t.path = "" then "/" else t.path in
  Buffer.add_string b path;
  if t.query <> []
  then (
    Buffer.add_char b '?';
    Buffer.add_string b (encoded_query t.query));
  (match t.fragment with
   | None -> ()
   | Some f ->
     Buffer.add_char b '#';
     Buffer.add_string b f);
  Buffer.contents b
;;

let path_and_query t =
  let path = if t.path = "" then "/" else t.path in
  if t.query = [] then path else path ^ "?" ^ encoded_query t.query
;;

let scheme t = t.scheme
let host t = t.host
let port t = t.port
let path t = t.path
let query t = t.query
let fragment t = t.fragment
let userinfo t = t.userinfo
let with_query t q = { t with query = q }
let with_query' t q = { t with query = List.map (fun (k, v) -> k, [ v ]) q }
