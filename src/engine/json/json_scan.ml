(** Allocation-light, single-pass JSON field extraction.

    The feed parsers build a full [Yojson.Safe.t] DOM per frame and then walk it with
    [member]/[to_*], which allocates a variant node per token and promotes through the
    minor heap. These helpers instead locate a named field's value *span* inside the raw
    string, so a caller extracts only the few fields it needs and pays no AST.

    Spans are [(start, stop)] offsets into the source string, [stop] exclusive. The leaf
    parsers and skippers are [@zero_alloc] (compiler-checked): nothing here allocates
    except [string_of_span] (the extracted string is the result), the [Some] returned by
    [find_field], and the [int * int] interior helpers. The int-based parsers and scanners
    are checked outright; the float parser ([float_of_span]) is ref-free and carries
    [@zero_alloc assume] because its intermediate arithmetic is unboxed only under the
    flambda release build (-O3) and the checked form would be a false positive on vanilla
    compilers. Nested objects/arrays and string escapes are handled by
    [skip_value]/[skip_string]; keys are compared without allocation.

    The bounds are authoritative: callers pass the *interior* of an object or array
    (between its braces/brackets, via [object_interior]/[array_interior]), and scanning
    never crosses [hi]. *)

let is_ws c =
  match c with
  | ' ' | '\t' | '\n' | '\r' -> true
  | _ -> false
[@@zero_alloc]
;;

let rec skip_ws s i hi = if i < hi && is_ws s.[i] then skip_ws s (i + 1) hi else i
[@@zero_alloc]
;;

(** [skip_string_from s j hi]: scan from [j] (just past an opening quote) to just past the
    closing quote, honouring backslash escapes. [skip_string] is the wrapper that consumes
    the opening quote: [i] is the opening quote and returns the index after the closing
    quote. *)
let rec skip_string_from s j hi =
  if j >= hi
  then j
  else (
    match s.[j] with
    | '\\' -> skip_string_from s (j + 2) hi
    | '"' -> j + 1
    | _ -> skip_string_from s (j + 1) hi)
[@@zero_alloc]
;;

let skip_string s i hi = if i >= hi then i else skip_string_from s (i + 1) hi
[@@zero_alloc]
;;

(** [skip_container s i hi]: [i] is the char after an opening brace/bracket;
    returns the index after its matching close, ignoring braces/brackets inside
    strings. Nested containers are consumed by recursion, so the matching close
    is whichever ['}']/[']'] hits first at this depth. *)
let rec skip_container s i hi =
  if i >= hi
  then i
  else (
    match s.[i] with
    | '"' -> skip_container s (skip_string s i hi) hi
    | '{' | '[' -> skip_container s (skip_container s (i + 1) hi) hi
    | '}' | ']' -> i + 1
    | _ -> skip_container s (i + 1) hi)
[@@zero_alloc]
;;

(** [skip_scalar s i hi]: index of the delimiter (whitespace, comma, close brace/bracket)
    ending an unscanned scalar starting at [i]. *)
let rec skip_scalar s i hi =
  if i >= hi
  then i
  else (
    match s.[i] with
    | ',' | '}' | ']' | ' ' | '\t' | '\n' | '\r' -> i
    | _ -> skip_scalar s (i + 1) hi)
[@@zero_alloc]
;;

(** [skip_value s i hi]: returns the index just past the JSON value at [i], skipping
    strings, nested containers, and unscanned scalars. *)
let skip_value s i hi =
  let i = skip_ws s i hi in
  if i >= hi
  then i
  else (
    match s.[i] with
    | '"' -> skip_string s i hi
    | '{' | '[' -> skip_container s (i + 1) hi
    | _ -> skip_scalar s i hi)
[@@zero_alloc]
;;

(** Interior offsets of an object/array whose value span is [i, j): [i] points
    at the opening brace/bracket, [j] just past the matching close. *)
let object_interior s i j =
  ignore s;
  i + 1, j - 1
;;

let array_interior s i j =
  ignore s;
  i + 1, j - 1
;;

(** [find_field s lo hi key]: the value span of the top-level [key] in the
    object interior [lo, hi). [None] if absent. Does not descend into nested
    objects, so a same-named nested key is not matched. *)
let find_field s lo hi key =
  let klen = String.length key in
  let rec go i =
    let i = skip_ws s i hi in
    if i >= hi || s.[i] <> '"'
    then None
    else (
      let key_end = skip_string s i hi in
      let matches =
        let kstart = i + 1
        and kend = key_end - 1 in
        kend - kstart = klen
        &&
        let rec eq k = k >= klen || (s.[kstart + k] = key.[k] && eq (k + 1)) in
        eq 0
      in
      let after = skip_ws s key_end hi in
      if after >= hi || s.[after] <> ':'
      then None
      else (
        let vstart = skip_ws s (after + 1) hi in
        let vend = skip_value s vstart hi in
        if matches
        then Some (vstart, vend)
        else (
          let next = skip_ws s vend hi in
          if next < hi && s.[next] = ',' then go (next + 1) else None)))
  in
  go lo
;;

(** [array_fold s lo hi init f]: fold over the elements of the array interior
    [lo, hi), giving each element's span. Allocation-free. *)
let array_fold s lo hi init f =
  let rec go i acc =
    let i = skip_ws s i hi in
    if i >= hi
    then acc
    else (
      let e = skip_value s i hi in
      let acc = f acc i e in
      let i = skip_ws s e hi in
      if i < hi && s.[i] = ',' then go (i + 1) acc else acc)
  in
  go lo init
;;

let array_iter s lo hi f = array_fold s lo hi () (fun () i e -> f i e)
let value_is_string s i = i < String.length s && s.[i] = '"' [@@zero_alloc]

(** Accumulate the integer value of the digit run starting at [i] into [acc]. Shared by
    the exponent parser and [int_of_span]. *)
let rec int_digits s i hi acc =
  if i < hi && s.[i] >= '0' && s.[i] <= '9'
  then int_digits s (i + 1) hi ((acc * 10) + (Char.code s.[i] - 48))
  else acc
[@@zero_alloc]
;;

(** [10.0] raised to [n] by repeated multiplication; exact for [n <= 15], within a few
    ulps of libm outside that. Allocation-free. *)
let rec pow10 acc n = if n <= 0 then acc else pow10 (acc *. 10.0) (n - 1)
[@@zero_alloc assume]
;;

(** Parse a JSON number from [i, j) without allocating. Tolerant of [j] landing
    on a trailing delimiter (stops at the first non-number char). Ref-free (a
    shared mantissa accumulator) so it holds a [@zero_alloc] annotation. *)
let rec parse_float s i hi acc scale =
  if i >= hi
  then acc
  else if s.[i] >= '0' && s.[i] <= '9'
  then (
    let d = Char.code s.[i] - 48 in
    if scale = 1.0
    then parse_float s (i + 1) hi ((acc *. 10.0) +. float d) scale
    else parse_float s (i + 1) hi (acc +. (float d *. scale)) (scale /. 10.0))
  else if s.[i] = '.'
  then if scale = 1.0 then parse_float s (i + 1) hi acc 0.1 else acc
  else if s.[i] = 'e' || s.[i] = 'E'
  then (
    let k = i + 1 in
    let neg = k < hi && s.[k] = '-' in
    let k = if neg || (k < hi && s.[k] = '+') then k + 1 else k in
    let e = int_digits s k hi 0 in
    let p = pow10 1.0 e in
    if neg then acc /. p else acc *. p)
  else acc
[@@zero_alloc assume]
;;

let float_of_span s i j =
  let i = skip_ws s i j in
  let neg = i < j && s.[i] = '-' in
  let i = if neg then i + 1 else i in
  let value = parse_float s i j 0.0 1.0 in
  if neg then -.value else value
[@@zero_alloc assume]
;;

(** Parse a JSON integer (sign + digits; fraction truncated) from [i, j). *)
let int_of_span s i j =
  let i = skip_ws s i j in
  let neg = i < j && s.[i] = '-' in
  let i = if neg then i + 1 else i in
  let v = int_digits s i j 0 in
  if neg then -v else v
[@@zero_alloc]
;;

(** Extract a JSON string value from [i, j) (quotes included), unescaping the
    common backslash escapes. Allocates the result, which is the payload. *)
let string_of_span s i j =
  if not (value_is_string s i)
  then ""
  else (
    let buf = Buffer.create (max 0 (j - i)) in
    let rec go k =
      if k >= j - 1
      then ()
      else (
        match s.[k] with
        | '\\' when k + 1 < j - 1 ->
          (match s.[k + 1] with
           | 'n' -> Buffer.add_char buf '\n'
           | 't' -> Buffer.add_char buf '\t'
           | 'r' -> Buffer.add_char buf '\r'
           | 'b' -> Buffer.add_char buf '\b'
           | 'f' -> Buffer.add_char buf '\012'
           | c -> Buffer.add_char buf c);
          go (k + 2)
        | '"' -> ()
        | c ->
          Buffer.add_char buf c;
          go (k + 1))
    in
    go (i + 1);
    Buffer.contents buf)
;;
