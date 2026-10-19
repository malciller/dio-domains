(** Minimal UTF-8 folder used by the terminal renderer.

    [notty] only needs [Uutf.String.fold_utf_8]; this implementation decodes standard
    UTF-8 and reports malformed sequences without raising. *)

module String = struct
  type 'a folder = 'a -> int -> [ `Uchar of Uchar.t | `Malformed of string ] -> 'a

  let malformed s start len =
    let len = max 1 (min len (String.length s - start)) in
    String.sub s start len
  ;;

  let fold_utf_8 ?(pos = 0) ?len f acc s =
    let n = String.length s in
    let len =
      match len with
      | Some l -> l
      | None -> n - pos
    in
    let stop = min n (pos + len) in
    let i = ref pos in
    let acc = ref acc in
    let emit start u = acc := f !acc start (`Uchar u) in
    let bad start l = acc := f !acc start (`Malformed (malformed s start l)) in
    while !i < stop do
      let start = !i in
      let b0 = Char.code s.[!i] in
      if b0 < 0x80
      then (
        emit start (Uchar.of_int b0);
        incr i)
      else if b0 land 0xE0 = 0xC0 && start + 1 < stop
      then (
        let b1 = Char.code s.[start + 1] in
        if b1 land 0xC0 = 0x80
        then (
          let u = ((b0 land 0x1F) lsl 6) lor (b1 land 0x3F) in
          if u >= 0x80
          then (
            emit start (Uchar.of_int u);
            i := start + 2)
          else (
            bad start 2;
            i := start + 2))
        else (
          bad start 1;
          incr i))
      else if b0 land 0xF0 = 0xE0 && start + 2 < stop
      then (
        let b1 = Char.code s.[start + 1] in
        let b2 = Char.code s.[start + 2] in
        if b1 land 0xC0 = 0x80 && b2 land 0xC0 = 0x80
        then (
          let u = ((b0 land 0x0F) lsl 12) lor ((b1 land 0x3F) lsl 6) lor (b2 land 0x3F) in
          if u >= 0x800 && not (u >= 0xD800 && u <= 0xDFFF)
          then (
            emit start (Uchar.of_int u);
            i := start + 3)
          else (
            bad start 3;
            i := start + 3))
        else (
          bad start 1;
          incr i))
      else if b0 land 0xF8 = 0xF0 && start + 3 < stop
      then (
        let b1 = Char.code s.[start + 1] in
        let b2 = Char.code s.[start + 2] in
        let b3 = Char.code s.[start + 3] in
        if b1 land 0xC0 = 0x80 && b2 land 0xC0 = 0x80 && b3 land 0xC0 = 0x80
        then (
          let u =
            ((b0 land 0x07) lsl 18)
            lor ((b1 land 0x3F) lsl 12)
            lor ((b2 land 0x3F) lsl 6)
            lor (b3 land 0x3F)
          in
          if u >= 0x10000 && u <= 0x10FFFF
          then (
            emit start (Uchar.of_int u);
            i := start + 4)
          else (
            bad start 4;
            i := start + 4))
        else (
          bad start 1;
          incr i))
      else (
        bad start 1;
        incr i)
    done;
    !acc
  ;;
end
