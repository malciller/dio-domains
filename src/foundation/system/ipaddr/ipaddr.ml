(** Minimal IP address type for the connection layer.

    The engine only needs to carry the resolved address from [Lwt_unix.getaddrinfo] into
    [Conduit_lwt_unix.client] and back out for logging. *)

module V4 = struct
  type t = int * int * int * int

  let to_string (a, b, c, d) = Printf.sprintf "%d.%d.%d.%d" a b c d

  let of_string s =
    match String.split_on_char '.' s with
    | [ a; b; c; d ] ->
      (match
         ( int_of_string_opt a
         , int_of_string_opt b
         , int_of_string_opt c
         , int_of_string_opt d )
       with
       | Some a, Some b, Some c, Some d
         when a >= 0
              && a <= 255
              && b >= 0
              && b <= 255
              && c >= 0
              && c <= 255
              && d >= 0
              && d <= 255 -> Some (a, b, c, d)
       | _ -> None)
    | _ -> None
  ;;
end

module V6 = struct
  type t = string

  let to_string s = s
  let of_string s = Some s
end

type t =
  | V4 of V4.t
  | V6 of V6.t

let to_string = function
  | V4 v4 -> V4.to_string v4
  | V6 v6 -> V6.to_string v6
;;

let of_string s =
  if String.contains s ':'
  then V6 (V6.of_string s |> Option.value ~default:s)
  else (
    match V4.of_string s with
    | Some v4 -> V4 v4
    | None -> invalid_arg ("Ipaddr.of_string: " ^ s))
;;
