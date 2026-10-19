(** Lwt body wrapper, matching the subset of [cohttp-lwt] the engine uses. *)

open Lwt.Infix

module Body = struct
  type t =
    [ `String of string
    | `Stream of string Lwt_stream.t
    ]

  let of_string s = `String s

  let to_string = function
    | `String s -> Lwt.return s
    | `Stream stream ->
      let buf = Buffer.create 1024 in
      Lwt_stream.iter (fun chunk -> Buffer.add_string buf chunk) stream
      >|= fun () -> Buffer.contents buf
  ;;

  let to_stream = function
    | `Stream stream -> stream
    | `String s -> Lwt_stream.of_list [ s ]
  ;;

  let drain_body body = to_string body >|= fun _ -> ()

  let write_body write = function
    | `String s -> write s
    | `Stream stream -> Lwt_stream.iter_s write stream
  ;;
end
