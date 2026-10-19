open Lwt.Infix

let rec iter_s f = function
  | [] -> Lwt.return_unit
  | x :: xs -> f x >>= fun () -> iter_s f xs
;;

let iter f xs =
  iter_s
    (fun x ->
      f x;
      Lwt.return_unit)
    xs
;;

(* The functions are all started before the results are collected, so the operations
   overlap even though collection is sequential. *)
let map_p f xs =
  let promises = List.map f xs in
  let rec collect acc = function
    | [] -> Lwt.return (List.rev acc)
    | p :: ps -> p >>= fun v -> collect (v :: acc) ps
  in
  collect [] promises
;;

let iter_p f xs = map_p f xs >|= fun _ -> ()

let rec map_s f = function
  | [] -> Lwt.return []
  | x :: xs -> f x >>= fun v -> map_s f xs >|= fun vs -> v :: vs
;;

let mapi_s f xs =
  let rec loop i = function
    | [] -> Lwt.return []
    | x :: xs -> f i x >>= fun v -> loop (i + 1) xs >|= fun vs -> v :: vs
  in
  loop 0 xs
;;

let rec fold_left_s f acc = function
  | [] -> Lwt.return acc
  | x :: xs -> f acc x >>= fun acc -> fold_left_s f acc xs
;;

let rec filter_map_s f = function
  | [] -> Lwt.return []
  | x :: xs ->
    f x
    >>= fun v ->
    filter_map_s f xs
    >|= fun vs ->
    (match v with
     | Some v -> v :: vs
     | None -> vs)
;;

let filter_map_p f xs = map_p f xs >|= fun vs -> List.filter_map (fun v -> v) vs
