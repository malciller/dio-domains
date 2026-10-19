(** In-tree HTTP client types used by the exchange adapters.

    This replaces the [cohttp] core library with the small surface the engine actually
    uses: status codes, case-insensitive headers, request/response records, and the
    client-side request writer. Server-side code and sexp serializers are intentionally
    absent. *)

module Code = struct
  type status =
    [ `Code of int
    | `Switching_protocols
    | `OK
    | `Created
    | `Accepted
    | `No_content
    | `Bad_request
    | `Unauthorized
    | `Forbidden
    | `Not_found
    | `Too_many_requests
    | `Internal_server_error
    | `Bad_gateway
    | `Service_unavailable
    ]

  let code_of_status = function
    | `Code n -> n
    | `Switching_protocols -> 101
    | `OK -> 200
    | `Created -> 201
    | `Accepted -> 202
    | `No_content -> 204
    | `Bad_request -> 400
    | `Unauthorized -> 401
    | `Forbidden -> 403
    | `Not_found -> 404
    | `Too_many_requests -> 429
    | `Internal_server_error -> 500
    | `Bad_gateway -> 502
    | `Service_unavailable -> 503
  ;;

  let status_of_code = function
    | 101 -> `Switching_protocols
    | 200 -> `OK
    | 201 -> `Created
    | 202 -> `Accepted
    | 204 -> `No_content
    | 400 -> `Bad_request
    | 401 -> `Unauthorized
    | 403 -> `Forbidden
    | 404 -> `Not_found
    | 429 -> `Too_many_requests
    | 500 -> `Internal_server_error
    | 502 -> `Bad_gateway
    | 503 -> `Service_unavailable
    | n -> `Code n
  ;;

  let is_success n = n >= 200 && n < 300
  let is_error n = n >= 400

  let string_of_status s =
    match s with
    | `Code n -> Printf.sprintf "HTTP %d" n
    | `Switching_protocols -> "101 Switching Protocols"
    | `OK -> "200 OK"
    | `Created -> "201 Created"
    | `Accepted -> "202 Accepted"
    | `No_content -> "204 No Content"
    | `Bad_request -> "400 Bad Request"
    | `Unauthorized -> "401 Unauthorized"
    | `Forbidden -> "403 Forbidden"
    | `Not_found -> "404 Not Found"
    | `Too_many_requests -> "429 Too Many Requests"
    | `Internal_server_error -> "500 Internal Server Error"
    | `Bad_gateway -> "502 Bad Gateway"
    | `Service_unavailable -> "503 Service Unavailable"
  ;;
end

module Header = struct
  type t = (string * string) list

  let init () = []
  let init_with k v = [ k, v ]
  let add t k v = (k, v) :: t
  let add_list t l = l @ t
  let of_list l = l
  let to_list t = t

  let get t k =
    let k = String.lowercase_ascii k in
    List.find_map
      (fun (k', v) -> if String.lowercase_ascii k' = k then Some v else None)
      t
  ;;

  let get_multi t k =
    let k = String.lowercase_ascii k in
    List.filter_map
      (fun (k', v) -> if String.lowercase_ascii k' = k then Some v else None)
      t
  ;;

  let set t k v =
    (k, v)
    :: List.filter
         (fun (k', _) -> String.lowercase_ascii k' <> String.lowercase_ascii k)
         t
  ;;

  let to_string t =
    let b = Buffer.create 256 in
    List.iter
      (fun (k, v) ->
        Buffer.add_string b k;
        Buffer.add_string b ": ";
        Buffer.add_string b v;
        Buffer.add_string b "\r\n")
      t;
    Buffer.contents b
  ;;
end

module Request = struct
  type meth =
    [ `GET
    | `POST
    | `PUT
    | `PATCH
    | `DELETE
    | `HEAD
    | `OPTIONS
    | `Other of string
    ]

  type t =
    { meth : meth
    ; uri : Uri.t
    ; headers : Header.t
    ; body : string
    }

  let make ?(meth = `GET) ?(headers = Header.init ()) ?(body = "") uri =
    { meth; uri; headers; body }
  ;;

  let meth t = t.meth
  let uri t = t.uri
  let headers t = t.headers
  let body t = t.body

  let string_of_meth = function
    | `GET -> "GET"
    | `POST -> "POST"
    | `PUT -> "PUT"
    | `PATCH -> "PATCH"
    | `DELETE -> "DELETE"
    | `HEAD -> "HEAD"
    | `OPTIONS -> "OPTIONS"
    | `Other s -> s
  ;;
end

module Response = struct
  type version =
    [ `HTTP_1_0
    | `HTTP_1_1
    ]

  type t =
    { status : Code.status
    ; headers : Header.t
    ; version : version
    }

  let status t = t.status
  let headers t = t.headers
  let version t = t.version
end
