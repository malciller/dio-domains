let of_inet_addr addr =
  let s = Unix.string_of_inet_addr addr in
  if String.contains s ':' then Ipaddr.V6 s else Ipaddr.of_string s
;;
