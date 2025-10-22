

module StringRegistry = Concurrency.Event_registry.Make(
  struct
    type t = string
    let equal = String.equal
    let hash = Hashtbl.hash
  end
)(struct type t = int end)

let test_registry_creation () =
  let registry = StringRegistry.create () in
  Alcotest.(check int) "registry starts empty" 0 (StringRegistry.size registry)

let test_registry_replace () =
  let registry = StringRegistry.create () in
  let _ = StringRegistry.replace registry "key1" 42 in
  Alcotest.(check int) "registry size after replace" 1 (StringRegistry.size registry);
  Alcotest.(check (option int)) "registry find works" (Some 42) (StringRegistry.find registry "key1")

let test_registry_remove () =
  let registry = StringRegistry.create () in
  let _ = StringRegistry.replace registry "key1" 42 in
  let _ = StringRegistry.remove registry "key1" in
  Alcotest.(check int) "registry size after remove" 0 (StringRegistry.size registry);
  Alcotest.(check (option int)) "registry find after remove" None (StringRegistry.find registry "key1")

let () =
  Alcotest.run "Event Registry" [
    "creation", [
      Alcotest.test_case "registry creation" `Quick test_registry_creation;
    ];
    "operations", [
      Alcotest.test_case "replace" `Quick test_registry_replace;
      Alcotest.test_case "remove" `Quick test_registry_remove;
    ];
  ]
