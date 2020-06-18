open Library
open Conn_manager
open Lwt.Infix

let of_store, to_store =
  let open Capnp.BytesMessage.Message in let of_store msg = of_storage [ msg ] in
  let to_store msg =
    to_storage msg
    |> List.map (fun descr -> descr.segment)
    |> Bytes.concat Bytes.empty
  in
  (of_store, to_store)

let test_message =
  let buf = Bytes.create 16 in
  Bytes.set_int64_le buf 0 (Random.int64 Int64.max_int);
  Bytes.set_int64_le buf 8 (Random.int64 Int64.max_int);
  buf

let addr i = TCP ("127.0.0.1", 5000 + i)

let mgr i handler =
  create ~listen_address:(addr i) ~node_id:(Int64.of_int i) handler

let timeout t f =
  let p = f () >>= Lwt.return_ok in
  let t = Lwt_unix.sleep t >>= Lwt.return_error in
  Lwt.choose [p;t] >>= function
  | Ok v -> Lwt.return v
  | Error () -> Alcotest.fail "timed out"

let test_loop () =
  Logs.debug (fun m -> m "Starting test");
  let p, f = Lwt.task () in
  let m1 =
    mgr 1 (fun src msg ->
        Lwt.wakeup f (src, msg);
        Lwt.return_unit)
  in
  let m2 = mgr 2 (fun _ _ -> Lwt.return_unit) in
  add_outgoing m2 (Int64.of_int 1) (addr 1) (`Persistant (addr 1)) >>= fun () ->
  send ~semantics:`AtLeastOnce m2 (Int64.of_int 1) (of_store test_message)
  >>= function
  | Error exn -> Alcotest.fail (Fmt.str "Sending failed with %a" Fmt.exn exn)
  | Ok () ->
      p >>= fun (src, msg) ->
      Alcotest.(check string)
        "Received message"
        (msg |> to_store |> Bytes.to_string)
        (test_message |> Bytes.to_string);
      Alcotest.(check int64) "Received message" src (Int64.of_int 2);
      close m1 >>= fun () -> close m2

let test_wrapper f _ () = 
  timeout 1. f

let reporter =
  let open Core in
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Time.pp (Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let () =
  Logs.(set_level (Some Debug));
  Logs.set_reporter reporter;
  Lwt_main.run @@ test_loop () 
    (*
  let open Alcotest_lwt in
  Lwt_main.run
  @@ run "messaging layer"
       [ ("basic", [ test_case "Loopback" `Quick (test_wrapper test_loop) ]) ]
       *)
