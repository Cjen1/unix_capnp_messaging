open Library
open Conn_manager
open Lwt.Infix

let of_store, to_store =
  let open Capnp.BytesMessage.Message in
  let of_store msg = of_storage [ msg ] in
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

let time_it f = 
  let start = Unix.gettimeofday () in
  f () >>= fun () ->
  Unix.gettimeofday () -. start |> Lwt.return

let throughput n () = 
  let ops = List.init n (fun _ -> test_message |> of_store) in
  let m1 = mgr 1 (fun t src msg -> send ~semantics:`AtLeastOnce t src msg) in
  let m2 = mgr 2 (fun _ _ _ -> Lwt.return_ok ()) in
  add_outgoing m2 (Int64.of_int 1) (addr 1) (`Persistant (addr 1)) >>= fun () ->
  let stream = Lwt_stream.of_list ops in
  let max_concurrency = 100 in
  let test () = 
    Lwt_stream.iter_n ~max_concurrency (fun msg -> 
        send ~semantics:`AtLeastOnce m2 (Int64.of_int 1) msg
        >>= function 
        | Error exn -> Fmt.failwith "Failed during throughput %a" Fmt.exn exn
        | Ok () -> Lwt.return_unit
      ) stream
  in 
  time_it test >>= fun time -> 
  Lwt.join [close m1; close m2]
  >>= fun () -> 
  Fmt.str "Took %f to do %d operations: %f ops/s" time n Core.Float.((of_int n) / time) |> Lwt.return

let reporter =
  let open Core in
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let src = Logs.Src.name src in
    msgf @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Time.pp (Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  { Logs.report }

let () =
  Logs.(set_level (Some Info));
  Logs.set_reporter reporter;
  let res = Lwt_main.run @@ throughput 100000 () in
  print_endline res
