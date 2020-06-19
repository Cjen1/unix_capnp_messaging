open Lwt.Infix

let ( >>>= ) a b = Lwt_result.bind a b

let src = Logs.Src.create "ConnMgr" ~doc:"Connection manager"

module Log = (val Logs.src_log src : Logs.LOG)

type address = Unix of string | TCP of (string * int)

let addr_of_string s =
  match String.split_on_char ':' s with
  | [ addr ] -> Ok (Unix addr)
  | [ addr; port ] -> (
      match int_of_string_opt port with
      | Some port -> Ok (TCP (addr, port))
      | None -> Error (`Msg (Fmt.str "Port (%s) is not a valid int" port)) )
  | _ -> Error (`Msg (Fmt.str "Expected address of the form ip:port, not %s" s))

module Utils = struct
  let pp_addr f = function
    | Unix s -> Fmt.pf f "unix:%s" s
    | TCP (s, p) -> Fmt.pf f "tcp://%s:%d" s p

  let addr_of_host host =
    match Unix.gethostbyname host with
    | exception Not_found -> Fmt.failwith "Unknown host %S" host
    | addr ->
        if Array.length addr.Unix.h_addr_list = 0 then
          Fmt.failwith "No addresses found for host name %S" host
        else addr.Unix.h_addr_list.(0)

  let connect_socket = function
    | Unix path ->
        Log.info (fun f -> f "Connecting to %S..." path);
        let socket =
          Unix.(socket PF_UNIX SOCK_STREAM 0) |> Lwt_unix.of_unix_file_descr
        in
        Lwt_unix.connect socket (Unix.ADDR_UNIX path) >|= fun () -> socket
    | TCP (host, port) ->
        Log.info (fun f -> f "Connecting to %s:%d..." host port);
        let socket = Unix.(socket PF_INET SOCK_STREAM 0) in
        Unix.setsockopt socket Unix.SO_KEEPALIVE true;
        let socket = Lwt_unix.of_unix_file_descr socket in
        Lwt_unix.connect socket (Unix.ADDR_INET (addr_of_host host, port))
        >|= fun () -> socket

  let bind_socket = function
    | Unix path ->
        Log.info (fun f -> f "Binding to %S..." path);
        ( match Unix.lstat path with
        | { Unix.st_kind = Unix.S_SOCK; _ } -> Unix.unlink path
        | _ -> ()
        | exception Unix.Unix_error (Unix.ENOENT, _, _) -> () );
        let socket = Unix.(socket PF_UNIX SOCK_STREAM 0) in
        let socket = socket |> Lwt_unix.of_unix_file_descr in
        Lwt_unix.bind socket (Unix.ADDR_UNIX path) >|= fun () -> socket
    | TCP (host, port) ->
        Log.info (fun f -> f "Binding to %s:%d..." host port);
        let socket = Unix.(socket PF_INET SOCK_STREAM 0) in
        Unix.setsockopt socket Unix.SO_REUSEADDR true;
        let socket = Lwt_unix.of_unix_file_descr socket in
        Lwt_unix.bind socket (Unix.ADDR_INET (addr_of_host host, port))
        >|= fun () -> socket
end

type node_id = int64

type conn_kind = [ `Persistant of address | `Ephemeral ]

type conn_descr = {
  node_id : node_id;
  in_socket : Sockets.Incomming.t;
  out_socket : Sockets.Outgoing.t;
  switch : Lwt_switch.t;
  mutable kind : conn_kind;
}

type recv_handler =
  t ->
  node_id ->
  Capnp.MessageSig.ro Capnp.BytesMessage.Message.t ->
  (unit, exn) Lwt_result.t

and t = {
  handler : recv_handler;
  mutable active_conns : (node_id * conn_descr) list;
  mutable persist_ids : (node_id * conn_kind) list;
  switch : Lwt_switch.t;
  switch_off_prom_fulfill : unit Lwt.t * unit Lwt.u;
  retry_connection_timeout : float;
  node_id : node_id;
}

exception EOF

let rec read_exactly fd buf offset len =
  Lwt.catch
    (fun () -> Lwt_unix.read fd buf offset len)
    (fun exn -> Lwt.fail exn)
  >>= function
  | 0 -> Lwt.fail EOF
  | read when read = len -> Lwt.return ()
  | read -> (read_exactly [@tailcall]) fd buf (offset + read) (len - offset)

let get_id_from_sock sock =
  let buf = Bytes.create 8 in
  Lwt_result.catch (read_exactly sock buf 0 8) >>>= fun () ->
  Bytes.get_int64_be buf 0 |> Lwt.return_ok

let recv_handler_wrapper t conn_descr () =
  let rec recv_loop () =
    Sockets.Incomming.recv conn_descr.in_socket >>>= fun msg ->
    Log.debug (fun m -> m "%a: Handling msg" Fmt.int64 t.node_id);
    Lwt.catch (fun () -> t.handler t conn_descr.node_id msg) Lwt.return_error
    >>>= fun () -> (recv_loop [@tailcall]) ()
  in
  recv_loop () >|= function
  | Ok () -> Fmt.failwith "Recv loop exited unexpectedly"
  | Error Sockets.Closed ->
      Log.debug (fun m -> m "%a: Socket closed" Fmt.int64 t.node_id)
  | Error exn ->
      Log.err (fun m -> m "Recv loop exited with exn: %a" Fmt.exn exn)

let rec add_conn t socket id kind =
  let switch = Lwt_switch.create () in
  Lwt.join
    [
      Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
          t.active_conns <- List.remove_assoc id t.active_conns;
          Lwt.return_unit);
      Lwt_switch.add_hook_or_exec (Some t.switch) (fun () ->
          Lwt_switch.turn_off switch >>= fun () -> Lwt_unix.close socket);
    ]
  >>= fun () ->
  ( match kind with
  | `Persistant addr ->
      Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
          (add_outgoing [@tailcall]) t id addr kind)
  | `Ephemeral -> Lwt.return_unit )
  >>= fun () ->
  Lwt.both
    (Sockets.Incomming.create ~switch socket)
    (Sockets.Outgoing.create ~switch socket)
  >|= fun (in_socket, out_socket) ->
  let conn_descr = { node_id = id; in_socket; out_socket; switch; kind } in
  t.active_conns <- (id, conn_descr) :: t.active_conns;
  Log.debug (fun m ->
      m "%a: Added %a to active connections" Fmt.int64 t.node_id Fmt.int64 id);
  Lwt.async (recv_handler_wrapper t conn_descr);
  match kind with
  | `Persistant _ as kind when List.mem_assoc id t.persist_ids ->
      t.persist_ids <- (id, kind) :: t.persist_ids
  | _ -> ()

and add_outgoing t id addr kind =
  let rec retry_loop () =
    let main =
      match Lwt_switch.is_on t.switch with
      | true ->
          Log.info (fun m -> m "Attempting to connect to %a" Utils.pp_addr addr);
          let connect () = Utils.connect_socket addr >>= Lwt.return_ok in
          Lwt.catch connect Lwt.return_error >>>= fun socket ->
          let buf = Bytes.create 8 in
          Bytes.set_int64_be buf 0 t.node_id;
          let buf = Lwt_bytes.of_bytes buf in
          Sockets.send (Lwt_bytes.write socket) buf 0 8 >>>= fun () ->
          Lwt.async (fun () -> (add_conn [@tailcall]) t socket id kind);
          Lwt.return_ok ()
      | false -> Lwt.return_ok ()
    in
    main >>= function
    | _ when not (Lwt_switch.is_on t.switch) ->
        Log.debug (fun m ->
            m "%a: Retry loop stopped because manager is closed" Fmt.int64
              t.node_id);
        Lwt.return_unit
    | Ok () -> Lwt.return_unit
    | Error exn -> (
        Log.err (fun m -> m "Failed while connecting: %a" Fmt.exn exn);
        match kind with
        | `Persistant _ ->
            Lwt_unix.sleep t.retry_connection_timeout >>= fun () ->
            retry_loop ()
        | `Ephemeral -> Lwt.return_unit )
  in
  retry_loop ()

let incomming_conn_handler t client_sock =
  get_id_from_sock client_sock >>>= fun id ->
  Log.debug (fun m ->
      m "%a: Completed handshake with %a" Fmt.int64 t.node_id Fmt.int64 id);
  match (List.assoc_opt id t.persist_ids, List.assoc_opt id t.active_conns) with
  | Some _kind, Some _conn when t.node_id > id ->
      Log.debug (fun m ->
          m "Preexisting conn from %d and we should initiate" (Int64.to_int id));
      Lwt.catch
        (fun () -> Lwt_unix.close client_sock >>= fun () -> Lwt.return_ok ())
        Lwt.return_error
  | Some kind, Some conn ->
      Log.debug (fun m ->
          m
            "Preexisting conn from %a and they initiate, thus stop existing, \
             add new"
            Fmt.int64 id);
      Lwt.async (fun () -> Lwt_switch.turn_off conn.switch);
      t.active_conns <- List.remove_assoc id t.active_conns;
      add_conn t client_sock id kind >>= Lwt.return_ok
  | None, Some conn ->
      Log.debug (fun m ->
          m
            "Preexisting conn from %a and they initiate, thus stop existing, \
             add new"
            Fmt.int64 id);
      Lwt.async (fun () -> Lwt_switch.turn_off conn.switch);
      t.active_conns <- List.remove_assoc id t.active_conns;
      add_conn t client_sock id `Ephemeral >>= Lwt.return_ok
  | Some _kind, None ->
      Log.debug (fun m ->
          m
            "Persistant connection but not yet established, will be \
             established later");
      Lwt.return_ok ()
  | None, None ->
      Log.debug (fun m ->
          m "%a: Got Ephemeral connection from %a" Fmt.int64 t.node_id Fmt.int64
            id);
      add_conn t client_sock id `Ephemeral >>= Lwt.return_ok

let accept_incomming_loop t addr () =
  let main () =
    let create_sock () =
      Utils.bind_socket addr >>= fun fd ->
      Lwt_unix.listen fd 128;
      Lwt.return_ok fd
    in
    Lwt.catch create_sock (fun exn -> Lwt.return_error (`Exn (`Create, exn)))
    >>>= fun bound_socket ->
    Lwt_switch.add_hook_or_exec (Some t.switch) (fun () ->
        Lwt_unix.close bound_socket)
    >>= fun () ->
    let rec accept_loop () =
      let accept () =
        let off =
          fst t.switch_off_prom_fulfill >>= fun () -> Lwt.return_error `Closed
        in
        let acc = Lwt_unix.accept bound_socket >>= Lwt.return_ok in
        Lwt.choose [ off; acc ]
      in
      Lwt.catch accept (fun exn -> Lwt.return_error (`Exn (`Accept, exn)))
      >>>= fun (client_sock, _) ->
      let handler () =
        incomming_conn_handler t client_sock >>= function
        | Ok () -> Lwt.return_unit
        | Error exn ->
            Log.err (fun m ->
                m "Incomming connection handler threw error %a" Fmt.exn exn);
            Lwt.return_unit
      in
      Lwt.async handler;
      (accept_loop [@tailcall]) ()
    in
    accept_loop ()
  in
  main () >>= function
  | Ok () -> Fmt.failwith "accept loop ended unexpectedly"
  | Error `Closed ->
      Log.debug (fun m -> m "%a: Accept loop closed" Fmt.int64 t.node_id)
      |> Lwt.return
  | Error (`Exn (`Create, exn)) ->
      Log.err (fun m ->
          m "Accept loop failed during socket creation with %a" Fmt.exn exn)
      |> Lwt.return
  | Error (`Exn (`Accept, exn)) ->
      Log.err (fun m ->
          m "Accept loop failed while accepting a connection with %a" Fmt.exn
            exn)
      |> Lwt.return

let create ?(retry_connection_timeout = 10.) ~listen_address ~node_id handler =
  let switch = Lwt_switch.create () in
  let switch_off_prom_fulfill = Lwt.task () in
  let t =
    {
      active_conns = [];
      persist_ids = [];
      retry_connection_timeout;
      handler;
      switch;
      switch_off_prom_fulfill;
      node_id;
    }
  in
  Lwt.async (accept_incomming_loop t listen_address);
  t

let close t =
  Lwt.wakeup (snd t.switch_off_prom_fulfill) ();
  Lwt_main.yield () >>= fun () -> Lwt_switch.turn_off t.switch

let send ?(semantics = `AtMostOnce) t id msg =
  let retry_loop handle_err f =
    let rec loop () =
      f () >>= function
      | Ok v -> Lwt.return v
      | Error Sockets.Closed ->
        Log.debug (fun m -> m "%a: Failed to send on closed socket" Fmt.int64 t.node_id);
        Lwt.return ()
      | Error exn ->
          handle_err exn;
          (loop [@tailcall]) ()
    in
    loop ()
  in
  let send () =
    match List.assoc_opt id t.active_conns with
    | None -> Lwt.return_error Not_found
    | Some conn_descr -> Sockets.Outgoing.send conn_descr.out_socket msg
  in
  match semantics with
  | `AtMostOnce -> send ()
  | `AtLeastOnce ->
      let err_handler exn =
        Log.err (fun m -> m "%a: Failed to send message with %a" Fmt.int64 t.node_id Fmt.exn exn)
      in
      retry_loop err_handler send >>= fun res -> Lwt.return_ok res
