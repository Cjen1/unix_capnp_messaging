open Lwt.Infix

let ( >>>= ) = Lwt_result.bind

let src = Logs.Src.create "Msg_layer" ~doc:"messaging layer"

module Log = (val Logs.src_log src : Logs.LOG)

let compression = `None

exception Closed

let rec send write buf offset len =
  Lwt.catch
    (fun () -> write buf offset len >>= Lwt.return_ok)
    Lwt.return_error
  >>= function
  | Ok len' ->
      Log.debug (fun m -> m "Wrote %d to fd" len');
      if len' < len then (send[@tailcall]) write buf (offset + len') (len - len')
      else Lwt.return_ok ()
  | Error e ->
      Log.err (fun m -> m "Failed to send %a" Fmt.exn e);
      Lwt.return_error e

module Outgoing = struct
  type t = {
    fd : Lwt_unix.file_descr;
    mutable latest_xmit : (unit, exn) Lwt_result.t;
    switch_off_promise : (unit, exn) Lwt_result.t;
    switch : Lwt_switch.t;
  }

  let prev_send_wrapper t f =
    let failure e =
      Log.err (fun m -> m "Failed to send with %a" Fmt.exn e);
      Lwt_switch.turn_off t.switch >>= fun () -> Lwt.return_error Closed
    in
    t.latest_xmit >>= function
    | Ok () -> (
      Log.debug (fun m -> m "trying to catch f");
      Lwt.catch f Lwt.return_error >>= function 
        Ok v -> Lwt.return_ok v 
      | Error e -> failure e
    )
    | Error e -> 
      Log.debug (fun m -> m "Prev failed");
      failure e

  let send t msg =
    if Lwt_switch.is_on t.switch then (
      Log.debug (fun m -> m "Trying to send");
      let write p str len = 
        p >>>= fun () ->
        send (Lwt_unix.write t.fd) (Bytes.unsafe_of_string str) 0 len
      in 
      let main () =
        let write_p () = Capnp.Codecs.serialize_fold_copyless msg ~compression:`None ~init:(Lwt.return_ok ()) ~f:write in
        let msg_p = prev_send_wrapper t write_p in
        let p = Lwt.choose [ msg_p; t.switch_off_promise ] in
        t.latest_xmit <- p;
        p
      in
      Lwt.catch main Lwt.return_error )
    else Lwt.return_error Closed

  let create ?switch fd =
    let switch =
      match switch with Some switch -> switch | None -> Lwt_switch.create ()
    in
    let off_p, off_f = Lwt.task () in
    Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
        Lwt.wakeup off_f (Error Closed);
        Lwt.return_unit)
    >>= fun () ->
    let latest_xmit = Lwt.return_ok () in
    let t = { fd; latest_xmit; switch_off_promise = off_p; switch } in
    Lwt.return t
end

module Incomming = struct
  type t = {
    fd : Lwt_unix.file_descr;
    decoder : Capnp.Codecs.FramedStream.t;
    switch : Lwt_switch.t;
    recv_cond : unit Lwt_condition.t;
  }

  let rec recv t =
    match Capnp.Codecs.FramedStream.get_next_frame t.decoder with
    | _ when not (Lwt_switch.is_on t.switch) -> Lwt.return_error Closed
    | Ok msg -> Lwt.return (Ok (Capnp.BytesMessage.Message.readonly msg))
    | Error Capnp.Codecs.FramingError.Unsupported ->
        failwith "Unsupported Cap'n'Proto frame received"
    | Error Capnp.Codecs.FramingError.Incomplete ->
        Log.debug (fun f -> f "Incomplete; waiting for more data...");
        Lwt_condition.wait t.recv_cond >>= fun () -> (recv[@tailcall]) t

  let recv_thread ?(buf_size = 256) t =
    let handler recv_buffer len =
      if len > 0 then (
        let buf = Bytes.sub recv_buffer 0 len in
        Capnp.Codecs.FramedStream.add_fragment t.decoder
          (Bytes.unsafe_to_string buf);
        Lwt_condition.broadcast t.recv_cond ();
        (* don't need to wipe buffer since will be wiped by Bytes.sub *)
        Ok () )
      else Error `EOF
    in
    let recv_buffer = Bytes.create buf_size in
    let rec loop () =
      Lwt.catch
        (fun () -> Lwt_unix.read t.fd recv_buffer 0 buf_size >>= Lwt.return_ok)
        Lwt.return_error
      >>= function
      | _ when not (Lwt_switch.is_on t.switch) ->
          Log.debug (fun m -> m "Connection closed");
          Lwt.return_unit
      | Error e ->
          Log.err (fun m ->
              m "Failed to receive with %a, closing conn" Fmt.exn e);
          Lwt_switch.turn_off t.switch
      | Ok len -> (
          Log.debug (fun m -> m "Recieved data from socket");
          match handler recv_buffer len with
          | Ok () -> (loop[@tailcall]) ()
          | Error `EOF ->
              Log.debug (fun m -> m "Connection closed with EOF");
              Lwt_switch.(
                if is_on t.switch then Lwt_switch.turn_off t.switch
                else Lwt.return_unit) )
    in
    loop ()

  let create ?switch fd =
    let decoder = Capnp.Codecs.FramedStream.empty compression in
    let switch =
      match switch with Some switch -> switch | None -> Lwt_switch.create ()
    in
    let recv_cond = Lwt_condition.create () in
    let t = { fd; decoder; switch; recv_cond } in
    Lwt.async (fun () -> recv_thread t);
    Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
        Lwt_condition.broadcast recv_cond ();
        Lwt.return_unit)
    >>= fun () -> Lwt.return t
end
