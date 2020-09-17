open Lwt.Infix
module U = Utils
module FS = Capnp.Codecs.FramedStream

let ( >>>= ) = Lwt_result.bind

let src = Logs.Src.create "Msg_layer" ~doc:"messaging layer"

module Log = (val Logs.src_log src : Logs.LOG)

let compression = `None

exception Closed

let turn_off_switch switch =
  if Lwt_switch.is_on switch then
    let p = Lwt_switch.turn_off switch in
    Lwt.on_failure p !Lwt.async_exception_hook

type 'a msg = 'a Capnp.BytesMessage.Message.t

let rec write_all fd buf offset len =
  let%lwt written = Lwt_unix.write fd buf offset len in
  if written < len then
    (write_all [@tailcall]) fd buf (offset + written) (len - written)
  else Lwt.return_unit

module Outgoing = struct
  type t = {
    fd : Lwt_unix.file_descr;
    queue_reader : (Capnp.MessageSig.ro msg * unit Lwt.u) Lwt_stream.t;
    queue_push : Capnp.MessageSig.ro msg * unit Lwt.u -> unit;
    switch : Lwt_switch.t;
  }

  let write_thread t =
    let iter (msg, fulfiller) =
      let buf =
        Capnp.Codecs.serialize ~compression:`None msg |> Bytes.unsafe_of_string
      in
      let%lwt () = write_all t.fd buf 0 (Bytes.length buf) in
      Lwt.wakeup_later fulfiller ();
      Lwt.return_unit
    in
    let p = Lwt_stream.iter_s iter t.queue_reader in
    Lwt.on_failure p (fun exn ->
        Log.err (fun m -> m "Write thread failed with %a" Fmt.exn exn);
        if Lwt_switch.is_on t.switch then turn_off_switch t.switch)

  let send t msg =
    let msg = Capnp.BytesMessage.Message.readonly msg in
    let task, u = Lwt.task () in
    t.queue_push (msg, u);
    task

  let create ?switch fd =
    let switch =
      match switch with Some switch -> switch | None -> Lwt_switch.create ()
    in
    let stream, queue_push = Lwt_stream.create () in
    let%lwt () =
      Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
          queue_push None;
          Lwt.return_unit)
    in
    let t =
      {
        fd;
        queue_reader = stream;
        queue_push = (fun v -> queue_push (Some v));
        switch;
      }
    in
    write_thread t;
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
    | _ when not (Lwt_switch.is_on t.switch) -> Lwt.fail Closed
    | Ok msg -> Lwt.return (Capnp.BytesMessage.Message.readonly msg)
    | Error Capnp.Codecs.FramingError.Unsupported ->
        failwith "Unsupported Cap'n'Proto frame received"
    | Error Capnp.Codecs.FramingError.Incomplete ->
        Log.debug (fun f -> f "Incomplete; waiting for more data...");
        Lwt_condition.wait t.recv_cond >>= fun () -> (recv [@tailcall]) t

  let recv_thread ?(buf_size = 256) t =
    let handler recv_buffer len =
      if len > 0 then (
        let buf = Bytes.sub recv_buffer 0 len in
        Capnp.Codecs.FramedStream.add_fragment t.decoder
          (Bytes.unsafe_to_string buf);
        Lwt_condition.signal t.recv_cond ();
        if Capnp.Codecs.FramedStream.bytes_available t.decoder > 2 * 1024 * 1024
        then
          Log.err (fun m ->
              m "Queuing in the recv socket of %d"
                (Capnp.Codecs.FramedStream.bytes_available t.decoder));
        (* don't need to wipe buffer since will be wiped by Bytes.sub *)
        Ok () )
      else Error `EOF
    in
    let recv_buffer = Bytes.create buf_size in
    let rec loop () =
      let%lwt read = Lwt_unix.read t.fd recv_buffer 0 buf_size in
      match Lwt_switch.is_on t.switch with
      | false ->
          Log.debug (fun m -> m "Connection closed");
          Lwt.fail Closed
      | true -> (
          match handler recv_buffer read with
          | Ok () ->
              let%lwt () =
                if FS.bytes_available t.decoder > 1024 * 1024 then Lwt.pause ()
                else Lwt.return_unit
              in
              (loop [@tailcall]) ()
          | Error `EOF ->
              Log.debug (fun m -> m "Connection closed with EOF");
              turn_off_switch t.switch;
              Lwt.return_unit )
    in
    Lwt.on_failure (loop ()) (function
      | _ when Lwt_switch.is_on t.switch -> turn_off_switch t.switch
      | _ -> ())

  let create ?switch fd =
    let decoder = Capnp.Codecs.FramedStream.empty compression in
    let switch =
      match switch with Some switch -> switch | None -> Lwt_switch.create ()
    in
    let recv_cond = Lwt_condition.create () in
    let t = { fd; decoder; switch; recv_cond } in
    recv_thread t;
    Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
        Lwt_condition.broadcast recv_cond ();
        Lwt.return_unit)
    >>= fun () -> Lwt.return t
end
