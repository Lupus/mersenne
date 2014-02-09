#!/usr/bin/ruby
require 'socket'
require 'msgpack'
require 'uuidtools'

socket = UNIXSocket.new("socks/0")
uuid = UUIDTools::UUID.random_create
msg = [0, uuid.raw, "My test paxos value!"]
socket.write(msg.to_msgpack)

u = MessagePack::Unpacker.new(socket)
u.each do |obj|
	p obj
end
