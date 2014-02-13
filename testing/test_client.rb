#!/usr/bin/ruby
require 'socket'
require 'msgpack'
require 'uuidtools'
require 'optparse'

options = {}

optparse = OptionParser.new do |opts|
	# Set a banner, displayed at the top
	# of the help screen.
	opts.banner = "Usage: test_client.rb [options]"

	# Define the options, and what they do
	options[:unix] = nil
	opts.on('-u', '--unix PATH', 'Connect over unix socket' ) do |sock|
		options[:unix] = sock
		p sock
	end

	options[:tcp] = nil
	opts.on('-t', '--tcp ENDPOINT', 'Connect over TCP' ) do |endpoint|
		options[:tcp] = endpoint
	end

	# This displays the help screen, all programs are
	# assumed to have this option.
	opts.on( '-h', '--help', 'Display this screen' ) do
		puts opts
		exit
	end
end

optparse.parse!

socket = nil
if options[:unix] then
	socket = UNIXSocket.new(options[:unix])
elsif options[:tcp] then
	if options[:tcp] =~ /\A([^:]+):([0-9]+)\z/ then
		address, port = $1, $2
	else
		puts("Invalid endpoint")
		exit
	end

	socket = TCPSocket.open(address, port)
	socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
else
	puts("No endpoint specified!")
	exit
end

uuid = UUIDTools::UUID.random_create
msg = [0, uuid.raw, "My test paxos value " + uuid + "!"]
socket.write(msg.to_msgpack)

u = MessagePack::Unpacker.new(socket)
u.each do |obj|
	p obj
end
