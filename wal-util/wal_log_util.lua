#!/usr/bin/luajit

-- local jd = require("jit.dump")
-- jd.on('A', 'wal_util.dump')

package.path = package.path .. ";./lualib/?.lua"

local ldcache = {}
do
	local pipe = io.popen("/sbin/ldconfig -p")
	if not pipe then
		error("unable to run /sbin/ldconfig")
	end
	for line in pipe:lines() do
		local a, b = string.match(line, "^%s*([^ ]+)[^=]+=>%s+(.*)$")
		if a and not ldcache[a] then
			local soname = string.match(a, "^([^.]+%.so)")
			if soname then
				ldcache[a] = b
				ldcache[soname] = b
			end
		end
	end
end

local ffi = require"ffi"
local bit = require"bit"
local rshift = bit.rshift
local lshift = bit.lshift
local band = bit.band
local bor = bit.bor

local function load_library(libname)
	libname = libname .. ".so"
	if not ldcache[libname] then
		error("library " .. libname .. " is not present in ldconfig")
	end
	return ffi.load(ldcache[libname])
end

local alt_getopt = require"alt_getopt"
local inspect = require"inspect"
local z = load_library"libz"
local uuid = load_library"libuuid"
local mp = require("luajit-msgpack-pure")

ffi.cdef[[
int printf(const char *format, ...);

typedef unsigned char Byte;
typedef Byte Bytef;
typedef unsigned long uLong;
typedef unsigned int uInt;
uLong crc32 (uLong crc, const Bytef *buf, uInt len);

typedef unsigned char uuid_t[16];
void uuid_unparse(const uuid_t uu, char *out);
]]

local long_opts = {
	["dump-mode"]     = "d",
	["unpack-module"] = "m",
	["all-records"]   = "a",
	["help"]          = "h",
}

local uint32_t = ffi.typeof("uint32_t")
local uint32_ptr_t = ffi.typeof("uint32_t *")
local uint8_t = ffi.typeof("uint8_t")
local uint8_ptr_t = ffi.typeof("uint8_t *")

local WAL_MAGIC = 0xcf52754d
local REC_MAGIC = 0xc548d94d
local EOF_MAGIC = 0x2478c945

ffi.cdef[[
struct wal_rec_header {
	uint32_t header_checksum;
	uint64_t lsn;
	double tstamp;
	uint16_t size;
	uint32_t checksum;
} __attribute__((packed));

struct value_header {
	uint8_t version;
	uuid_t server_id;
	uint64_t value_id;
} __attribute__((packed));
]]
local wal_rec_header_t = ffi.typeof("struct wal_rec_header")
local wal_rec_header_ptr_t = ffi.typeof("struct wal_rec_header *")
local value_header_t = ffi.typeof("struct value_header")
local value_header_ptr_t = ffi.typeof("struct value_header *")

function print_help()
	print [[
	wal_log_util [options] <file1> [file2] [file3]

	WAL Log Dumper
	Util that will dump the contents of passed log files

	-d, --dump-module <unpack|text|hex>
		specify how the value contents should be dumped
	-m, --unpack-module <module.name>
		specify custom Lua module for unpacking the values
	-a, --all-records
		include all WAL record types, not only value ones
	-h, --help
		print this help message
	]]
end

function hex_dump(bytes)
	local cbytes = ffi.cast("char *", bytes)
	local x = 1
	for i = 0, #bytes - 1 do
		local fmt
		if 0 == x % 27 then
			fmt = "%02x\n"
		else
			fmt = "%02x "
		end
		ffi.C.printf(fmt, ffi.cast("int", cbytes[i]))
		x = x + 1
	end
end

local function eprintf(format, ...)
	io.stderr:write(string.format(format, ...))
end

local function exit_with_error(msg)
	io.stderr:write(msg)
	os.exit(1)
end

local function hdr_field_ptr(header, field)
	return ffi.cast("char *", header) +
			ffi.offsetof(wal_rec_header_t, field)
end

local function calc_header_checksum(header)
	local crc
	crc = z.crc32(0, nil, 0);
	crc = z.crc32(crc, hdr_field_ptr(header, "lsn"),
			ffi.sizeof("uint64_t"))
	crc = z.crc32(crc, hdr_field_ptr(header, "tstamp"),
			ffi.sizeof("double"))
	crc = z.crc32(crc, hdr_field_ptr(header, "size"),
			ffi.sizeof("uint16_t"))
	crc = z.crc32(crc, hdr_field_ptr(header, "checksum"),
			ffi.sizeof("uint32_t"))
	return crc
end


local function read_typed(file, type_, ptr_type)
	local buf = file:read(ffi.sizeof(type_))
	if not buf then
		return nil
	end
	return ffi.cast(ptr_type, buf)[0], buf
end

local function find_rec_magic(env)
	local magic, x = read_typed(env.file, uint32_t, uint32_ptr_t)
	if not magic then
		return false
	end
	if magic == EOF_MAGIC then
		env.eof_magic_found = true
		return false
	end
	local skipped = 0
	while REC_MAGIC ~= magic do
		local c, x = read_typed(env.file, uint8_t, uint8_ptr_t)
		skipped = skipped + 1
		if not c then
			return false
		end
		magic = tonumber(magic)
		local r = rshift(magic, 8)
		local l = lshift(band(c, 0xff), ffi.sizeof("uint32_t") * 8 - 8)
		magic = ffi.cast("uint32_t", bor(r, l));
	end
	if skipped > 0 then
		eprintf("skipped %d bytes to find record magic\n", skipped)
	end
	return true
end

local WAL_REC_TYPE_STATE = 1
local WAL_REC_TYPE_VALUE = 2
local WAL_REC_TYPE_PROMISE = 3

function wrap(str, limit, indent, indent1)
	indent = indent or ""
	indent1 = indent1 or indent
	limit = limit or 72
	local here = 1-#indent1
	return indent1..str:gsub("(%s+)()(%S+)()",
			function(sp, st, word, fi)
				if fi-here > limit then
					here = st - #indent
					return "\n"..indent..word
				end
			end)
end


local function dump_current_record(env)
	if not env.optarg["a"] then
		if "value" ~= env.current_rec.pxs_type then
			return
		end
	end
	io.write("-----BEGIN MERSENNE WAL RECORD-----\n")
	io.write("LSN: ")
	io.write(env.current_rec.lsn)
	io.write("\n")
	if "state" == env.current_rec.pxs_type then
		io.write("Type: state\n")
		io.write("Paxos info: highest_accepted=")
		io.write(env.current_rec.highest_accepted)
		io.write(", highest_finalized=")
		io.write(env.current_rec.highest_finalized)
		io.write("\n")
	elseif "value" == env.current_rec.pxs_type then
		io.write("Type: value\n")
		io.write("Paxos info: iid=")
		io.write(env.current_rec.iid)
		io.write(", b=")
		io.write(tostring(env.current_rec.b))
		io.write(", vb=")
		io.write(tostring(env.current_rec.vb))
		io.write("\n")
		if env.current_rec.header then
			io.write("Value header info: version=")
			io.write(env.current_rec.header.version)
			io.write(", server_id=")
			io.write(env.current_rec.header.server_id)
			io.write(", value_id=")
			io.write(env.current_rec.header.value_id)
			io.write("\n")
		end
		io.write("\n")
		if "unpack" == env.optarg["d"] then
			local data = env.unpack_fn(env.current_rec.payload)
			io.write(inspect(data))
		elseif "hex" == env.optarg["d"] then
			hex_dump(env.current_rec.payload)
		elseif "text" == env.optarg["d"] then
			local data = env.current_rec.payload
			io.write(data)
		end
		io.write("\n")
	elseif "promise" == env.current_rec.pxs_type then
		io.write("Type: promise\n")
		io.write("Paxos info: iid=")
		io.write(env.current_rec.iid)
		io.write(", b=")
		io.write(tostring(env.current_rec.b))
		io.write("\n")
	end
	io.write("-----END MERSENNE WAL RECORD-----\n\n")
end

local function decode_value_content(env, content)
	local buf = ffi.new("char[37]")
	local content = env.current_rec.content
	local ptr = ffi.cast("void *", ffi.cast("char *", content))
	assert(ptr)
	local value_header = ffi.cast(value_header_ptr_t, ptr)
	uuid.uuid_unparse(value_header.server_id, buf)
	buf = ffi.string(buf)
	env.current_rec.header = {}
	env.current_rec.header.version = tonumber(value_header.version)
	env.current_rec.header.server_id = buf
	env.current_rec.header.value_id = tonumber(value_header.value_id)
	env.current_rec.payload = content:sub(ffi.sizeof(value_header_t))
end

local function process_value(env, val)
	local off, decoded

	off, decoded = mp.unpack(val)

	if WAL_REC_TYPE_STATE == decoded[1] then
		env.current_rec.pxs_type = "state"
		env.current_rec.highest_accepted = decoded[2]
		env.current_rec.highest_finalized = decoded[3]
	elseif WAL_REC_TYPE_VALUE == decoded[1] then
		env.current_rec.pxs_type = "value"
		env.current_rec.iid = decoded[2]
		env.current_rec.b = decoded[3]
		env.current_rec.vb = decoded[4]
		env.current_rec.content = decoded[5]
		decode_value_content(env)
	elseif WAL_REC_TYPE_PROMISE == decoded[1] then
		if not env.optarg["a"] then
			return
		end
		env.current_rec.pxs_type = "promise"
		env.current_rec.iid = decoded[2]
		env.current_rec.b = decoded[3]
	else
		exit_with_error(string.format("unexpected wal record: %s\n",
				inspect(decoded)))
	end
end

local function file_dumper(env)
	env.eof_magic_found = false
	local magic, x = read_typed(env.file, uint32_t, uint32_ptr_t)
	if WAL_MAGIC ~= magic then
		exit_with_error("WAL magic mismatch")
	end
	while true do
		if not find_rec_magic(env) then
			break
		end
		local header, x = read_typed(env.file, wal_rec_header_t,
				wal_rec_header_ptr_t)
		local crc = calc_header_checksum(header)
		if header.header_checksum ~= crc then
			exit_with_error("header checksum mismatch")
		end
		env.current_rec = {
			lsn = tonumber(header.lsn)
		}
		local val = env.file:read(tonumber(header.size))
		if not val then
			eprintf("unexpected end of file while reading value\n")
			break
		end
		if #val < header.size then
			eprintf("value size is less than expected\n")
			break
		end
		local crc = z.crc32(0, nil, 0)
		crc = z.crc32(crc, val, #val)
		if ffi.cast("uint32_t", crc) == header.checksum then
			process_value(env, val)
		else
			eprintf("value checksum mismatch %d != %d\n",
					tonumber(crc),
					tonumber(header.checksum))
		end
		dump_current_record(env)
	end
	if not env.eof_magic_found then
		magic, x = read_typed(env.file, uint32_t, uint32_ptr_t)
		if not magic then
			eprintf("unable to find eof magic\n")
		elseif EOF_MAGIC ~= magic then
			eprintf("eof magic is corrupted\n")
		end
	end
end

local valid_dump_options = {
	hex    = true,
	text   = true,
	unpack = true,
}

local function main(argv)
	local ret
	local optarg
	local optind

	local opt_line = "hd:m:a"
	optarg, optind = alt_getopt.get_opts(argv, opt_line, long_opts)
	if optarg["h"] then
		print_help()
		os.exit(0)
	end
	if not optarg["d"] then
		optarg["d"] = "text"
	elseif not valid_dump_options[optarg["d"]] then
		exit_with_error("invalid dump flavour specified")
	end

	local env = {
		optarg = optarg,
	}

	if optarg["m"] then
		env.unpack_fn = require(optarg["m"])
	else
		env.unpack_fn = function(what)
			local off, decoded
			off, decoded = mp.unpack(val)
			return decoded
		end
	end

	if 1 == optind - #argv then
		print_help()
		os.exit(0)
	end
	for i=optind,#argv do
		if "-" == argv[i] then
			file = io.stdin
			env.file = file
			file_dumper(env)
		else
			local file = io.open(argv[i], "r")
			if not file then
				exit_with_error("Unable to open " .. argv[i])
			end
			env.file = file
			file_dumper(env)
		end
	end
end

main(arg)
