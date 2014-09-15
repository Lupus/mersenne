/* vim: set syntax=c: */
#include <assert.h>
#include <stdio.h>

#include <msgpack-proto-ragel/tokens.h>
#include <mersenne/proto.h>

#define elog(format, ...) do { \
			asprintf(error_msg, format, ##__VA_ARGS__); \
			error = 1; \
		} while (0)
#define mp_uint(fpc) (fpc->obj->via.u64)
#define mp_raw(fpc) (fpc->obj->via.raw)
#define mp_array(fpc) (fpc->obj->via.array)
#define mp_raw_cpy(fpc, x) memcpy(x, mp_raw(fpc).ptr, mp_raw(fpc).size)
#define mp_raw_dup(fpc, x, y) do {                                    \
			x = malloc(mp_raw(fpc).size);                 \
			memcpy(x, mp_raw(fpc).ptr, mp_raw(fpc).size); \
			y = mp_raw(fpc).size;                         \
		} while (0)
#define mp_raw_ref(fpc, x, y) do {                   \
			x = (char *)mp_raw(fpc).ptr; \
			y = mp_raw(fpc).size;        \
		} while (0)
#define mp_raw_opt(fpc, x, y) do {                     \
			if (alloc_mem) {               \
				mp_raw_dup(fpc, x, y); \
			} else {                       \
				mp_raw_ref(fpc, x, y); \
			}                              \
		} while (0)

%%{
	machine cli_msg_parser;
	alphtype unsigned int;
	getkey fpc->id;

	include mppr_common_decls "msgpack-proto-ragel/mppr_common.rl";

	action validate_uuid_length {
		if (mp_raw(fpc).size != sizeof(uuid_t)) {
			elog("invalid uuid length: %d", mp_raw(fpc).size);
			fbreak;
		}
	}

	action validate_ip_string_length {
		if (mp_raw(fpc).size >= ME_CMT_IP_SIZE) {
			elog("invalid ip string length: %d", mp_raw(fpc).size);
			fbreak;
		}
	}

	action validate_pxs_value_length {
		if (0 >= mp_raw(fpc).size) {
			elog("empty paxos value received");
			fbreak;
		}
	}

	m_type = MOBJ_POS_INT;
	iid = MOBJ_POS_INT;
	uuid = MOBJ_RAW @validate_uuid_length;
	ip_string = MOBJ_RAW @validate_ip_string_length;
	pxs_value = MOBJ_RAW @validate_pxs_value_length;
	arr_start = MOBJ_ARRAY;
	arr_end = MOBJ_END;

	m_new_value :=
		uuid @{
			mp_raw_cpy(fpc, r->new_value.request_id);
		} .
		pxs_value @{
			mp_raw_opt(fpc, r->new_value.buf, r->new_value.size);
		}
		@{ fret; };

	m_arrived_value :=
		pxs_value @{
			mp_raw_opt(fpc, r->arrived_value.buf,
					r->arrived_value.size);
		} .
		iid @{
			r->arrived_value.iid = mp_uint(fpc);
		}
		@{ fret; };

	m_redirect :=
		ip_string @{
			memcpy(r->redirect.ip, mp_raw(fpc).ptr,
					mp_raw(fpc).size);
			r->redirect.ip[mp_raw(fpc).size] = '\0';
		}
		@{ fret; };

	m_server_hello :=
		arr_start @{
			r->server_hello.peers = calloc(mp_array(fpc).size,
					sizeof(void *));
			r->server_hello.count = mp_array(fpc).size;
			i = 0;
		} .
		ip_string+ @{
			assert(i < r->server_hello.count);
			r->server_hello.peers[i] = malloc(mp_raw(fpc).size + 1);
			memcpy(r->server_hello.peers[i], mp_raw(fpc).ptr,
					mp_raw(fpc).size);
			r->server_hello.peers[i][mp_raw(fpc).size] = '\0';
			i++;
		} .
		arr_end
		@{ fret; };

	m_client_hello :=
		iid @{
			r->client_hello.starting_iid = mp_uint(fpc);
		}
		@{ fret; };

	m_error :=
		MOBJ_POS_INT @{
			r->error.code = mp_uint(fpc);
		}
		@{ fret; };


	action dispatch_m_type {
		r->m_type = mp_uint(fpc);
		switch (mp_uint(fpc)) {
		case ME_CMT_NEW_VALUE:
			fcall m_new_value;
			break;
		case ME_CMT_ARRIVED_VALUE:
			fcall m_arrived_value;
			break;
		case ME_CMT_REDIRECT:
			fcall m_redirect;
			break;
		case ME_CMT_SERVER_HELLO:
			fcall m_server_hello;
			break;
		case ME_CMT_CLIENT_HELLO:
			fcall m_client_hello;
			break;
		case ME_CMT_ERROR:
			fcall m_error;
			break;
		default:
			elog("invalid message type: %ld", mp_uint(fpc));
			break;
		}
		if (error)
			fbreak;
	}

	main := arr_start . m_type @dispatch_m_type . arr_end;
}%%

%% write data;

int me_cli_msg_unpack(msgpack_object *obj, union me_cli_any *r,
		int alloc_mem, char **error_msg)
{
	/* Ragel variables */
	int cs = 0;
	int top;
	int stack[16];
	struct mppr_parser_token *p, *pe;

	/* Other variables */
	struct mppr_tokens *tokens;
	int error = 0;
	unsigned int i = 0;
	%% write init;

	tokens = mppr_tokenize_object(obj);
	mppr_set_ragel_p_pe(tokens, &p, &pe);
	%% write exec;
	mppr_destroy_tokens(tokens);

	assert(p <= pe && "Buffer overflow after parsing.");

	if (cs == cli_msg_parser_error) {
		elog("unable to parse messagepack message structure");
		return -1;
	}
	if (error)
		return -1;
	return 0;
}

#undef elog
#undef mp_uint
#undef mp_raw
#undef mp_array
#undef mp_raw_cpy
#undef mp_raw_dup
#undef mp_raw_ref
#undef mp_raw_opt

int me_cli_msg_pack(msgpack_packer *pk, union me_cli_any *u)
{
	struct me_cli_new_value *new_value;
	struct me_cli_arrived_value *arrived_value;
	struct me_cli_redirect  *redirect;
	struct me_cli_server_hello *server_hello;
	struct me_cli_client_hello *client_hello;
	struct me_cli_error *error;
	int retval;
	unsigned int i;
	#define rv(stmt) do {                  \
			retval = (stmt);       \
			if (retval)            \
				return retval; \
		} while (0)
	#define mp_array(sz) rv(msgpack_pack_array(pk, (sz)))
	#define mp_uint(uint) rv(msgpack_pack_unsigned_int(pk, (uint)))
	#define mp_uint64(uint) rv(msgpack_pack_uint64(pk, (uint)))
	#define mp_raw(ptr, size) do {                                 \
			size_t _sz_tmp = (size);                       \
			rv(msgpack_pack_raw(pk, _sz_tmp));             \
			rv(msgpack_pack_raw_body(pk, (ptr), _sz_tmp)); \
		} while(0)
	#define mp_raw_sizeof(what) mp_raw(what, sizeof(what))

	switch (u->m_type) {
	case ME_CMT_NEW_VALUE:
		new_value = &u->new_value;
		mp_array(3);
		mp_uint(u->m_type);
		mp_raw_sizeof(new_value->request_id);
		mp_raw(new_value->buf, new_value->size);
		break;
	case ME_CMT_ARRIVED_VALUE:
		arrived_value = &u->arrived_value;
		mp_array(3);
		mp_uint(u->m_type);
		mp_raw(arrived_value->buf, arrived_value->size);
		mp_uint64(arrived_value->iid);
		break;
	case ME_CMT_REDIRECT:
		redirect = &u->redirect;
		mp_array(2);
		mp_uint(u->m_type);
		mp_raw(redirect->ip, strlen(redirect->ip));
		break;
	case ME_CMT_SERVER_HELLO:
		server_hello = &u->server_hello;
		mp_array(2);
		mp_uint(u->m_type);
		mp_array(server_hello->count);
		for (i = 0; i < server_hello->count; i++) {
			mp_raw(server_hello->peers[i],
					strlen(server_hello->peers[i]));
		}
		break;
	case ME_CMT_CLIENT_HELLO:
		client_hello = &u->client_hello;
		mp_array(2);
		mp_uint(u->m_type);
		mp_uint64(client_hello->starting_iid);
		break;
	case ME_CMT_ERROR:
		error = &u->error;
		mp_array(2);
		mp_uint(u->m_type);
		mp_uint(error->code);
		break;
	}
	return 0;
	#undef mp_array
	#undef mp_uint
	#undef mp_uint64
	#undef mp_raw
	#undef mp_raw_sizeof
	#undef rv
}

void me_cli_msg_free(union me_cli_any *u)
{
	unsigned int i;
	struct me_cli_new_value *new_value;
	struct me_cli_arrived_value *arrived_value;
	struct me_cli_server_hello *server_hello;

	switch (u->m_type) {
	case ME_CMT_NEW_VALUE:
		new_value = &u->new_value;
		free(new_value->buf);
		break;
	case ME_CMT_ARRIVED_VALUE:
		arrived_value = &u->arrived_value;
		free(arrived_value->buf);
		break;
	case ME_CMT_REDIRECT:
		break;
	case ME_CMT_SERVER_HELLO:
		server_hello = &u->server_hello;
		for (i = 0; i < server_hello->count; i++)
			free(server_hello->peers[i]);
		free(server_hello->peers);
		break;
	case ME_CMT_CLIENT_HELLO:
		break;
	case ME_CMT_ERROR:
		break;
	}
}
