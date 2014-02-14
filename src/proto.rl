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

	action validate_pxs_value_length {
		if (0 >= mp_raw(fpc).size) {
			elog("empty paxos value received");
			fbreak;
		}
	}

	m_type = MOBJ_POS_INT;
	iid = MOBJ_POS_INT;
	uuid = MOBJ_RAW @validate_uuid_length;
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
		uuid @{
			mp_raw_cpy(fpc, r->arrived_value.request_id);
		} .
		iid @{
			r->arrived_value.iid = mp_uint(fpc);
		}
		@{ fret; };

	m_other_value :=
		pxs_value @{
			mp_raw_opt(fpc, r->other_value.buf,
					r->other_value.size);
		} .
		iid @{
			r->other_value.iid = mp_uint(fpc);
		}
		@{ fret; };

	action dispatch_m_type {
		r->m_type = mp_uint(fpc);
		switch (mp_uint(fpc)) {
		case ME_CMT_NEW_VALUE:
			fcall m_new_value;
			break;
		case ME_CMT_ARRIVED_OTHER_VALUE:
			fcall m_other_value;
			break;
		case ME_CMT_ARRIVED_VALUE:
			fcall m_arrived_value;
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
#undef mp_raw_cpy
#undef mp_raw_dup
#undef mp_raw_ref
#undef mp_raw_opt

int me_cli_msg_pack(msgpack_packer *pk, union me_cli_any *u)
{
	struct me_cli_new_value *new_value;
	struct me_cli_arrived_value *arrived_value;
	struct me_cli_arrived_other_value *other_value;
	int retval;
	#define rv(stmt) do {                  \
			retval = (stmt);       \
			if (retval)            \
				return retval; \
		} while (0)
	#define mp_array(sz) rv(msgpack_pack_array(pk, (sz)))
	#define mp_uint(uint) rv(msgpack_pack_unsigned_int(pk, (uint)))
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
	case ME_CMT_ARRIVED_OTHER_VALUE:
		other_value = &u->other_value;
		mp_array(3);
		mp_uint(u->m_type);
		mp_raw(other_value->buf, other_value->size);
		mp_uint(other_value->iid);
		break;
	case ME_CMT_ARRIVED_VALUE:
		arrived_value = &u->arrived_value;
		mp_array(3);
		mp_uint(u->m_type);
		mp_raw_sizeof(arrived_value->request_id);
		mp_uint(arrived_value->iid);
		break;
	}
	return 0;
	#undef mp_array
	#undef mp_uint
	#undef mp_raw
	#undef mp_raw_sizeof
	#undef rv
}

void me_cli_msg_free(union me_cli_any *u)
{
	struct me_cli_new_value *new_value;
	struct me_cli_arrived_other_value *other_value;

	switch (u->m_type) {
	case ME_CMT_NEW_VALUE:
		new_value = &u->new_value;
		free(new_value->buf);
		break;
	case ME_CMT_ARRIVED_OTHER_VALUE:
		other_value = &u->other_value;
		free(other_value->buf);
		break;
	case ME_CMT_ARRIVED_VALUE:
		break;
	}
}
