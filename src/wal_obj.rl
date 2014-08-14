/* vim: set syntax=c: */
#include <assert.h>
#include <stdio.h>

#include <msgpack-proto-ragel/tokens.h>
#include <mersenne/wal_obj.h>

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
			x = (uint8_t *)mp_raw(fpc).ptr; \
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
	machine wal_parser;
	alphtype unsigned int;
	getkey fpc->id;

	include mppr_common_decls "msgpack-proto-ragel/mppr_common.rl";
 
	action validate_value_length {
		if(0 >= mp_raw(fpc).size){
			elog("empty value encountered");
			fbreak;
		}
	}

	action validate_iid_value {
		if(0 >= mp_uint(fpc)){
			elog("iid not greater than zero");
			fbreak;
		}
	}

	action validate_b_value {
		if(0 >= mp_uint(fpc)){
			elog("b not greater than zero");
			fbreak;
		}
	}

	w_type = MOBJ_POS_INT;
	iid = MOBJ_POS_INT @validate_iid_value;
	b = MOBJ_POS_INT @validate_b_value;
	content = MOBJ_RAW @validate_value_length;
	vb = b;
	highest_accepted = MOBJ_POS_INT;
	highest_finalized = MOBJ_POS_INT;
	arr_start = MOBJ_ARRAY;
	arr_end = MOBJ_END;

	m_wal_value :=
		iid @{
			r->value.iid = mp_uint(fpc);
		} .
		b @{
			r->value.b = mp_uint(fpc);
		} .
		vb @{
			r->value.vb = mp_uint(fpc);
		} .
		content @{
			mp_raw_opt(fpc, r->value.content.data, r->value.content.len);
		}
		@{ fret; };

	m_wal_promise :=
		iid @{
			r->promise.iid = mp_uint(fpc);
		} .
		b @{
			r->promise.b = mp_uint(fpc);
		}
		@{ fret; };

	m_wal_state :=
		highest_accepted @{
			r->state.highest_accepted = mp_uint(fpc);
		} .
		highest_finalized @{
			r->state.highest_finalized = mp_uint(fpc);
		}
		@{ fret; };

	action dispatch_w_type {
		r->w_type = mp_uint(fpc);
		switch (mp_uint(fpc)) {
		case WAL_REC_TYPE_VALUE:
			fcall m_wal_value;
			break;
		case WAL_REC_TYPE_PROMISE:
			fcall m_wal_promise;
			break;
		case WAL_REC_TYPE_STATE:
			fcall m_wal_state;
			break;
		default:
			elog("invalid message type: %ld", mp_uint(fpc));
			break;
		}
		if (error)
			fbreak;
	}

	main := arr_start . w_type @dispatch_w_type . arr_end;
}%%

%% write data;

int wal_msg_unpack(msgpack_object *obj, union wal_rec_any *r, int alloc_mem, char **error_msg)
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

	if (cs == wal_parser_error) {
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

int wal_msg_pack(msgpack_packer *pk, union wal_rec_any *u)
{
	struct wal_value *value;
	struct wal_promise *promise;
	struct wal_state *state;
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

	switch (u->w_type) {
	case WAL_REC_TYPE_VALUE:
		value = &u->value;
		mp_array(5);
		mp_uint(u->w_type);
		mp_uint(value->iid);
		mp_uint(value->b);
		mp_uint(value->vb);
		mp_raw(value->content.data, value->content.len);
		break;
	case WAL_REC_TYPE_PROMISE:
		promise = &u->promise;
		mp_array(3);
		mp_uint(u->w_type);
		mp_uint(promise->iid);
		mp_uint(promise->b);
		break;
	case WAL_REC_TYPE_STATE:
		state = &u->state;
		mp_array(3);
		mp_uint(u->w_type);
		mp_uint(state->highest_accepted);
		mp_uint(state->highest_finalized);
		break;
	}

	return 0;
	#undef mp_array
	#undef mp_uint
	#undef mp_raw
	#undef mp_raw_sizeof
	#undef rv
}

void wal_msg_free(union wal_rec_any *u)
{
	switch (u->w_type) {
	case WAL_REC_TYPE_VALUE:
		free(u->value.content.data);
		break;
	case WAL_REC_TYPE_PROMISE:
		break;
	case WAL_REC_TYPE_STATE:
		break;
	}
}
