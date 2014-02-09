#include <msgpack.h>

#include <mersenne/kvec.h>

enum me_proto_parser_token_type {
	MOBJ_NIL = 0x00,
	MOBJ_BOOLEAN = 0x01,
	MOBJ_POS_INT = 0x02,
	MOBJ_NEG_INT = 0x03,
	MOBJ_DOUBLE = 0x04,
	MOBJ_RAW = 0x05,
	MOBJ_ARRAY = 0x06,
	MOBJ_MAP = 0x07,

	MOBJ_END = 0x100,
};

struct me_proto_parser_token {
	enum me_proto_parser_token_type id;
	msgpack_object *obj;
};

typedef kvec_t(struct me_proto_parser_token) me_proto_parser_tokens;

me_proto_parser_tokens me_proto_tokenize_object(msgpack_object *obj);

static inline void me_proto_set_ragel_p_pe(me_proto_parser_tokens tokens,
		struct me_proto_parser_token **p,
		struct me_proto_parser_token **pe)
{
	*p = &kv_A(tokens, 0);
	*pe = &kv_A(tokens, kv_size(tokens));
}

static inline void me_proto_destroy_tokens(me_proto_parser_tokens tokens)
{
	kv_destroy(tokens);
}
