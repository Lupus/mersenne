#include <msgpack.h>

enum mppr_parser_token_type {
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

struct mppr_parser_token {
	enum mppr_parser_token_type id;
	msgpack_object *obj;
};

struct mppr_tokens;

struct mppr_tokens *mppr_tokenize_object(msgpack_object *obj);
void mppr_set_ragel_p_pe(struct mppr_tokens *tokens,
		struct mppr_parser_token **p,
		struct mppr_parser_token **pe);
void mppr_destroy_tokens(struct mppr_tokens *tokens);
