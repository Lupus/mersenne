#include <msgpack-proto-ragel/tokens.h>
#include <msgpack-proto-ragel/kvec.h>

struct mppr_tokens {
	kvec_t(struct mppr_parser_token) v;
};

static void tokenize_object_recurse(struct mppr_tokens *arr,
		msgpack_object *obj)
{
	struct mppr_parser_token token;
	token.obj = obj;
	int i;
	switch (obj->type) {
	case MSGPACK_OBJECT_NIL:
		token.id = MOBJ_NIL;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	case MSGPACK_OBJECT_BOOLEAN:
		token.id = MOBJ_BOOLEAN;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	case MSGPACK_OBJECT_POSITIVE_INTEGER:
		token.id = MOBJ_POS_INT;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	case MSGPACK_OBJECT_NEGATIVE_INTEGER:
		token.id = MOBJ_NEG_INT;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	case MSGPACK_OBJECT_DOUBLE:
		token.id = MOBJ_DOUBLE;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	case MSGPACK_OBJECT_RAW:
		token.id = MOBJ_RAW;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	case MSGPACK_OBJECT_ARRAY:
		token.id = MOBJ_ARRAY;
		kv_push(struct mppr_parser_token, arr->v, token);
		for (i = 0; i < obj->via.array.size; i++) {
			tokenize_object_recurse(arr, &obj->via.array.ptr[i]);
		}
		token.id = MOBJ_END;
		token.obj = NULL;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	case MSGPACK_OBJECT_MAP:
		token.id = MOBJ_MAP;
		kv_push(struct mppr_parser_token, arr->v, token);
		for (i = 0; i < obj->via.map.size; i++) {
			tokenize_object_recurse(arr, &obj->via.map.ptr[i].key);
			tokenize_object_recurse(arr, &obj->via.map.ptr[i].val);
		}
		token.id = MOBJ_END;
		token.obj = NULL;
		kv_push(struct mppr_parser_token, arr->v, token);
		break;
	};
}

struct mppr_tokens *mppr_tokenize_object(msgpack_object *obj)
{
	struct mppr_tokens *tokens;
	tokens = malloc(sizeof(*tokens));
	kv_init(tokens->v);
	tokenize_object_recurse(tokens, obj);
	return tokens;
}

void mppr_set_ragel_p_pe(struct mppr_tokens *tokens,
		struct mppr_parser_token **p,
		struct mppr_parser_token **pe)
{
	*p = &kv_A(tokens->v, 0);
	*pe = &kv_A(tokens->v, kv_size(tokens->v));
}

void mppr_destroy_tokens(struct mppr_tokens *tokens)
{
	kv_destroy(tokens->v);
	free(tokens);
}
