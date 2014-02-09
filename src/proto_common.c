#include <mersenne/proto_common.h>

static void tokenize_object_recurse(me_proto_parser_tokens *arr,
		msgpack_object *obj)
{
	struct me_proto_parser_token token;
	token.obj = obj;
	int i;
	switch (obj->type) {
	case MSGPACK_OBJECT_NIL:
		token.id = MOBJ_NIL;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	case MSGPACK_OBJECT_BOOLEAN:
		token.id = MOBJ_BOOLEAN;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	case MSGPACK_OBJECT_POSITIVE_INTEGER:
		token.id = MOBJ_POS_INT;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	case MSGPACK_OBJECT_NEGATIVE_INTEGER:
		token.id = MOBJ_NEG_INT;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	case MSGPACK_OBJECT_DOUBLE:
		token.id = MOBJ_DOUBLE;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	case MSGPACK_OBJECT_RAW:
		token.id = MOBJ_RAW;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	case MSGPACK_OBJECT_ARRAY:
		token.id = MOBJ_ARRAY;
		kv_push(struct me_proto_parser_token, *arr, token);
		for (i = 0; i < obj->via.array.size; i++) {
			tokenize_object_recurse(arr, &obj->via.array.ptr[i]);
		}
		token.id = MOBJ_END;
		token.obj = NULL;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	case MSGPACK_OBJECT_MAP:
		token.id = MOBJ_MAP;
		kv_push(struct me_proto_parser_token, *arr, token);
		for (i = 0; i < obj->via.map.size; i++) {
			tokenize_object_recurse(arr, &obj->via.map.ptr[i].key);
			tokenize_object_recurse(arr, &obj->via.map.ptr[i].val);
		}
		token.id = MOBJ_END;
		token.obj = NULL;
		kv_push(struct me_proto_parser_token, *arr, token);
		break;
	};
}

me_proto_parser_tokens me_proto_tokenize_object(msgpack_object *obj)
{
	me_proto_parser_tokens tokens;
	kv_init(tokens);
	tokenize_object_recurse(&tokens, obj);
	return tokens;
}
