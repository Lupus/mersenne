#include <uuid/uuid.h>
#include <msgpack.h>

enum me_cli_message_type {
	ME_CMT_NEW_VALUE = 0,
	ME_CMT_ARRIVED_OTHER_VALUE = 1,
	ME_CMT_ARRIVED_VALUE = 2,
};

struct me_cli_new_value {
	enum me_cli_message_type m_type;
	uuid_t request_id;
	void *buf;
	size_t size;
};

struct me_cli_arrived_value {
	enum me_cli_message_type m_type;
	uuid_t request_id;
	uint64_t iid;
};

struct me_cli_arrived_other_value {
	enum me_cli_message_type m_type;
	void *buf;
	size_t size;
	uint64_t iid;
};

union me_cli_any {
	enum me_cli_message_type m_type;
	struct me_cli_new_value new_value;
	struct me_cli_arrived_value arrived_value;
	struct me_cli_arrived_other_value other_value;
};

int me_cli_msg_pack(msgpack_packer *pk, union me_cli_any *msg);
int me_cli_msg_unpack(msgpack_object *obj, union me_cli_any *result,
		int alloc_mem, char **error_msg);
void me_cli_msg_free(union me_cli_any *result);
