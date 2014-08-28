#include <uuid/uuid.h>
#include <msgpack.h>

enum me_cli_message_type {
	ME_CMT_NEW_VALUE = 0,
	ME_CMT_ARRIVED_VALUE = 1,
	ME_CMT_REDIRECT = 2,
	ME_CMT_SERVER_HELLO = 1000,
	ME_CMT_CLIENT_HELLO = 1001,
	ME_CMT_ERROR = 1002,
};

struct me_cli_new_value {
	enum me_cli_message_type m_type;
	uuid_t request_id;
	void *buf;
	size_t size;
};

struct me_cli_arrived_value {
	enum me_cli_message_type m_type;
	void *buf;
	size_t size;
	uint64_t iid;
};

#define ME_CMT_IP_SIZE 15

struct me_cli_redirect {
	enum me_cli_message_type m_type;
	char ip[ME_CMT_IP_SIZE + 1];
};

struct me_cli_server_hello {
	enum me_cli_message_type m_type;
	char **peers;
	unsigned int count;
};

struct me_cli_client_hello {
	enum me_cli_message_type m_type;
	uint64_t starting_iid;
};

enum me_cli_error_code {
	ME_CME_SUCCESS = 0,
	ME_CME_IID_UNAVAILABLE,
};

struct me_cli_error {
	enum me_cli_message_type m_type;
	enum me_cli_error_code code;
};

union me_cli_any {
	enum me_cli_message_type m_type;
	struct me_cli_new_value new_value;
	struct me_cli_arrived_value arrived_value;
	struct me_cli_redirect redirect;
	struct me_cli_server_hello server_hello;
	struct me_cli_client_hello client_hello;
	struct me_cli_error error;
};

int me_cli_msg_pack(msgpack_packer *pk, union me_cli_any *msg);
int me_cli_msg_unpack(msgpack_object *obj, union me_cli_any *result,
		int alloc_mem, char **error_msg);
void me_cli_msg_free(union me_cli_any *result);
