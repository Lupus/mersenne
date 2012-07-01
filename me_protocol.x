#define __MTU     1500
#define __UDP_HEADER_SIZE 28
%#define ME_UDP_HEADER_SIZE __UDP_HEADER_SIZE

#define __MAX_XDR_MESSAGE_LEN (__MTU - __UDP_HEADER_SIZE - 8)
%#define ME_MAX_XDR_MESSAGE_LEN __MAX_XDR_MESSAGE_LEN

enum me_message_supertype {
	ME_OMEGA
};

enum me_message_type {
	ME_OMEGA_OK,
	ME_OMEGA_START,
	ME_OMEGA_ALERT
};

struct me_timeval {
	uint32_t tv_sec;
	uint32_t tv_usec;
};

struct me_omega_msg_header {
	int momh_count;
	struct me_timeval momh_sent;
};

union me_omega_msg_data switch(me_message_type type) {
	case ME_OMEGA_OK:
	case ME_OMEGA_START:
	case ME_OMEGA_ALERT:
	int mmd_k;

	default:
	void;
};

struct me_omega_message {
	struct me_omega_msg_header mom_header;
	struct me_omega_msg_data mom_data;
};

union me_message switch(me_message_supertype stype) {
	case ME_OMEGA:
	struct me_omega_message mm_omega_msg;

	default:
	void;
};
