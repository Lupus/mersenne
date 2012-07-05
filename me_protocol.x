#define __MTU     1500
#define __UDP_HEADER_SIZE 28
%#define ME_UDP_HEADER_SIZE __UDP_HEADER_SIZE

#define __MAX_XDR_MESSAGE_LEN (__MTU - __UDP_HEADER_SIZE - 8)
%#define ME_MAX_XDR_MESSAGE_LEN __MAX_XDR_MESSAGE_LEN

%#include <xdr.h>

enum me_message_supertype {
	ME_OMEGA
};

enum me_message_type {
	ME_OMEGA_OK,
	ME_OMEGA_START,
	ME_OMEGA_ACK
};

struct me_omega_msg_header {
	int count;
	struct timeval sent;
	opaque config_checksum[36];
};

struct me_omega_msg_ok_data {
	int k;
	bitmask_ptr trust;
};

struct me_omega_msg_round_data {
	int k;
};

union me_omega_msg_data switch(me_message_type type) {
	case ME_OMEGA_START:
	case ME_OMEGA_ACK:
	struct me_omega_msg_round_data round;

	case ME_OMEGA_OK:
	struct me_omega_msg_ok_data ok;

	default:
	void;
};

struct me_omega_message {
	struct me_omega_msg_header header;
	struct me_omega_msg_data data;
};

union me_message switch(me_message_supertype mm_stype) {
	case ME_OMEGA:
	struct me_omega_message omega_message;

	default:
	void;
};
