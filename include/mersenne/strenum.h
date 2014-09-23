#include <mersenne/fiber_args.h>
#include <mersenne/me_protocol.h>
#include <mersenne/proposer_prv.h>

static inline
const char *strval_fiber_args_type(enum fiber_args_type val)
{
	switch(val) {
		case FAT_ME_MESSAGE: return "FAT_ME_MESSAGE";
		case FAT_PXS_CLIENT_VALUE: return "FAT_PXS_CLIENT_VALUE";
		case FAT_PXS_DELIVERED_VALUE: return "FAT_PXS_DELIVERED_VALUE";
		case FAT_QUIT: return "FAT_QUIT";
	}	return (char*)0;

}

static inline
const char *strval_me_message_supertype(enum me_message_supertype val)
{
	switch(val) {
		case ME_LEADER: return "ME_LEADER";
		case ME_PAXOS: return "ME_PAXOS";
	}	return (char*)0;

}

static inline
const char *strval_me_leader_message_type(enum me_leader_message_type val)
{
	switch(val) {
		case ME_LEADER_OK: return "ME_LEADER_OK";
		case ME_LEADER_START: return "ME_LEADER_START";
		case ME_LEADER_ACK: return "ME_LEADER_ACK";
	}	return (char*)0;

}

static inline
const char *strval_me_paxos_message_type(enum me_paxos_message_type val)
{
	switch(val) {
		case ME_PAXOS_PREPARE: return "ME_PAXOS_PREPARE";
		case ME_PAXOS_PROMISE: return "ME_PAXOS_PROMISE";
		case ME_PAXOS_ACCEPT: return "ME_PAXOS_ACCEPT";
		case ME_PAXOS_REJECT: return "ME_PAXOS_REJECT";
		case ME_PAXOS_ACCEPTOR_STATE: return "ME_PAXOS_ACCEPTOR_STATE";
		case ME_PAXOS_LEARN: return "ME_PAXOS_LEARN";
		case ME_PAXOS_RELEARN: return "ME_PAXOS_RELEARN";
		case ME_PAXOS_RETRANSMIT: return "ME_PAXOS_RETRANSMIT";
		case ME_PAXOS_CLIENT_VALUE: return "ME_PAXOS_CLIENT_VALUE";
	}	return (char*)0;

}

static inline
const char *strval_instance_state(enum instance_state val)
{
	switch(val) {
		case IS_EMPTY: return "IS_EMPTY";
		case IS_P1_PENDING: return "IS_P1_PENDING";
		case IS_P1_READY_NO_VALUE: return "IS_P1_READY_NO_VALUE";
		case IS_P1_READY_WITH_VALUE: return "IS_P1_READY_WITH_VALUE";
		case IS_P2_PENDING: return "IS_P2_PENDING";
		case IS_DELIVERED: return "IS_DELIVERED";
		case IS_MAX: return "IS_MAX";
	}	return (char*)0;

}

static inline
const char *strval_instance_event_type(enum instance_event_type val)
{
	switch(val) {
		case IE_I: return "IE_I";
		case IE_S: return "IE_S";
		case IE_TO: return "IE_TO";
		case IE_TO21: return "IE_TO21";
		case IE_P: return "IE_P";
		case IE_R0: return "IE_R0";
		case IE_R1: return "IE_R1";
		case IE_NV: return "IE_NV";
		case IE_A: return "IE_A";
		case IE_E: return "IE_E";
		case IE_D: return "IE_D";
	}	return (char*)0;

}
