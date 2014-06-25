#include <msgpack.h>

enum WalRecType {
  WAL_REC_TYPE_STATE = 1,
  WAL_REC_TYPE_VALUE = 2,
  WAL_REC_TYPE_PROMISE = 3,
};

struct WalValuePayload {
  uint8_t *data;
  unsigned len;
};

struct WalValue {
  enum WalRecType type;
  uint64_t iid;
  uint64_t b;
  uint64_t vb;
  struct WalValuePayload content;
  int has_content;
  int has_vb;
};

struct WalPromise {
  enum WalRecType type;
  uint64_t iid;
  uint64_t b;
};

struct WalState {
  enum WalRecType type;
  uint64_t highest_accepted;
  uint64_t highest_finalized;
};

union WalRec {
  enum WalRecType w_type;
  struct WalValue value;
  struct WalPromise promise;
  struct WalState state;
};

int wal_msg_pack(msgpack_packer *pk, union WalRec *msg);
int wal_msg_unpack(msgpack_object *obj, union WalRec *result,
             int alloc_mem, char **error_msg);
void wal_msg_free(union WalRec *result);
