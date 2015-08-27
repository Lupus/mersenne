#include <msgpack.h>

enum wal_rec_type {
  WAL_REC_TYPE_STATE = 1,
  WAL_REC_TYPE_VALUE = 2,
  WAL_REC_TYPE_PROMISE = 3,
};

struct wal_value_payload {
  uint8_t *data;
  unsigned len;
};

struct wal_value {
  enum wal_rec_type type;
  uint64_t iid;
  uint64_t b;
  uint64_t vb;
  struct wal_value_payload content;
};

struct wal_promise {
  enum wal_rec_type type;
  uint64_t iid;
  uint64_t b;
};

struct wal_state {
  enum wal_rec_type type;
  uint64_t highest_accepted;
};

union wal_rec_any {
  enum wal_rec_type w_type;
  struct wal_value value;
  struct wal_promise promise;
  struct wal_state state;
};

int wal_msg_pack(msgpack_packer *pk, union wal_rec_any *msg);
int wal_msg_unpack(msgpack_object *obj, union wal_rec_any *result,
             int alloc_mem, char **error_msg);
void wal_msg_free(union wal_rec_any *result);
