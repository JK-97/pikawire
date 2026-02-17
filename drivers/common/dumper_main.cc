// pikawire-dumper: sidecar reader exposing the snapshot iterator over the
// framed stdin/stdout protocol (see pikawire_dumper_proto.h). Running the
// version-pinned storage layer in a child process dodges glibc's static TLS
// limit for dlopen of the huge rocksdb TLS image, and isolates storage-layer
// LOG(FATAL) aborts from the product process.
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <string>

#include "pikawire_driver.h"

namespace {

bool ReadFull(void* p, size_t n) {
  auto* q = (uint8_t*)p;
  while (n) {
    ssize_t r = read(STDIN_FILENO, q, n);
    if (r <= 0) return false;
    q += r;
    n -= (size_t)r;
  }
  return true;
}

bool WriteFull(const void* p, size_t n) {
  auto* q = (const uint8_t*)p;
  while (n) {
    ssize_t r = write(STDOUT_FILENO, q, n);
    if (r <= 0) return false;
    q += r;
    n -= (size_t)r;
  }
  return true;
}

template <typename T>
void Put(std::string* out, T v) {
  out->append((const char*)&v, sizeof(v));
}

template <typename T>
bool Get(const uint8_t** p, const uint8_t* end, T* v) {
  if (end - *p < (ptrdiff_t)sizeof(T)) return false;
  memcpy(v, *p, sizeof(T));
  *p += sizeof(T);
  return true;
}

bool GetStr(const uint8_t** p, const uint8_t* end, uint16_t maxn, std::string* s, bool u8len = false) {
  uint16_t n = 0;
  if (u8len) {
    uint8_t b;
    if (!Get(p, end, &b)) return false;
    n = b;
  } else if (!Get(p, end, &n)) {
    return false;
  }
  if (n > maxn || (size_t)(end - *p) < n) return false;
  s->assign((const char*)*p, n);
  *p += n;
  return true;
}

bool WriteReply(const std::string& payload) {
  uint32_t len = (uint32_t)payload.size();
  return WriteFull(&len, 4) && WriteFull(payload.data(), len);
}

SnapshotIter* g_iter = nullptr;

constexpr int32_t kBatch = 2048;  // records per 'N' reply frame

}  // namespace

int main() {
  for (;;) {
    uint8_t op;
    if (!ReadFull(&op, 1)) return 0;  // stdin closed: parent gone
    switch (op) {
      case 'O': {
        if (g_iter) {
          snapshot_close(g_iter);
          g_iter = nullptr;
        }
        static const uint16_t kBody = 8192 - 1;  // frame body after the op byte
        std::string raw(kBody, '\0');
        if (!ReadFull(&raw[0], kBody)) return 0;
        const uint8_t* p = (const uint8_t*)raw.data();
        const uint8_t* end = p + kBody;
        std::string dir, db, pat;
        SnapshotOptions opts{};
        if (!GetStr(&p, end, 3000, &dir) || !GetStr(&p, end, 256, &db) ||
            !Get(&p, end, &opts.batch_num) || !Get(&p, end, &opts.scan_batch) ||
            !Get(&p, end, &opts.type_mask) || !Get(&p, end, &opts.scan_strategy) ||
            !Get(&p, end, &opts.list_tail_n) || !GetStr(&p, end, 1024, &pat))
          return 1;
        opts.scan_pattern = pat.c_str();
        std::string rtype, rkey;
        if (!GetStr(&p, end, 16, &rtype, true) || !GetStr(&p, end, 3000, &rkey)) {
          return 1;
        }
        opts.resume_type = rtype.empty() ? nullptr : rtype.c_str();
        opts.resume_key = rkey.empty() ? nullptr : rkey.c_str();
        char* err = nullptr;
        g_iter = snapshot_open(dir.c_str(), db.c_str(), opts, &err);
        std::string reply;
        Put(&reply, (uint8_t)(g_iter ? 0 : 1));
        if (!g_iter) {
          const char* msg = err ? err : "unknown open failure";
          Put(&reply, (uint16_t)strlen(msg));
          reply.append(msg);
        }
        free(err);
        if (!WriteReply(reply)) return 1;
        break;
      }
      case 'N': {
        if (!g_iter) return 1;
        std::string payload;
        Put(&payload, (uint32_t)0);  // count, patched later
        SnapshotRecord* recs = nullptr;
        size_t n = 0;
        char* err = nullptr;
        int r = snapshot_next_batch(g_iter, &recs, &n, kBatch, &err);
        if (r < 0) {
          free(err);
          return 1;
        }
        for (size_t i = 0; i < n; i++) {
          Put(&payload, (uint16_t)recs[i].data_type_len);
          payload.append(recs[i].data_type, recs[i].data_type_len);
          Put(&payload, (uint16_t)recs[i].key_len);
          payload.append(recs[i].key, recs[i].key_len);
          Put(&payload, (uint32_t)recs[i].raw_resp_len);
          payload.append(recs[i].raw_resp, recs[i].raw_resp_len);
        }
        uint32_t count = (uint32_t)n;
        if (recs) snapshot_free_batch(recs, n);
        payload.replace(0, 4, std::string((const char*)&count, 4));
        if (!WriteReply(payload)) return 1;
        break;
      }
      case 'C':
        if (g_iter) snapshot_close(g_iter);
        return 0;
      default:
        return 1;
    }
  }
}
