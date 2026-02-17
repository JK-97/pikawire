// pikawire reader driver for pikiwidb 3.5.x dumps (six per-type engine dirs).
#include "pikawire_driver.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>


#include "storage/storage.h"

#include "rocksdb/db.h"

namespace {

std::string AppendPath(const std::string& dir, const std::string& sub) {
  if (!dir.empty() && dir.back() == '/') return dir + sub;
  return dir + "/" + sub;
}

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";

struct RecordBuffer {
  std::string data_type;
  std::string key;
  std::string raw_resp;
};

std::string SerializeRESP(const std::vector<std::string>& argv) {
  std::string out;
  out.reserve(64);
  out.append("*");
  out.append(std::to_string(argv.size()));
  out.append("\r\n");
  for (const auto& arg : argv) {
    out.append("$");
    out.append(std::to_string(arg.size()));
    out.append("\r\n");
    out.append(arg);
    out.append("\r\n");
  }
  return out;
}

int64_t ScanBatchCount(int batch_num) {
  const int64_t max_batch = 30000;
  int64_t batch_count = static_cast<int64_t>(batch_num) * 10;
  if (batch_count < max_batch) {
    if (batch_num < max_batch) {
      batch_count = max_batch;
    } else {
      batch_count = static_cast<int64_t>(batch_num) * 2;
    }
  }
  return batch_count;
}

template <typename T, typename = void>
struct HasTTLOneArg : std::false_type {};

template <typename T>
struct HasTTLOneArg<T, std::void_t<decltype(static_cast<int64_t (T::*)(const storage::Slice&)>(
                           &T::TTL))>> : std::true_type {};

template <typename T, typename = void>
struct HasTTLTwoArg : std::false_type {};

template <typename T>
struct HasTTLTwoArg<T, std::void_t<decltype(static_cast<std::map<storage::DataType, int64_t> (T::*)(
                           const storage::Slice&, std::map<storage::DataType, storage::Status>*)>(
                           &T::TTL))>> : std::true_type {};

template <typename StorageT>
int64_t GetTTLCompat(StorageT* db, const std::string& key, storage::DataType type) {
  if constexpr (HasTTLTwoArg<StorageT>::value) {
    std::map<storage::DataType, storage::Status> type_status;
    std::map<storage::DataType, int64_t> ttl_map = db->TTL(storage::Slice(key), &type_status);
    auto it = ttl_map.find(type);
    if (it != ttl_map.end()) {
      return it->second;
    }
    return -1;
  }
  if constexpr (HasTTLOneArg<StorageT>::value) {
    return db->TTL(storage::Slice(key));
  }
  return -1;
}

bool ShouldSkipKey(const std::string& key) {
  return key.compare(0, SlotKeyPrefix.size(), SlotKeyPrefix) == 0;
}

std::string DataTypeName(storage::DataType type) {
  switch (type) {
    case storage::DataType::kStrings:
      return "string";
    case storage::DataType::kLists:
      return "list";
    case storage::DataType::kHashes:
      return "hash";
    case storage::DataType::kSets:
      return "set";
    case storage::DataType::kZSets:
      return "zset";
    default:
      return "unknown";
  }
}

}  // namespace

struct SnapshotIter {
  storage::Storage storage;
  storage::StorageOptions options;
  std::string db_root;

  int batch_num = 512;
  int64_t scan_batch = 30000;
  std::string scan_pattern = "*";
  uint32_t type_mask = SNAPSHOT_TYPE_ALL;
  int scan_strategy = SNAPSHOT_SCAN_CURSOR;
  std::string scan_start;

  enum Stage {
    kStrings,
    kLists,
    kHashes,
    kSets,
    kZSets,
    kDone,
  } stage = kStrings;

  int64_t cursor = 0;
  std::vector<std::string> keys;
  size_t key_index = 0;
  bool scan_done = false;

  // Per-key state
  std::string current_key;
  bool key_loaded = false;

  // lists
  int64_t list_pos = 0;
  int64_t list_start_pos = 0;
  bool list_len_loaded = false;
  int64_t list_tail_n = 0;
  bool list_expire_emitted = false;
  bool list_done = false;

  // hashes
  std::vector<storage::FieldValue> hash_fvs;
  size_t hash_index = 0;
  bool hash_loaded = false;
  bool hash_expire_emitted = false;

  // sets
  std::vector<std::string> set_members;
  size_t set_index = 0;
  bool set_loaded = false;
  bool set_expire_emitted = false;

  // zsets
  int64_t zset_pos = 0;
  std::vector<storage::ScoreMember> zset_members;
  size_t zset_index = 0;
  bool zset_expire_emitted = false;
  bool zset_loaded = false;

  bool first_next_logged = false;
  bool eof_logged = false;
  // Durable resume: skip stages before resume_type; within the matching
  // stage scan from resume_key inclusive (STARTKEY), then revert to cursor.
  std::string resume_type;
  std::string resume_key;
  bool resume_pending = false;
  bool resume_engaged = false;
  int scan_log_remaining = 5;
  bool is_type_allowed(storage::DataType type) const {
    switch (type) {
      case storage::DataType::kStrings:
        return (type_mask & SNAPSHOT_TYPE_STRINGS) != 0;
      case storage::DataType::kLists:
        return (type_mask & SNAPSHOT_TYPE_LISTS) != 0;
      case storage::DataType::kHashes:
        return (type_mask & SNAPSHOT_TYPE_HASHES) != 0;
      case storage::DataType::kSets:
        return (type_mask & SNAPSHOT_TYPE_SETS) != 0;
      case storage::DataType::kZSets:
        return (type_mask & SNAPSHOT_TYPE_ZSETS) != 0;
      case storage::DataType::kStreams:
        return (type_mask & SNAPSHOT_TYPE_STREAMS) != 0;
      default:
        return true;
    }
  }

  bool load_keys(storage::DataType type, std::string* err) {
    if (key_index < keys.size()) {
      return true;
    }
    if (scan_done) {
      return false;
    }
    keys.clear();
    key_index = 0;
    if (scan_log_remaining > 0) {
      fprintf(stderr, "[snapshot] scan start type=%d cursor=%lld count=%lld\n", static_cast<int>(type),
              static_cast<long long>(cursor), static_cast<long long>(scan_batch));
      fflush(stderr);
    }
    if (scan_strategy == SNAPSHOT_SCAN_STARTKEY || resume_engaged) {
      std::string next_key;
      auto s = storage.Scanx(type, scan_start, scan_pattern, scan_batch, &keys, &next_key);
      if (!s.ok()) {
        if (err) {
          *err = s.ToString();
        }
        return false;
      }
      scan_start = next_key;
      scan_done = scan_start.empty();
    } else {
      cursor = storage.Scan(type, cursor, scan_pattern, scan_batch, &keys);
      scan_done = (cursor == 0);
    }
    if (scan_log_remaining > 0) {
      fprintf(stderr, "[snapshot] scan done type=%d cursor=%lld keys=%zu\n", static_cast<int>(type),
              static_cast<long long>(cursor), keys.size());
      fflush(stderr);
      scan_log_remaining--;
    }
    if (scan_strategy == SNAPSHOT_SCAN_STARTKEY || resume_engaged) {
      return !keys.empty() || !scan_start.empty();
    }
    return !keys.empty() || cursor != 0;
  }

  void reset_key_state() {
    current_key.clear();
    key_loaded = false;
    list_pos = 0;
    list_start_pos = 0;
    list_len_loaded = false;
    list_done = false;
    list_expire_emitted = false;
    hash_fvs.clear();
    hash_index = 0;
    hash_loaded = false;
    hash_expire_emitted = false;
    set_members.clear();
    set_index = 0;
    set_loaded = false;
    set_expire_emitted = false;
    zset_pos = 0;
    zset_members.clear();
    zset_index = 0;
    zset_loaded = false;
    zset_expire_emitted = false;
  }

  bool next_record(RecordBuffer* out, std::string* err) {
    while (stage != kDone) {
      storage::DataType dtype = storage::DataType::kStrings;
      switch (stage) {
        case kStrings:
          dtype = storage::DataType::kStrings;
          break;
        case kLists:
          dtype = storage::DataType::kLists;
          break;
        case kHashes:
          dtype = storage::DataType::kHashes;
          break;
        case kSets:
          dtype = storage::DataType::kSets;
          break;
        case kZSets:
          dtype = storage::DataType::kZSets;
          break;
        default:
          stage = kDone;
          continue;
      }

      if (resume_pending && DataTypeName(dtype) != resume_type) {
        // stage precedes the resume coordinate: skip without scanning
        stage = static_cast<Stage>(static_cast<int>(stage) + 1);
        cursor = 0;
        scan_start.clear();
        scan_done = false;
        continue;
      }
      if (resume_pending && DataTypeName(dtype) == resume_type) {
        resume_pending = false;
        resume_engaged = true;
        scan_start = resume_key;  // inclusive; the re-emitted image is idempotent
      }
      if (!is_type_allowed(dtype)) {
        stage = static_cast<Stage>(static_cast<int>(stage) + 1);
        cursor = 0;
        scan_start.clear();
        scan_done = false;
        continue;
      }

      if (!key_loaded) {
        if (!load_keys(dtype, err)) {
          if (err && !err->empty()) {
            return false;
          }
          stage = static_cast<Stage>(static_cast<int>(stage) + 1);
          cursor = 0;
          scan_start.clear();
          scan_done = false;
          resume_engaged = false;
          continue;
        }
        if (key_index >= keys.size()) {
          continue;
        }
        reset_key_state();
        current_key = keys[key_index++];
        key_loaded = true;
        if (ShouldSkipKey(current_key)) {
          key_loaded = false;
          continue;
        }
      }

      switch (stage) {
        case kStrings: {
          std::string value;
          auto s = storage.Get(current_key, &value);
          if (!s.ok()) {
            key_loaded = false;
            continue;
          }
          std::vector<std::string> argv;
          argv.reserve(4);
          argv.push_back("SET");
          argv.push_back(current_key);
          argv.push_back(value);
          int64_t ttl = GetTTLCompat(&storage, current_key, storage::DataType::kStrings);
          if (ttl > 0) {
            argv.push_back("EX");
            argv.push_back(std::to_string(ttl));
          }
          out->data_type = DataTypeName(dtype);
          out->key = current_key;
          out->raw_resp = SerializeRESP(argv);
          key_loaded = false;
          return true;
        }
        case kLists: {
          if (list_tail_n > 0 && !list_len_loaded) {
            uint64_t len = 0;
            auto s = storage.LLen(current_key, &len);
            if (!s.ok()) {
              key_loaded = false;
              continue;
            }
            if (len > static_cast<uint64_t>(list_tail_n)) {
              list_start_pos = static_cast<int64_t>(len - static_cast<uint64_t>(list_tail_n));
            } else {
              list_start_pos = 0;
            }
            list_pos = list_start_pos;
            list_len_loaded = true;
          }
          if (!list_done) {
            std::vector<std::string> list;
            auto s = storage.LRange(current_key, list_pos, list_pos + batch_num - 1, &list);
            if (!s.ok()) {
              key_loaded = false;
              continue;
            }
            if (!list.empty()) {
              std::vector<std::string> argv;
              argv.reserve(list.size() + 2);
              argv.push_back("RPUSH");
              argv.push_back(current_key);
              for (const auto& e : list) {
                argv.push_back(e);
              }
              out->data_type = DataTypeName(dtype);
              out->key = current_key;
              out->raw_resp = SerializeRESP(argv);
              list_pos += batch_num;
              return true;
            }
            list_done = true;
          }
          if (!list_expire_emitted) {
            int64_t ttl = GetTTLCompat(&storage, current_key, storage::DataType::kLists);
            list_expire_emitted = true;
            if (ttl > 0) {
              std::vector<std::string> argv;
              argv.push_back("EXPIRE");
              argv.push_back(current_key);
              argv.push_back(std::to_string(ttl));
              out->data_type = DataTypeName(dtype);
              out->key = current_key;
              out->raw_resp = SerializeRESP(argv);
              return true;
            }
          }
          key_loaded = false;
          continue; // next key (return false here would END THE WHOLE DUMP)
        }
        case kHashes: {
          if (!hash_loaded) {
            auto s = storage.HGetall(current_key, &hash_fvs);
            if (!s.ok()) {
              key_loaded = false;
              continue;
            }
            hash_loaded = true;
            hash_index = 0;
          }
          if (hash_index < hash_fvs.size()) {
            size_t batch = std::min(static_cast<size_t>(batch_num), hash_fvs.size() - hash_index);
            std::vector<std::string> argv;
            argv.reserve(batch * 2 + 2);
            argv.push_back("HMSET");
            argv.push_back(current_key);
            for (size_t i = 0; i < batch; ++i) {
              const auto& fv = hash_fvs[hash_index + i];
              argv.push_back(fv.field);
              argv.push_back(fv.value);
            }
            hash_index += batch;
            out->data_type = DataTypeName(dtype);
            out->key = current_key;
            out->raw_resp = SerializeRESP(argv);
            return true;
          }
          if (!hash_expire_emitted) {
            int64_t ttl = GetTTLCompat(&storage, current_key, storage::DataType::kHashes);
            hash_expire_emitted = true;
            if (ttl > 0) {
              std::vector<std::string> argv;
              argv.push_back("EXPIRE");
              argv.push_back(current_key);
              argv.push_back(std::to_string(ttl));
              out->data_type = DataTypeName(dtype);
              out->key = current_key;
              out->raw_resp = SerializeRESP(argv);
              return true;
            }
          }
          key_loaded = false;
          continue; // next key (return false here would END THE WHOLE DUMP)
        }
        case kSets: {
          if (!set_loaded) {
            auto s = storage.SMembers(current_key, &set_members);
            if (!s.ok()) {
              key_loaded = false;
              continue;
            }
            set_loaded = true;
            set_index = 0;
          }
          if (set_index < set_members.size()) {
            size_t batch = std::min(static_cast<size_t>(batch_num), set_members.size() - set_index);
            std::vector<std::string> argv;
            argv.reserve(batch + 2);
            argv.push_back("SADD");
            argv.push_back(current_key);
            for (size_t i = 0; i < batch; ++i) {
              argv.push_back(set_members[set_index + i]);
            }
            set_index += batch;
            out->data_type = DataTypeName(dtype);
            out->key = current_key;
            out->raw_resp = SerializeRESP(argv);
            return true;
          }
          if (!set_expire_emitted) {
            int64_t ttl = GetTTLCompat(&storage, current_key, storage::DataType::kSets);
            set_expire_emitted = true;
            if (ttl > 0) {
              std::vector<std::string> argv;
              argv.push_back("EXPIRE");
              argv.push_back(current_key);
              argv.push_back(std::to_string(ttl));
              out->data_type = DataTypeName(dtype);
              out->key = current_key;
              out->raw_resp = SerializeRESP(argv);
              return true;
            }
          }
          key_loaded = false;
          continue; // next key (return false here would END THE WHOLE DUMP)
        }
        case kZSets: {
          if (!zset_loaded || (zset_loaded && zset_index >= zset_members.size())) {
            zset_members.clear();
            zset_index = 0;
            auto s = storage.ZRange(current_key, static_cast<int32_t>(zset_pos),
                                    static_cast<int32_t>(zset_pos + batch_num - 1), &zset_members);
            if (!s.ok()) {
              key_loaded = false;
              continue;
            }
            zset_loaded = true;
            if (zset_members.empty()) {
              if (!zset_expire_emitted) {
                int64_t ttl = GetTTLCompat(&storage, current_key, storage::DataType::kZSets);
                zset_expire_emitted = true;
                if (ttl > 0) {
                  std::vector<std::string> argv;
                  argv.push_back("EXPIRE");
                  argv.push_back(current_key);
                  argv.push_back(std::to_string(ttl));
                  out->data_type = DataTypeName(dtype);
                  out->key = current_key;
                  out->raw_resp = SerializeRESP(argv);
                  return true;
                }
              }
              key_loaded = false;
              continue; // next key (return false here would END THE WHOLE DUMP)
            }
          }
          if (zset_index < zset_members.size()) {
            size_t batch = std::min(static_cast<size_t>(batch_num), zset_members.size() - zset_index);
            std::vector<std::string> argv;
            argv.reserve(batch * 2 + 2);
            argv.push_back("ZADD");
            argv.push_back(current_key);
            for (size_t i = 0; i < batch; ++i) {
              const auto& sm = zset_members[zset_index + i];
              argv.push_back(std::to_string(sm.score));
              argv.push_back(sm.member);
            }
            zset_index += batch;
            if (zset_index >= zset_members.size()) {
              zset_pos += batch_num;
            }
            out->data_type = DataTypeName(dtype);
            out->key = current_key;
            out->raw_resp = SerializeRESP(argv);
            return true;
          }
          key_loaded = false;
          continue; // next key (return false here would END THE WHOLE DUMP)
        }
        default:
          stage = kDone;
          break;
      }
    }
    return false;
  }
};

static void set_error(char** err, const std::string& msg) {
  if (!err) {
    return;
  }
  *err = static_cast<char*>(malloc(msg.size() + 1));
  if (*err) {
    memcpy(*err, msg.data(), msg.size());
    (*err)[msg.size()] = '\0';
  }
}

SnapshotIter* snapshot_open(const char* dump_path, const char* db_name, SnapshotOptions opts, char** err) {
  if (!dump_path || dump_path[0] == '\0') {
    set_error(err, "dump_path required");
    return nullptr;
  }
  std::string db_root(dump_path);
  if (!db_root.empty() && db_root.back() != '/') {
    db_root.push_back('/');
  }
  if (db_name && db_name[0] != '\0') {
    std::string candidate = db_root + db_name;
    if (std::filesystem::exists(candidate)) {
      db_root = candidate;
    }
  }

  auto* it = new SnapshotIter();
  it->batch_num = opts.batch_num > 0 ? opts.batch_num : 512;
  it->scan_batch = opts.scan_batch > 0 ? opts.scan_batch : ScanBatchCount(it->batch_num);
  if (opts.scan_pattern && opts.scan_pattern[0] != '\0') {
    it->scan_pattern = opts.scan_pattern;
  }
  it->type_mask = opts.type_mask == 0 ? SNAPSHOT_TYPE_ALL : opts.type_mask;
  it->scan_strategy = (opts.scan_strategy == SNAPSHOT_SCAN_STARTKEY) ? SNAPSHOT_SCAN_STARTKEY
                                                                     : SNAPSHOT_SCAN_CURSOR;
  it->list_tail_n = opts.list_tail_n > 0 ? opts.list_tail_n : 0;
  it->db_root = db_root;
  if (opts.resume_type != nullptr && opts.resume_type[0] != '\0') {
    it->resume_type = opts.resume_type;
    it->resume_pending = true;
    if (opts.resume_key != nullptr) {
      it->resume_key = opts.resume_key;
    }
  }

  // Layout guard: pika's Storage::Open calls LOG(FATAL) (abort!) on any
  // unexpected directory, so we must pre-validate BEFORE touching it. The
  // storage layout is version-coupled: 3.5.x keeps six per-type rocksdb dirs
  // (<root>/strings/CURRENT ...), 4.0.x uses numeric kv/svc/meta instances
  // (<root>/0/...). Detect the shape and refuse mismatches with a clean
  // error instead of crashing the process.
  if (!std::filesystem::exists(db_root)) {
    set_error(err, "dump directory missing: " + db_root);
    delete it;
    return nullptr;
  }
  {
    // A 3.5 db always materializes the per-type engine dirs; if strings/ is
    // absent but numeric engines exist, this is a 4.0.x dump — wrong driver.
    if (!std::filesystem::exists(AppendPath(db_root, "strings")) &&
        std::filesystem::exists(AppendPath(db_root, "0/CURRENT"))) {
      set_error(err, "dump layout is pika 4.0.x (numeric kv/svc/meta dirs) but this driver is v35: loader picked the wrong driver");
      delete it;
      return nullptr;
    }
    // Per-engine skeleton repair: a type whose rocksdb was never written
    // exports as an EMPTY directory (no CURRENT) — pika's Storage::Open
    // LOG(FATAL)s (aborts the process) on such an engine, so bootstrap the
    // empty skeletons first. A directory that has data files but no CURRENT
    // is real corruption and still fails closed.
    static const char* kTypes[] = {"strings", "hashes", "lists", "zsets", "sets", "streams"};
    bool saw_strings = false;
    for (const char* t : kTypes) {
      const std::string dir = AppendPath(db_root, t);
      std::error_code mkec;
      std::filesystem::create_directories(dir, mkec); // absent engine -> empty skeleton below
      if (std::filesystem::exists(AppendPath(dir, "CURRENT"))) {
        if (std::string(t) == "strings") saw_strings = true;
        continue;
      }
      std::error_code ec;
      const bool empty_dir = std::filesystem::is_empty(dir, ec) && !ec;
      if (!empty_dir) {
        set_error(err, std::string("dump engine ") + t + " has files but no CURRENT (corrupt?)");
        delete it;
        return nullptr;
      }
      rocksdb::Options bo;
      bo.create_if_missing = true;
      rocksdb::DB* bdb = nullptr;
      if (!rocksdb::DB::Open(bo, dir, &bdb).ok() || bdb == nullptr) {
        set_error(err, std::string("failed to bootstrap empty dump engine ") + t);
        delete it;
        return nullptr;
      }
      delete bdb; // leaves a valid empty rocksdb skeleton (CURRENT + MANIFEST)
      if (std::string(t) == "strings") saw_strings = true;
    }
    if (!saw_strings) {
      set_error(err, "unrecognized dump layout under " + db_root);
      delete it;
      return nullptr;
    }
  }

  rocksdb::Options options;
  // NEVER create: opening a missing/empty directory must surface as an error
  // here, not silently yield an empty database (a layout bug would then
  // masquerade as "the dump had no keys").
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.keep_log_file_num = 10;
  options.max_manifest_file_size = 64 * 1024 * 1024;
  options.max_log_file_size = 512 * 1024 * 1024;
  options.write_buffer_size = 512 * 1024 * 1024;
  options.target_file_size_base = 40 * 1024 * 1024;

  it->options.options = options;
  auto status = it->storage.Open(it->options, it->db_root);
  if (!status.ok()) {
    set_error(err, status.ToString());
    delete it;
    return nullptr;
  }
  fprintf(stderr, "[snapshot] open ok db_root=%s batch=%d scan_batch=%lld pattern=%s type_mask=%u strategy=%d\n",
          it->db_root.c_str(), it->batch_num, static_cast<long long>(it->scan_batch),
          it->scan_pattern.c_str(), it->type_mask, it->scan_strategy);
  fflush(stderr);
  return it;
}

int snapshot_next(SnapshotIter* it, SnapshotRecord* out, char** err) {
  if (!it || !out) {
    set_error(err, "invalid iterator");
    return -1;
  }
  if (!it->first_next_logged) {
    fprintf(stderr, "[snapshot] first next call stage=%d cursor=%lld\n", static_cast<int>(it->stage),
            static_cast<long long>(it->cursor));
    fflush(stderr);
    it->first_next_logged = true;
  }
  RecordBuffer buf;
  std::string msg;
  bool ok = it->next_record(&buf, &msg);
  if (!msg.empty()) {
    set_error(err, msg);
    return -1;
  }
  if (!ok) {
    if (!it->eof_logged) {
      fprintf(stderr, "[snapshot] eof reached\n");
      fflush(stderr);
      it->eof_logged = true;
    }
    return 0;
  }

  memset(out, 0, sizeof(*out));
  if (!buf.data_type.empty()) {
    out->data_type_len = buf.data_type.size();
    out->data_type = static_cast<char*>(malloc(out->data_type_len));
    memcpy(out->data_type, buf.data_type.data(), out->data_type_len);
  }
  if (!buf.key.empty()) {
    out->key_len = buf.key.size();
    out->key = static_cast<char*>(malloc(out->key_len));
    memcpy(out->key, buf.key.data(), out->key_len);
  }
  if (!buf.raw_resp.empty()) {
    out->raw_resp_len = buf.raw_resp.size();
    out->raw_resp = static_cast<char*>(malloc(out->raw_resp_len));
    memcpy(out->raw_resp, buf.raw_resp.data(), out->raw_resp_len);
  }
  return 1;
}

int snapshot_next_batch(SnapshotIter* it, SnapshotRecord** out, size_t* count, int max_count, char** err) {
  if (!it || !out || !count) {
    set_error(err, "invalid iterator");
    return -1;
  }
  if (max_count <= 0) {
    max_count = 1;
  }
  if (!it->first_next_logged) {
    fprintf(stderr, "[snapshot] first next call stage=%d cursor=%lld\n", static_cast<int>(it->stage),
            static_cast<long long>(it->cursor));
    fflush(stderr);
    it->first_next_logged = true;
  }

  std::vector<RecordBuffer> buffers;
  buffers.reserve(static_cast<size_t>(max_count));
  for (int i = 0; i < max_count; i++) {
    RecordBuffer buf;
    std::string msg;
    bool ok = it->next_record(&buf, &msg);
    if (!msg.empty()) {
      set_error(err, msg);
      return -1;
    }
    if (!ok) {
      break;
    }
    buffers.push_back(std::move(buf));
  }

  if (buffers.empty()) {
    if (!it->eof_logged) {
      fprintf(stderr, "[snapshot] eof reached\n");
      fflush(stderr);
      it->eof_logged = true;
    }
    *out = nullptr;
    *count = 0;
    return 0;
  }

  auto total = buffers.size();
  auto* recs = static_cast<SnapshotRecord*>(malloc(sizeof(SnapshotRecord) * total));
  if (!recs) {
    set_error(err, "snapshot_next_batch malloc failed");
    return -1;
  }
  memset(recs, 0, sizeof(SnapshotRecord) * total);
  for (size_t i = 0; i < total; i++) {
    const auto& buf = buffers[i];
    if (!buf.data_type.empty()) {
      recs[i].data_type_len = buf.data_type.size();
      recs[i].data_type = static_cast<char*>(malloc(recs[i].data_type_len));
      memcpy(recs[i].data_type, buf.data_type.data(), recs[i].data_type_len);
    }
    if (!buf.key.empty()) {
      recs[i].key_len = buf.key.size();
      recs[i].key = static_cast<char*>(malloc(recs[i].key_len));
      memcpy(recs[i].key, buf.key.data(), recs[i].key_len);
    }
    if (!buf.raw_resp.empty()) {
      recs[i].raw_resp_len = buf.raw_resp.size();
      recs[i].raw_resp = static_cast<char*>(malloc(recs[i].raw_resp_len));
      memcpy(recs[i].raw_resp, buf.raw_resp.data(), recs[i].raw_resp_len);
    }
  }
  *out = recs;
  *count = total;
  return static_cast<int>(total);
}

void snapshot_free_record(SnapshotRecord* rec) {
  if (!rec) {
    return;
  }
  if (rec->data_type) {
    free(rec->data_type);
  }
  if (rec->key) {
    free(rec->key);
  }
  if (rec->raw_resp) {
    free(rec->raw_resp);
  }
  rec->data_type = nullptr;
  rec->key = nullptr;
  rec->raw_resp = nullptr;
  rec->data_type_len = 0;
  rec->key_len = 0;
  rec->raw_resp_len = 0;
}

void snapshot_free_batch(SnapshotRecord* recs, size_t count) {
  if (!recs) {
    return;
  }
  for (size_t i = 0; i < count; i++) {
    snapshot_free_record(&recs[i]);
  }
  free(recs);
}

void snapshot_close(SnapshotIter* it) {
  delete it;
}

extern "C" uint32_t pikawire_abi(void) { return PIKAWIRE_ABI_VERSION; }
extern "C" const char* pikawire_driver_layout(void) { return "v35"; }
