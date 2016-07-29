#include <iostream>
#include <cstring>
#include <assert.h>
#include <string>
#include <sys/time.h>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/env.h"


using rocksdb::Slice;
using rocksdb::Env;

using namespace std;

struct Node {
  std::string path;
  char meta_prefix;
  char data_prefix;

  Node(std::string &_path, char _meta_prefix, char _data_prefix)
      : path(_path), meta_prefix(_meta_prefix), data_prefix(_data_prefix) {}
};

rocksdb::Options options;
pthread_t threads[4];
int64_t num[4];
bool stale_flag;
int64_t st, ed;
int64_t total = 0;

void Usage() {
  printf ("Usage:\n"
          "  ./stale_scan nemo_db_path [all | hash | list | zset | set ]  [new | old]\n"
          "  example:\n"
          "    ./stale_scan  pika/db/ all         -- scan the stale data of all\n"
          "    ./stale_scan  pika/db/ hash,list   -- scan the stale data of hash and list\n"
          "    ./stale_scan  pika/db/ all  new    -- scan the valid data of all\n"
        );
}

const int kTSLength = 4;

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

inline uint32_t DecodeFixed32(const char* ptr) {
  uint32_t result;
  memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
  return result;
}

bool IsStale(const Slice& value, int32_t ttl, Env* env) {
  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  int32_t timestamp_value = DecodeFixed32(value.data() + value.size() - kTSLength);
  if (timestamp_value == 0) { // 0 means fresh
    return false;
  }
  return timestamp_value < curtime - ttl;
}

bool IsStale(int32_t timestamp, int32_t ttl, Env* env) {
  if (timestamp == 0) { // 0 means fresh
    return false;
  }

  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  return timestamp < curtime - ttl;
}

void FindLongSuccessor(std::string* key) {
  size_t n = key->size();
  for (size_t i = n - 1; i > 0; i--) {
    const uint8_t byte = (*key)[i];
    if (byte != static_cast<uint8_t>(0xff)) {
      (*key)[i] = byte + 1;
      key->resize(i+1);
      return;
    }
  }
}

static std::string escape(const char *p, ssize_t len) {
  std::string s = "\"";

  while(len--) {
    switch(*p) {
      case '\\':
      case '"':
        s.append(1, '\\');
        s.append(1, *p);
        break;
      case '\n':
        s.append(1, '\\');
        s.append(1, 'n');
        break;
      case '\r':
        s.append(1, '\\');
        s.append(1, 'r');
        break;
      case '\t':
        s.append(1, '\\');
        s.append(1, 't');
        break;
      case '\a':
        s.append(1, '\\');
        s.append(1, 'a');
        break;
      case '\b':
        s.append(1, '\\'); s.append(1, 'b');
        break;
      default:
        if (isprint(*p)) {
          s.append(1, *p);
        } else {
          char buf[30];
          sprintf(buf,"\\x%02x", (unsigned char)*p);
          s.append(buf);
        }
        break;
    }
    p++;
  }
  s.append(1, '"');
  return s;
}

void* ScanSpecify(void* arg) {
  Node *node = (Node *)arg;
  char kType = node->meta_prefix;
  char data_prefix = node->data_prefix; 

  int64_t* result = new int64_t;
  *result = 0;
  printf ("-> A ScanThread start: path=%s meta_prefix=%c data_prefix=%c stale_flag=%d\n", node->path.c_str(), kType, data_prefix, stale_flag);

  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, node->path, &db);
  printf ("OpenForReadOnly at %s, %s\n", node->path.c_str(), status.ToString().c_str());
  if (!status.ok()) {
    //printf ("OpenForReadOnly at %s, %s\n", path.c_str(), status.ToString().c_str());
    return NULL;
  }

  Env* env = db->GetEnv();
  rocksdb::ReadOptions iterate_options;

  const rocksdb::Snapshot* snap = db->GetSnapshot();
  assert(snap != NULL);

  iterate_options.snapshot = snap;
  iterate_options.fill_cache = false;

  int64_t meta_num = 0;
  int64_t data_num = 0;
  int64_t valid_size = 0;
  int64_t stale_size = 0;
  bool stale_meta_flag = false;
  int64_t checkpoint = 0;


  // meta iterator
  rocksdb::Iterator *meta_it = db->NewIterator(iterate_options);
  std::string meta_key_start(1, kType);
  meta_it->Seek(meta_key_start);

  // data iterator
  rocksdb::Iterator *data_it = db->NewIterator(iterate_options);

  // scan meta part
  for (; meta_it->Valid(); meta_it->Next()) {
    stale_meta_flag = false;
    Slice meta_key = meta_it->key();
    Slice meta_value = meta_it->value();
    int32_t meta_version = DecodeFixed32(meta_value.data() + meta_value.size() - 8);
    int32_t meta_timestamp = DecodeFixed32(meta_value.data() + meta_value.size() - 4);

    //printf (" meta_key=(%s) value=(%s) val_size=%lu meta_version=%d meta_ts=%d\n", meta_key.ToString().c_str(), meta_value.ToString().c_str(),
    //        meta_value.size(), meta_version, meta_timestamp);
    if (kType != meta_key[0]) {
      break;
    }

    bool is_new = !(IsStale(meta_timestamp, 0, env));

    //printf ("is_new=%d stale_flag=%d\n", is_new, stale_flag);
    // We only care the stale ones or valid ones, not both;
    // case 1: stale_flag is true, we omit the valid new data;
    // case 2: stale_flag is false, we omit the stale old data;
    if (stale_flag == is_new) {
      checkpoint++;
      meta_num++;
      if (is_new) {
        valid_size += meta_key.size() + meta_value.size();
      } else {
        stale_size += meta_key.size() + meta_value.size();
      }
      continue;
    }

    // scan the data part
    std::string key_start(1, data_prefix);
    //printf ("\nkey_start=(%s) size=%d\n", escape(key_start.data(), key_start.size()).c_str(), key_start.size());
    key_start.append(1, (uint8_t)(meta_key.size() - 1));
    //printf ("key_start=(%s) size=%d\n", escape(key_start.data(), key_start.size()).c_str(), key_start.size());
    key_start.append(meta_key.data() + 1, meta_key.size() - 1);
    //printf ("key_start=(%s) size=%d\n", escape(key_start.data(), key_start.size()).c_str(), key_start.size());
    Slice prefix(key_start);
    //std::string key_end(key_start);
    //FindLongSuccessor(&key_end);

    //printf ("--> start(%s) end(%s)\n", escape(key_start.data(), key_start.size()).c_str(), escape(key_end.data(), key_end.size()).c_str());
    data_it->Seek(key_start);
    for (; data_it->Valid(); data_it->Next()) {
      Slice data_key = data_it->key();
      Slice data_value = data_it->value();
      int32_t data_version = DecodeFixed32(data_value.data() + data_value.size() - 8);
      int32_t data_timestamp = DecodeFixed32(data_value.data() + data_value.size() - 4);
      
      //printf ("--- : data_key(%s) version(%d) timestamp(%d) value(%s)\n", data_key.ToString().c_str(),
      //          data_version, data_timestamp, data_value.ToString().c_str());
      if (!data_key.starts_with(prefix)) {
        break;
      }

      checkpoint++;
      data_num++;
      if (data_version < meta_version || IsStale(data_timestamp, 0, env)) {
        stale_meta_flag = true;
        stale_size += data_key.size() + data_value.size();
        //printf ("---- = data_key(%s) version(%d) timestamp(%d) value(%s)\n", data_key.ToString().c_str(),
        //        data_version, data_timestamp, data_value.ToString().c_str());
      } else {
        valid_size += data_key.size() + data_value.size();
        //printf ("---- + data_key(%s) version(%d) timestamp(%d) value(%s)\n", data_key.ToString().c_str(),
        //        data_version, data_timestamp, data_value.ToString().c_str());
      }

      if (checkpoint % 50000 == 0) {
        printf ("---Scanning .. : path=%s stale_size=%ld valid_size=%ld meta_num=%ld data_num=%ld\n", node->path.c_str(), stale_size, valid_size, meta_num, data_num);
      }
    }
    //printf ("<-- start(%s) end(%s)\n", escape(key_start.data(), key_start.size()).c_str(), escape(key_end.data(), key_end.size()).c_str());

    meta_num++;
    if (stale_meta_flag) {
      valid_size += meta_key.size() + meta_value.size();
    } else {
      stale_size += meta_key.size() + meta_value.size();
    }
  }


  if (stale_flag) {
    *result = stale_size;
  } else {
    *result = valid_size;
  }
  printf ("<- A ScanThread End: path=%s stale_size=%ld valid_size=%ld meta_num=%ld data_num=%ld\n", node->path.c_str(), stale_size, valid_size, meta_num, data_num);

  //char ch;
  //scanf ("%c", &ch);

  db->ReleaseSnapshot(iterate_options.snapshot);
  delete meta_it;
  delete data_it;
  delete node;
  delete db;

  return (void*)result;
}

void create_thread(int id, std::string path, char meta_prefix, char data_prefix) {
  Node *node = new Node(path, meta_prefix, data_prefix);
  int result = pthread_create(&threads[id], NULL,  &ScanSpecify, (void *)node);
  if (result != 0) {
    printf ("pthread create(%d): %s", id, strerror(result));
    return;
  }
}

void join_thread(int id) {
  void *retval;

  int ret = pthread_join(threads[id], &retval);
  if (ret != 0) {
    std::string msg = std::to_string(ret);
    printf ("pthead_join(%d) failed with %s\n", id, msg.c_str());
  } else {
    total += *((int64_t *)retval);
    printf ("pthead_join(%d) ok, num is %ld\n", id, *((int64_t *)retval));
  }
}

void Tokenize(const string& str, vector<string>& tokens, const string& delimiters = " ") {
  // Skip delimiters at beginning.
  string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  // Find first "non-delimiter".
  string::size_type pos     = str.find_first_of(delimiters, lastPos);

  while (string::npos != pos || string::npos != lastPos) {
    // Found a token, add it to the vector.
    tokens.push_back(str.substr(lastPos, pos - lastPos));
    // Skip delimiters.  Note the "not_of"
    lastPos = str.find_first_not_of(delimiters, pos);
    // Find next "non-delimiter"
    pos = str.find_first_of(delimiters, lastPos);
  }
}

int main(int argc, char **argv) {
  if (argc < 3 || argc > 5) {
    Usage();
    exit(0);
  }
  std::string path(argv[1]);
  std::string str(argv[2]);

  std::vector<std::string> types;
  Tokenize(str, types, ",");

  for (int i = 0; i < types.size(); i++) {
    if (types[i] != "all" && types[i] != "hash" && types[i] != "zset"
        && types[i] != "set" && types[i] != "list") {
      printf ("invalid type, should be all, hash, list, zset or set\n");
      Usage();
      exit(0);
    }
  }

  stale_flag = true;
  if (argc == 4 && strcmp("new", argv[3]) == 0) {
    stale_flag = false;
  }

  options.write_buffer_size = 268435456;
  options.target_file_size_base = 20971520; 

  st = NowMicros();
  printf ("Start timestamp=%ld\n", st);
  for (int i = 0; i < types.size(); i++) {
    std::string type = types[i];
    if (type == "all" || type == "hash") {
      create_thread(0, path + "/hash", 'H', 'h');
    }
    if (type == "all" || type == "zset") {
      create_thread(1, path + "/zset", 'Z', 'z');
    }
    if (type == "all" || type == "set") {
      create_thread(2, path + "/set", 'S', 's');
    }
    if (type == "all" || type == "list") {
      create_thread(3, path + "/list", 'L', 'l');
    }
  }

  for (int i = 0; i < types.size(); i++) {
    std::string type = types[i];
    if (type == "all" || type == "hash") {
      join_thread(0);
    }
    if (type == "all" || type == "zset") {
      join_thread(1);
    }
    if (type == "all" || type == "set") {
      join_thread(2);
    }
    if (type == "all" || type == "list") {
      join_thread(3);
    }
  }

  printf ("\n===================================\n"
          "Total num is %ld Bytes, in %ld us\n"
          "====================================\n",
          total, NowMicros() - st);

  //char ch;
  //scanf ("%c", &ch);

  return 0;
}
