#pragma once

#include "db/compaction/compaction_iterator.h"

namespace ROCKSDB_NAMESPACE {

// remaps sequence numbers onto the key
class RemapIterator {
 private:
  CompactionIterator* it;
  char* mapped_key_buffer;
  SequenceNumber start_seq;
  tkrzw::PolyDBM *_keydb;

 public:
  RemapIterator(CompactionIterator* in, SequenceNumber seq, tkrzw::PolyDBM *keydb) : it(in), mapped_key_buffer(new char[9 + 8]), start_seq(seq), _keydb(keydb) {}

  bool Valid() const { return it->Valid(); }
  void Next() { it->Next(); }
   Slice key()  {
    // std::cerr << "key: " << key.ToString(true) << std::endl;
    // perhaps memcpy is faster? last 8 bytes of internal key is packed
    ParsedInternalKey ikey = it->ikey();
    char* p = EncodeVarint64(mapped_key_buffer, start_seq++); // p is the new start of the buffer
    uint64_t packed = PackSequenceAndType(ikey.sequence, ikey.type);
    EncodeFixed64(p, packed);
    Slice mapped_key = Slice(mapped_key_buffer, p - mapped_key_buffer + 8);
    // std::cerr << "mapped key: " << mapped_key.ToString(true) << std::endl;
    // save mapped key to keydb
    // std::cerr << "Saving mapping: " << ikey.user_key.ToString(true) << " -> " << mapped_key.ToString(true) << std::endl;
    // std::cerr << "Key size: " << ikey.user_key.size() << std::endl;
    _keydb->Set(ikey.user_key.ToStringView(), Slice(mapped_key_buffer, p - mapped_key_buffer).ToStringView());
    return mapped_key;
  }
  Slice value() const { return it->value(); }
};

}  // namespace ROCKSDB_NAMESPACE