#pragma once

namespace leveldb{
namespace log{
enum RecordType{
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4
};
static const int kMaxRecordType = kLastType;
static const int kBlockSize = 32768;
static const int kHeaderSize = 4 + 2 + 1;//4字节checksum,2字节长度，1字节type
    
} // namespace log
    
} // namespace leveldb
