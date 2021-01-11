#pragma once
#include<stdint.h>
#include "db/log_format.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb{

class WritableFile;

namespace log{
class Writer{
public:
    explicit Writer(WritableFile* dest);

    Writer(WritableFile* dest,uint64_t dest_length);

    Writer(const Writer&)=delete;
    Writer& operator=(const Writer&)=delete;
    ~Writer();
    Status AddRecord(const Slice& slice);

private:
    Status EmitPhysicalRecord(RecordType type,const char* ptr,size_t length);
    WritableFile* dest_;
    int block_offset_;

    uint32_t type_crc_[kMaxRecordType + 1];
};

static void InitTypeCrc(uint32_t* type_crc){
    for(int i=0;i<kMaxRecordType;i++){
        char t = static_cast<char>(i);
        type_crc[i]=crc32c::Value(&t,1);
    }
}
Writer::Writer(WritableFile* dest):dest_(dest),block_offset_(0){
    InitTypeCrc(type_crc_);
}

Writer::~Writer()=default;

Status Writer::AddRecord(const Slice& slice){
    const char* ptr=slice.data();
    size_t left = slice.size();
    Status s;
    bool begin = true;
    do{
        const int leftover = kBlockSize - block_offset_;
        assert(leftover>=0);
        if(leftover<kHeaderSize){
            //转向一个新的block
            if(leftover>0){
                static_assert(kHeaderSize==7,"");
                dest_->Append(Slice("\x00\x00\x00\x00\x00",leftover));
            }
            block_offset_=0;//启用一个新的块
        }
        assert(kBlockSize-block_offset_-kHeaderSize>=0);
        const size_t avail = kBlockSize-block_offset_-kHeaderSize;
        const size_t fragment_length = (left<avail) ? left:avail;

        RecordType type;
        const bool end=(left==fragment_length);
        if(begin&& end){
            type = kFullType;
        }else if(begin){
            type = kFirstType;
        }else if(end){
            type = kLastType;
        }else{
            type = kMiddleType;
        }
        s = EmitPhysicalRecord(type,ptr,fragment_length);
        ptr += fragment_length;
        left -= fragment_length;
        begin = false;
    }while(s.ok()&& left>0);
    return s;
}

Status Writer::EmitPhysicalRecord(RecordType t,const char*ptr,size_t length){
    assert(length<=0xffff);
    assert(block_offset_+ kHeaderSize+length<=kBlockSize);
    char buf[kHeaderSize];
    buf[4]=static_cast<char>(length& 0xff);
    buf[5]=static_cast<char>(length>>8);
    buf[6] = static_cast<char>(t);
    uint32_t crc = crc32c::Extend(type_crc_[t],ptr,length);
    crc = crc32c::Mask(crc);
    EncodeFixed32(buf,crc);
    Status s = dest->Append(Slice(buf,kHeaderSize));
    if(s.ok()){
        s = dest->Append(Slice(ptr,length));
        if(s.ok()){
            s = dest_->Flush();
        }
    }
    block_offset_ += kHeaderSize + length;
    return s;
}
    
} // namespace log
} // namespace leveldb
