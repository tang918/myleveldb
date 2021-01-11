#pragma once
#include<stdint.h>
#include"db/log_format.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb{
class SequentialFile;
namespace log{

class Reader{
public:
    class Reporter{
     public:
        virtual ~Reporter();
        virtual void Corruption(size_t bytes,const Status& status)=0;
    };

    Reader(SequentialFile* file,Reporter* reporter,bool checksum,uint64_t initial_offset);
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;
    ~Reader();

    bool ReadRecord(Slice* record,std::string* scratch);

    uint64_t LastRecordOffset();

private:
    enum{
        kEof = kMaxRecordType + 1,
        kBadRecord = kMaxRecordType +2
    };
    bool SkipToInitialBlock();
    unsigned int ReadPhysicalRecord(Slice* result);
    void ReportCorruption(uint64_t bytes,const char* reason);
    void ReportDrop(uint64_t bytes,const Status& reason);

    SequentialFile* const file_;
    Reporter* const reporter_;//数据损坏报告
    bool const checksum_;//是否进行数据校验
    char* const backing_store_;//read以Block为单位从磁盘读取数据，取完数据就是存在blocking_store_里面。相当于读数据的buffer
    Slice buffer_;
    bool eof_;
    uint64_t last_record_offset_;//上一条记录的偏移
    uint64_t end_of_buffer_offset_;//当前Block的结束位置的偏移
    uint64_t const initial_offset_;//初始offset,从该偏移查找第一条记录

    bool resyncing_;
}; 

Reader::Reporter::~Reporter()= default;
Reader::Reader(SequentialFile* file,Reporter* reporter,bool checksum,uint64_t initial_offset):
    file_(file),reporter_(reporter),checksum_(checksum),
    backing_store_(new char[kBlockSize]),
    buffer_(),
    eof_(false),
    last_record_offset_(0),
    end_of_buffer_offset_(0),
    initial_offset_(initial_offset),
    resyncing_(initial_offset>0){}

Reader::~Reader() { delete[] backing_store_;}

bool Reader::SkipToInitialBlock(){
    const size_t offset_in_block = initial_offset_ % kBlockSize;
    uint64_t block_start_location = initial_offset_ - offset_in_block;

    if(offset_in_block > kBlockSize-6){
        block_start_location += kBlockSize;
    }
    end_of_buffer_offset_ = block_start_location;
    if(block_start_location>0){
        Status skip_status = file->Skip(block_start_location);
        if(!skip_status.ok()){
            ReportDrop(block_start_location,skip_status);
            return false;
        }
    }
    return true;
}

bool Reader::ReadRecord(Slice* record,std::string* scratch){
    if(last_record_offset_ < initial_offset_){
        if(!SkipToInitialBlock()){
            return false;
        }
    }

    scratch->clear();
    record->clear();
    bool in_fragmented_record = false;

    uint64_t prospective_record_offset = 0;
    Slice fragment;
    while(true){
        const unsigned int record_type = ReadPhysicalRecord(&fragment);
        uint64_t  physical_record_offset = end_of_buffer_offset_ - buffer_.size()-kHeaderSize-fragment.size();
        if(resyncing_){
            if(record_type==kMiddleType){
                continue;
            }else if(record_type==kLastType){
                resyncing_=false;
                continue;
            }else{
                resyncing_ = false;
            }
        }
        switch(record_type){
            case kFullType:
                if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
                    if (!scratch->empty()) {
                        ReportCorruption(scratch->size(), "partial record without end(1)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->clear();
                *record = fragment;
                last_record_offset_ = prospective_record_offset;
                return true;

            case kFirstType:
                if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
                    if (!scratch->empty()) {
                        ReportCorruption(scratch->size(), "partial record without end(1)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->assign(fragment.data(),fragment.size());
                in_fragmented_record = true;
                break;
            case kMiddleType:
                if(!in_fragmented_record){
                    ReportCorruption(fragment.size(),"missing start of fragmented record(1)");
                }else{
                    scratch->append(fragment.data(),fragment.size());
                }
                break;
            case kLastType:
                if(!in_fragmented_record){
                    ReportCorruption(fragment.size(),"missing start of fragemented record(2)");
                }else{
                    scratch->append(fragment.data(),fragment.size());
                    *record=Slice(*scratch);
                    last_record_offset_ = prospective_record_offset;
                    return true;
                }
                break;

            case kEof:
                if(in_fragmented_record){
                    scratch->clear();
                }
                return false;
            case kBadRecord:
                if(in_fragmented_record){
                    ReportCorruption(scratch->size(),"error in middle of record");
                    in_fragmented_record = false;
                    scratch->clear();
                }
                break;
            default:{
                char buf[40];
                snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
                ReportCorruption(
                    (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
                    buf);
                in_fragmented_record = false;
                scratch->clear();
                break;
            }
        }
    }
    return false;
}

uint64_t Reader::LastRecordOffset(){return last_record_offset_;}
void Reader::ReportCorruption(uint64_t bytes,const char* reason){
   ReportDrop(bytes,Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result){
    while(true){
            //两种情况下该条件成立
            //1.出现哎第一次read,因为buffer_在reader的构造函数里面是初始化为空
            //2.当前buffer_的内容为Block尾部的6个空字符，这时实际上当前Block已经解析完了，准备解析下一个Block
        if(buffer_.size() < kHeaderSize){
            if(!eof_){
                buffer_.clear();
                Status status = file_->Read(kBlockSize,&buffer_,backing_store_);
                end_of_buffer_offset_ += buffer_.size();
                if(!status.ok()){
                    buffer_.clear();
                    ReportDrop(kBlockSize,status);
                    eof_ = true;
                    return kEof;
                }else if(buffer_.size()<kBlockSize){
                    eof_ = true;
                }
                continue;
            }else{
                buffer_.clear();
                return kEof;
            }
        }
        const char* header = buffer_.data();
        const uint32_t a=static_cast<uint32_t>(header[4] & 0xff);
        const uint32_t b = static_cast<uint32_t>(header[5]& 0xff);
        const unsigned int type = header[6];
        const uint32_t length =  a |(b << 8);
        if(kHeaderSize+length>buffer_.size()){
            size_t drop_size = buffer_.size();
            buffer_.clear();
            if(!eof_){
                ReportCorruption(drop_size,"bad record length");
                return kBadRecord;
            }
            return kEof;
        }
        if(type==kZeroType && length==0){
            buffer_.clear();
            return kBadRecord;
        }

        if(checksum_){
            uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
            uint32_t actual_crc = crc32c::Value(header+6,1+length);
            if(actual_crc != expected_crc){
                size_t drop_size = buffer_.size();
                buffer_.clear();
                ReportCorruption(drop_size,"checksum mismatch");
                return kBadRecord;
            }
        }

        buffer_.remove_prefix(kHeaderSize+length);
        if(end_of_buffer_offset_-buffer_.size()-kHeaderSize-length<initial_offset_){
            result->clear();
            return kBadRecord;
        }

        *result = Slice(header+kHeaderSize,length);
        return type;
    }
}

} // namespace log
} // namespace leveldb
