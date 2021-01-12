#pragma once
#include<stdint.h>
#include<string>

#include "util/slice.h"
#include "util/status.h"
#include "util/crc32c.h"
namespace leveldb{

class BlockHandle{
public:
    enum { kmaxEncodedLength = 10 + 10};
    BlockHandle();
    uint64_t offset() const { return offset_;}
    void set_offset(uint64_t offset) {offset_ = offset;}

    uint64_t size() const { return size_;}
    void set_size(uint64_t size){size_ = size;}
    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input); 

private:
    uint64_t offset_;//一个block相对于sstable的位移
    uint64_t size_;//一个block的大小，该大小只是block_data的大小，不包括type字段和检验和字段(总共5字节)
};

inline BlockHandle::BlockHandle()
    :offset_(~static_cast<uint64_t>(0)),size_(~static_cast<uint64_t>(0)){}

Status BlockHandle::DecodeFrom(Slice* input){
    if(GetVarint64(input,&offset_)&& GetVarint64(input,&size_)){
        return Status::OK();
    }else{
        return Status::Corruption("bad block handle");
    }
}

void BlockHandle::EncodeTo(std::string* dst)const{
    assert(offset_ != ~static_cast<uint64_t>(0));
    assert(size_ != ~static_cast<uint64_t>(0));
    PutVarint64(dst,offset_);
    PutVarint64(dst,size_);
}
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;
static const size_t kBlockTrailerSize = 5;
/**
 * Footer组成部分
 *metaindex_handle指出了meta_index block的起始位置和大小,最大10
 *index_hanlde指出了index block的起始位置和大小 最大10
 * padding填充，保证footer的大小固定
 * Magic number
 * **/
class Footer{
public:
    enum { kEncodedLength = 2 *BlockHandle::kmaxEncodedLength+8};//footer大小固定为48个字节
    Footer()=default;
    const BlockHandle& metaindex_handle() const { return metaindex_handle_;}
    void set_metaindex_hanlde(const BlockHandle& h){metaindex_handle_=h;}
    const BlockHandle& index_handle() const { return index_handle_;}
    void set_index_hanlde(const BlockHandle& h){ index_handle_ = h;}

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);
private:
    BlockHandle metaindex_handle_;
    BlockHandle index_handle_;
};

void Footer::EncodeTo(std::string* dst)const {
    const size_t original_size = dst->size();
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    dst->resize(2 * BlockHandle::kmaxEncodedLength);//pandding
    PutFixed32(dst,static_cast<uint32_t>(kTableMagicNumber& 0xffffffffu));
    PutFixed32(dst,static_cast<uint32_t>(kTableMagicNumber>>32));
    assert(dst->size()==original_size + kEncodedLength);
    (void)original_size;
}

Status Footer::DecodeFrom(Slice* input){
    const char* magic_ptr = input->data()+kEncodedLength-8;
    const uint32_t magic_lo = DecodeFixed32(magic_ptr);
    const uint32_t magic_hi = DecodeFixed32(magic_ptr+4);
    const uint64_t magic =((static_cast<uint64_t>(magic_hi)<<32)| (static_cast<uint64_t>(magic_lo)));
    if(magic != kTableMagicNumber){
        return Status::Corruption("not an sstable(bad magic number");
    }

    Status result = metaindex_handle_.DecodeFrom(input);
    if(result.ok()){
        result = index_handle_.DecodeFrom(input);
    }
    if(result.ok()){
        const char* end = magic_ptr+8;
        *input = Slice(end,input->data()+input->size()-end);
    }
    return result;
}

struct BlockContents{
    Slice data;
    bool cachable;
    bool heap_allocated;
};

Status ReadBlock(RandomAccessFile* file,const ReadOptions& options,const BlockHandle& handle,BlockContents* result){
    //初始化result
    result->data=Slice();
    result->cachable = false;
    reuslt->heap_allocated = false;

    size_t n = static_cast<size_t>(handle.size());
    char* buf = new char[n+kBlockTrailerSize];
    Slice contents;
    Status s = file->Read(handle.offset(),n+kBlockTrailerSize,&contents,buf);
    if(!s.ok()){
        delete[] buf;
        return s;
    }
    if(contents.size()!=n+kBlockTrailerSize){
        delete[] buf;
        return Status::Corruption("truncated block read");
    }
    const char* data = contents.data();
    if(options.verify_checksums){
        const uint32_t crc = crc32c::Unmask(DecodeFixed32(data+n+1));
        const uint32_t actual = crc32c::value(data,n+1);
        if(actual != crc){
            delete[] buf;
            s = Status::Corruption("block checksum mismatch");
            return s;
        }
    }
    switch (data[n])
    {
    case kNoCompression:
        if(data!=buf){
            delete[] buf;
            result->data = Slice(data,n);
            result->heap_allocated = false;
            result->cachable = false;
        }else{
            result->data= Slice(buf,n);
            result->heap_allocated=true;
            result->cachable = true;
        }
        break;
    
    case kSnappyCompression:{
        size_t ulength = 0;
        if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
            delete[] buf;
            return Status::Corruption("corrupted compressed block contents");
        }
        char* ubuf = new char[ulength];
        if (!port::Snappy_Uncompress(data, n, ubuf)) {
            delete[] buf;
            delete[] ubuf;
            return Status::Corruption("corrupted compressed block contents");
        }
         delete[] buf;
        result->data = Slice(ubuf, ulength);
        result->heap_allocated = true;
        result->cachable = true;
        break;

    }
    default:
        delete[] buf;
        return status::Corruption("bad block type");
    }
    return Status::OK();
}

}//namespace leveldb