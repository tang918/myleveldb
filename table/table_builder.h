#pragma once
#include<stdint.h>
#include "util/options.h"
#include "util/status.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "table/block_builder.h"
#include "util/options.h"
#include "table/format.h"
#include "util/comparator.h"

namespace leveldb{
    
class BlockBuilder;
class BlockHandle;
class WritableFile;
class TableBuilder{
public:
    TableBuilder(const Options& options,WritableFile* file);
    TableBuilder(const TableBuilder&)=delete;
    TableBuilder& operator=(const TableBuilder&)=delete;
    ~TableBuilder();

    Status ChangeOptions(const Options& options);
    void Add(const Slice& key,const Slice& value);
    void Flush();
    Status Finish();
    Status status() const;
    void Abandon();
    uint64_t NumEntries() const;
    uint64_t FileSize() const;
private:
    bool ok() const { return status().ok();}
    void WriteBlock(BlockBuilder* block,BlockHandle* handle);
    void WriteRawBlock(const Slice& data,CompressionType,BlockHandle* handle);

    struct Rep;
    Rep* rep_;
};

struct TableBuilder::Rep{
    Rep(const Options& opt,WritableFile* f):
        Options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy==nullptr?nullptr:new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false){
            index_block_options.block_restart_interval=1;
        }
    
    Options options;//data block的选项
    Options index_block_options; //index block的选项
    WritableFile* file;//sstale文件
    uint64_t offset;//要写入data block在sstable文件中的偏移，初始0
    Status status; //当前状态-初始ok
    BlockBuilder data_block; //当前操作的data block
    BlockBuilder index_block;//sstable的index block
    std::string last_key; //当前data block最后的k/v对的key
    int64_t num_entries; //当前data block的个数，初始0
    bool closed;//调用了Finsh() or Abandon(),初始false
    FilterBlockBuilder* filter_block;//根据filter数据快速定位key是否在block中
    bool pending_index_entry;//见下面的Add函数，初始false
    BlockHandle pending_handle;//添加到index block的data block的信息
    std::string compressed_output;//压缩后的data block,临时存储，写入后即被清空
};
TableBuilder::TableBuilder(const Options& options,WritableFile* file)
    : rep_(new Rep(options,file)){
    if(rep_->filter_block != nullptr){
          rep_->filter_block->StartBlock(0);
    }
}
TableBuilder::~TableBuilder(){
    assert(rep_->closed);
    delete rep_->filter_block;
    delete rep_;
}
Status TableBuilder::ChangeOptions(const Options& options){
    if(options.comparator != rep_->options.comparator){
        return Status::InvalidArgument("changing comparator while building table");
    }
    rep_->options = options;
    rep_->index_block_options = options;
    rep_->index_block_options.block_restart_interval = 1;
    return Status::OK();
}

void TableBuilder::Add(const Slice& key,const Slice& value){
    Rep* r= rep_;
    assert(!r->closed);
    if(!ok()) return;
    if(r->num_entries>0){
        assert(r->options.comparator->Compare(key,Slice(r->last_key))>0);
    }
    //1.如果标记r->pending_index_entry为true,表明遇到下一个data block的第一个k/v,根据key调整r->last_key,这是通过Comparator的
    //FindShortestSeparator完成的
    if(r->pending_index_entry){
        assert(r->data_block.empty());
        r->options.comparator->FindShortestSeparator(& r->last_key,key);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding);
        r->index_block.Add(r->last_key,Slice(handle_encoding));
        r->pending_index_entry = false;
    }
    if(r->filter_block != nullptr){
        r->filter_block->AddKey(key);
    }

    //设置r->last_key = key,将(key,value)添加到r->data_block中，并更新entry数。
    r->last_key.assign(key.data(),key.size());
    r->num_entries++;
    r->data_block.Add(key,value);
    //如果数据块大小已达上限，写入文件
    const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
    if(estimated_block_size>=r->options.block_size){
        Flush();
    }
}

void TableBuilder::Flush(){
    Rep* r=rep_;
    assert(!r->closed);
    if(!ok())return;
    if(r->data_block.empty())return;
    assert(!r->pending_index_entry);//保证pending_index_entry为false,即data block的Add已经完成
    WriteBlock(&r->data_block,&r->pending_handle);
    if(ok()){
        r->pending_index_entry = true;
        r->status = r->file->Flush();
    }
    if(r->filter_block!=nullptr){
        r->filter_block->StartBlock(r->offset);//将data_block在sstable中的偏移加入到filter block中，并指明开始新的data block
    }
}

void TableBuilder::WriteBlock(BlockBuilder* block,BlockHandle* handle){
    assert(ok());
    Rep* r = rep_;
    Slice raw = block->Finish();
    Slice block_contents;
    CompressionType type = r->options.compression;
    switch(type){
        case kNoCompression:
            block_contents = raw;
            break;
        //采用Snappy压缩，Snappy是谷歌开源的压缩库
        case kSnappyCompression:{
           std::string* compressed = &r->compressed_output;
           if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) {
                block_contents = *compressed;
            } else {
                // Snappy not supported, or compressed(压缩比) less than 12.5%, so just
                // store uncompressed form
                block_contents = raw;
                type = kNoCompression;
            }
            break; 
        }
    }
    WriteRawBlock(block_contents,type,handle);
    r->compressed_output.clear();
    block->Reset();
}
void TableBuilder::WriteRawBlock(const Slice& block_contents,CompressionType type,BlockHandle* handle){
    Rep* r=rep_;
    handle->set_offset(r->offset);
    handle->set_size(block_contents.size());
    r->status = r->file->Append(block_contents);
    if(r->status.ok()){
        char trailer[kBlockTrailerSize];
        trailer[0]=type;
        uint32_t crc = crc32c::Value(block_contents.data(),block_contents.size());
        crc = crc32c::Extend(crc,trailer,1);
        EncodeFixed32(trailer+1,crc32c::Mask(crc));
        //向block_data末尾加上type和校验和，这样就构成一个完成的Block
        r->status = r->file->Append(Slice(trailer,kBlockTrailerSize));
        if(r->status.ok()){
            //新的block的偏移量应该加上type和校验和
            r->offset += block_contents.size()+kBlockTrailerSize;
        }
    }
}
//调用Finish函数，表明调用者将所有已经添加的K/V对持久化到sstable,并关闭sstable文件。
Status TableBuilder::Finish(){
    
    //1.首先调用Flush,写入最后一块data block,然后设置关闭标志closed=true,表明该sstable已经关闭，不能再添加k/v对
    Rep* r = rep_;
    Flush();
    assert(!r->closed);
    r->closed = true;
    BlockHandle filter_block_handle,metaindex_block_handle,index_block_handle;
    //2.写入filter block到文件中
    if(ok() && r->filter_block != NULL){
        //将filter_block的offset和size写入filter_block_handle中，并将filter_block写入sstable中
        WriteRawBlock(r->filter_block->Finish(),kNoCompression,&filter_block_handle);
    }
    //3.写入metaindex block
    if(ok()){
        BlockBuilder meta_index_block(&r->options);
        if(r->filter_block!=nullptr){
            std::string key = "filter.";
            key.append(r->options.filter_policy->Name());
            std::string handle_encoding;
            filter_block_handle.EncodeTo(&handle_encoding);//将filter_block的offset和size编码到handle_encoding中
            meta_index_block.Add(key,handle_encoding);//写入filter_block的信息
        }
        WriteBlock(&meta_index_block,&metaindex_block_handle);//将其他meta block写入文件，并将meta_block的offset和size写入metaindex_block_handle
    }
    //4.写入index block,如果成功Flush过data block,那么需要维最后一块data block设置index block,并加入到index block中
    if(ok()){
        if(r->pending_index_entry){
            r->options.comparator->FindShortSuccessor(&r->last_key);
            std::string handle_encoding;
            r->pending_handle.EncodeTo(&handle_encoding);
            r->index_block.Add(r->last_key,Slice(handle_encoding));
            r->pending_index_entry = false;
        }
        //在向sstable写入index_block时，将index_block的offset和size写入index_block_handle中
        WriteBlock(&r->index_block,&index_block_handle);
    }
    //5.写入Footer
    if(ok()){
        Footer footer;
        footer.set_metaindex_hanlde(metaindex_block_handle);
        footer.set_index_hanlde(index_block_handle);
        std::string footer_encoding;
        footer.EncodeTo(&footer_encoding);
        r->status = r->file->Append(footer_encoding);
        if(r->status.ok()){
            r->offset += footer_encoding.size();
        }
    }
    return r->status;
}

Status TableBuilder::status() const { return rep_->status;}
void TableBuilder::Abandon(){
    Rep* r= rep_;
    assert(!r->closed);
    r->closed = true;
}
uint64_t TableBuilder::NumEntries() const { return rep_->num_entries;}
uint64_t TableBuilder::FileSize() const { return rep_->offset;}
} // namespace leveldb
