#pragma once
#include <stdint.h>
#include "table/iterator.h"

#include "util/cache.h"
#include "util/comparator.h"
#include "util/options.h"
#include "util/coding.h"
#include "table/block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"

namespace leveldb{
  
class Table{
public:
    static Status Open(const Options& options,RandomAccessFile* file,uint64_t file_size,Table** table);

    Table(const Table&)=delete;
    Table& operator=(const Table&)=delete;
    ~Table();
    Iterator* NewIterator(const ReadOptions&) const;
    uint64_t ApproximateOffsetOf(const Slice& key)const;
private:
    friend class TableCache;
    struct Rep;
    static Iterator* BlockReader(void*,const ReadOptions&,const Slice&);
    explicit Table(Rep* rep):rep_(rep){}
    Status InternalGet(const ReadOptions&,const Slice& key,void* arg,void(*handle_result)(void* arg,const Slice&k,const Slice& v));
    void ReadMeta(const Footer& footer);
    void ReadFilter(const Slice& filter_handle_value);
    Rep* const rep_;
};

struct Table::Rep{
    ~Rep(){
        delete filter;
        delete[] filter_data;
        delete index_block;
    }
    Options options;
    Status status;
    RandomAccessFile* file;
    uint64_t cache_id;
    FilterBlockReader* filter;
    const char* filter_data;
    BlockHandle metaindex_handle;
    Block* index_block;
};

//打开一个sstable文件
//file为要打开的文件，size为要打开的文件的大小，若操作成功，table指向新打开的表，否则返回错误
Status Table::Open(const Options& options,RandomAccessFile* file,uint64_t size,Table** table){
    *table=nullptr;

    //从文件末尾读取footer
    if(size<Footer::kEncodedLength){
        //文件太短
        return Status::Corruption("file is too short to be an sstable");
    }
    char footer_space[Footer::kEncodedLength];
    Slice footer_input;
    Status s = file->Read(size-Footer::kEncodedLength,Footer::kEncodedLength,&footer_input,footer_space);
    if(!s.ok())return s;
    Footer footer;
    s = footer.DecodeFrom(&footer_input);
    if(!s.ok())return s;
    //读取index block
    BlockContents index_block_contens;
    if(s.ok()){
        ReadOptions opt;
        if(options.paranoid_checks){
            opt.verify_checksums = true;
        }
        s = ReadBlock(file,opt,footer.index_handle(),&index_block_contens);
    }
    if(s.ok()){
        //已成功读取footer和index_block,可以响应请求了
        Block* index_block = new Block(index_block_contens);
        Rep* rep = new Table::Rep;
        rep->options = options;
        rep->file = file;
        rep->metaindex_handle = footer.metaindex_handle();
        rep->index_block = index_block;
        rep->cache_id = (options.block_cache? options.block_cache->NewId():0);
        rep->filter_data = nullptr;
        rep->filter = nullptr;
        *table = new Table(rep);
        (*table)->ReadMeta(footer);
    }
    return s;
}

void Table::ReadMeta(const Footer& footer){
    if(rep_->options.filter_policy==nullptr){
        return;
    }
    ReadOptions opt;
    if(rep_->options.paranoid_checks){
        opt.verify_checksums=true;
    }
    BlockContents contents;
    if(!ReadBlock(rep_->file,opt,footer.metaindex_handle(),&contents).ok()){
        return;
    }
    Block* meta = new Block(contents);
    Iterator* iter = meta->NewIterator(BytewiseComparator());
    std::string key = "filter.";
    key.append(rep_->options.filter_policy->Name());
    iter->Seek(key);
    if(iter->Valid() && iter->key()==Slice(key)){
        ReadFilter(iter->value());
    }
    delete iter;
    delete meta;
}
void Table::ReadFilter(const Slice& filter_handle_value){
    Slice v = filter_handle_value;
    BlockHandle filter_handle;
    if(!filter_handle.DecodeFrom(&v).ok()){
        return;
    }
    ReadOptions opt;
    if (rep_->options.paranoid_checks) {
        opt.verify_checksums = true;
    }
    BlockContents block;
    if(!ReadBlock(rep_->file,opt,filter_handle,&block).ok()){
        return;
    }
    if(block.heap_allocated){
        rep_->filter_data = block.data.data();
    }
    rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}
Iterator* Table::NewIterator(const ReadOptions& options) const{
    return NewTwoLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator),  
                                &Table::BlockReader,const_cast<Table*>(this), options);  

}

Iterator* Table::BlockReader(void* arg,const ReadOptions& options,const Slice& index_value){
    Table* table = reinterpret_cast<Table*>(arg);
    Block* block=nullptr;
    Cache* block_cache = table->rep_->options.block_cache;
    Cache::Handle* cache_handle = nullptr;
    BlockHandle handle;
    Slice input = index_value;
    Status s= handle.DecodeFrom(&input);//解析出block在sstable中的偏移和大小
    if(s.ok()){
        BlockContents contents;
        if(block_cache != nullptr){
            char cache_key_buffer[16];
            EncodeFixed64(cache_key_buffer,table->rep_->cache_id);
            EncodeFixed64(cache_key_buffer+8,handle.offset());
            //根据block的大小和cache_id组成key，来查找block在LRU中的位置
            Slice key(cache_key_buffer,sizeof(cache_key_buffer));
            cache_handle = block_cache->Lookup(key);
            if(cache_handle!=nullptr){
                block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
            }else{
                s = ReadBlock(table->rep_->file, options, handle, &contents);
                if(s.ok()){
                    block = new Block(contents);
                    if(contents.cachable&& options.fill_cache)
                    //尝试加到cache中
                        cache_handle = block_cache->Insert(key,block,block->size(),&DeleteCachedBlock);
                }
            }
        }else{
            s = ReadBlock(table->rep_->file, options, handle, &contents);
            if (s.ok()) {
                block = new Block(contents);
            }
        }
    }
    Iterator* iter;
    if(block != nullptr){
        iter = block->NewIterator(table->rep_->options.comparator);
        if(cache_handle==nullptr){
            iter->RegsiterCleanup(&DeleteBlock,block,nullptr);
        }else{
           iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
        }
    }else{
        iter = NewErrorIterator(s);
    }
    return iter;
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const{
    Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
    index_iter->Seek(key);
    uint64_t result;
    if(index_iter->Valid()){
        BlockHandle handle;
        Slice input = index_iter->value();
        Status s = handle.DecodeFrom(&input);
        if(s.ok()){
            result = handle.offset();//找到第一个大于等于key得block的偏移量
        }else{
            result = rep_->metaindex_handle.offset();
        }
    }else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

Status Table::InternalGet(const ReadOptions& options,const Slice& k,void* arg,void(*handle_result)(void*,const Slice&,const Slice&)){
    Status s;
    Iterator* iter = rep_->index_block->NewIterator(rep_->options.comparator);
    iter->Seek(k);
    if(iter->Valid()){
        Slice hanlde_value = iter->value();
        FilterBlockReader* filter = rep->filter;
        BlockHandle handle;
        if(filter!=nullptr && handle.DecodeFrom(&handle_value).ok()&& !filter->KeyNayMatch(handle.offset(),k)){

        }else{
             Iterator* block_iter = BlockReader(this, options, iiter->value());
            block_iter->Seek(k);
            if (block_iter->Valid()) {
                (*handle_result)(arg, block_iter->key(), block_iter->value());
            }
            s = block_iter->status();
            delete block_iter;
        }
    }
    if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

} // namespace leveldb
