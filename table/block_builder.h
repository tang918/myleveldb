#pragma once
#include<stdint.h>
#include<vector>
#include "util/slice.h"
#include "util/comparator.h"
#include "util/options.h"
#include "util/coding.h"
#include <algorithm>
namespace leveldb
{

struct Options;
//构建Block_data数据块，一个Block_data块包括kv数据，重启点数组，重启点数量
class BlockBuilder{
public:
    explicit BlockBuilder(const Options* options);
    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder& operator=(const BlockBuilder&)=delete;

    void Reset();
    void Add(const Slice& key,const Slice& value);
    Slice Finish();
    size_t CurrentSizeEstimate() const;
    bool  empty() const { return buffer_.empty();}
private:
    const Options* options_;
    std::string buffer_;//代表当前数据块
    std::vector<uint32_t>restarts_;
    int counter_;
    bool finished_;
    std::string last_key_;//记录最后添加的key
};

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),restarts_(),counter_(0),finished_(false){
        assert(options->block_restart_interval>=1);
        restarts_.push_back(0);
}

void BlockBuilder::Reset(){
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    counter_ = 0;
    finished_ = false;
    last_key_.clear();
}
size_t BlockBuilder::CurrentSizeEstimate() const {
    return (buffer_.size() + restarts_.size()*sizeof(uint32_t)+sizeof(uint32_t));
}

//调用该函数完成Block_data构建
Slice BlockBuilder::Finish(){
    //加入重启点信息
    for(size_t i=0;i<restarts_.size();i++){
        PutFixed32(&buffer_,restarts_[i]);
    }
    //加入重启点个数
    PutFixed32(&buffer_,restarts_.size());
    finished_ = true;
    return Slice(buffer_);
}

void BlockBuilder::Add(const Slice&key,const Slice&value){
    Slice last_key_piece(last_key_);
    assert(!finished_);
    assert(counter_<= options_->block_restart_interval);
    //保证新加入的key>已经加入的任何一个key
    assert(buffer_.empty() || options_->comparator->Compare(key,last_key_piece)>0);
    //如果计数器counter_<options->block_restart_interval,则使用前缀算法压缩key,否则就把key作为一个重启点，无压缩存储
    size_t shared=0;
    if(counter_<options_->block_restart_interval){
        const size_t min_length = std::min(last_key_piece.size(),key.size());
        while((shared<min_length)&&(last_key_piece[shared]==key[shared])) shared++;
    }else{
        restarts_.push_back(buffer_.size());
        counter_=0;
    }
    const size_t non_shared = key.size()-shared;
    //数据存储格式:shared_bytes|unshared_bytes|value_length|key_delta|value
    PutVarint32(&buffer_,shared);//采用的append方法，故buffer_位置始终未变
    PutVarint32(&buffer_,non_shared);
    PutVarint32(&buffer_,value.size());
    buffer_.append(key.data()+shared,non_shared);
    buffer_.append(value.data(),value.size());
    last_key_.resize(shared);
    last_key_.append(key.data()+shared,non_shared);
    assert(Slice(last_key_)==key);
    counter_++;


}

} // namespace laveldb
