#pragma once
#include "table/iterator.h"
#include "table/table.h"
#include "table/block.h"
#include "table/format.h"
#include  "table/filter_block.h"
#include  "util/coding.h"
#include "table/iterator_wrapper.h"
namespace leveldb{
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(void* arg,const ReadOptions& options,const Slice& index_value),
    void* arg,const ReadOptions& options);

typedef Iterator* (*BlockFunction)(void*,const ReadOptions&,const Slice&);

class TwoLevelIterator:public Iterator{
public:
    TwoLevelIterator(Iterator* index_iter,BlockFunction block_function,void* arg,const ReadOptions& options);
    ~TwoLevelIterator() override=default;
    void Seek(const Slice& target) override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Next() override;
    void Prev() override;
    bool Valid() const override{ return data_iter_.Valid();}

    Slice key() const override{
        assert(Valid());
        return data_iter_.key();
    }
    Slice value() const override{
        assert(Valid());
        return data_iter_.value();
    }
    Status status() const override{
        if (!index_iter_.status().ok()) {
        return index_iter_.status();
        } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
            return data_iter_.status();
        } else {
            return status_;
        }
    }

private:
    void SaveError(const Status& s){
        if (status_.ok() && !s.ok()) status_ = s;
    }
    void SkipEmptyDataBlocksForward();
    void SkipEmptyDataBlocksBackward();
    void SetDataIterator(Iterator* data_iter);
    void InitDataBlock();
    BlockFunction block_function_; //block操作函数
    void* arg_; //BlockFunction的自定义参数
    const ReadOptions options_;//BlockFunction的read option参数
    Status status_;//当前状态
    IteratorWrapper index_iter_;//遍历block的迭代器
    IteratorWrapper data_iter_;//遍历block data的迭代器
    //如果data_iter_!=null,data_block_handle_保存的是传递给block_function的index value,以用来创建data_iter
    std::string data_block_handle_;

};
TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,BlockFunction block_function,void* arg,const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr){}


void TwoLevelIterator::Seek(const Slice& target){
    index_iter_.Seek(target);
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
    SkipEmptyDataBlocksForward();
}
void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward(){
    while(data_iter_.iter()==nullptr || data_iter_.Valid()){
        if(!index_iter_.Valid()){
            SetDataIterator(nullptr);
            return;
        }
        index_iter_.Next();
        InitDataBlock();
        if(data_iter_.iter()!=nullptr) data_iter_.SeekToFirst();
    }
}
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter){
    if(data_iter_.iter() !=nullptr)SaveError(data_iter_.status());
    data_iter_.Set(data_iter);
}
void TwoLevelIterator::InitDataBlock(){
    if(!index_iter_.Valid()){
        SetDataIterator(nullptr);
    }else{
        Slice handle = index_iter_.value();
        if(data_iter_.iter() != nullptr && handle.compare(data_block_handle_)==0){

        }else{
            Iterator* iter = (*block_function_)(arg_,options_,handle);
            data_block_handle_.assign(handle.data(),handle.size());
            SetDataIterator(iter);

        }
    }

}

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

    
} // namespace leveldb;
