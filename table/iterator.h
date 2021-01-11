#pragma once
#include "util/slice.h"
#include "util/status.h"
namespace leveldb
{
class Iterator{
public:
    Iterator();
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&)=delete;
    virtual ~Iterator();
    virtual bool Valid() const=0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice& target)=0;
    virtual void Next()=0;
    virtual void Prev()=0;
    virtual Slice key() const=0;
    virtual Slice value() const = 0;
    virtual Status status() const=0;
    using CleanupFunction = void(*)(void* arg1,void* arg2);
    void RegisterCleanup(CleanupFunction function,void* arg1,void* arg2);
private:
    struct CleanupNode{
        bool IsEmpty() const { return function==nullptr;}

    void Run(){
        assert(function != nullptr);
        (*function)(arg1,arg2);
    }
        CleanupFunction function;
        void* arg1;
        void* arg2;
        CleanupNode* next;
    };
    CleanupNode cleanup_head_;
};
Iterator* NewEmptyIterator();
Iterator* NewErrorIterator(const Status& status);

Iterator::Iterator(){
    cleanup_head_.function=nullptr;
    cleanup_head_.next = nullptr;
}
Iterator::~Iterator(){
    if(!cleanup_head_.IsEmpty()){
        cleanup_head_.Run();
        for(CleanupNode* node =cleanup_head_.next;node!=nullptr;){
            node->Run();
            CleanupNode* next_node = node->next;
            delete node;
            node = next_node;
        }
    }
}

void Iterator::RegisterCleanup(CleanupFunction func,void* arg1,void* arg2){
    assert(func != nullptr);
    CleanupNode* node;
    if(cleanup_head_.IsEmpty()){
        node = &cleanup_head_;
    }else{
        node = new CleanupNode();
        node->next = cleanup_head_.next;
        cleanup_head_.next = node;
    }
    node->function = func;
    node->arg1 = arg1;
    node->arg2 = arg2;
}

namespace{
class EmptyIterator:public Iterator{
public:
    EmptyIterator(const Status& s): status_(s){}
    ~EmptyIterator() override=default;

    bool Valid() const override { return false; }
    void Seek(const Slice& target) override{}
    void SeekToFirst() override{}
    void SeekToLast() override{}
    void Next() override { assert(false);}
    void Prev() override { assert(false);}
    Slice key() const override{
        assert(false);
        return Slice();
    }
    Slice value() const override{
        assert(false);
        return Slice();
    }
    Status status() const override { return status_;}

private:
    Status status_;
};
}
Iterator* NewEmptyIterator() { return new EmptyIterator(Status::OK());}
Iterator* NewErrorIterator(const Status& status){
    return new EmptyIterator(status);
}
} // namespace leveldb
