#pragma once

#include<atomic>
#include<cassert>
#include<cstddef>
#include<vector>
#include<iostream>

namespace leveldb
{
static const int KBlockSize = 4096;
class Arena{
public:
    Arena();

    //拷贝构造函数和赋值拷贝定义为删除，即不允许拷贝构造和拷贝赋值
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&)= delete;

    ~Arena();

    char* Allocate(size_t bytes);
    char* AllocateAligned(size_t bytes);

    size_t MemoryUsage() const{
        return memory_usage_.load(std::memory_order_relaxed);
    }

    void printArenaMessage(){
        std::cout<<"alloc_bytes_remain_ing:"<<alloc_bytes_remaining_<<std::endl;
        std::cout<<"内存池大小为:"<<blocks_.size()<<std::endl;
    }

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

    char* alloc_ptr_; //指向当前内存块可分配的内存的地址
    size_t alloc_bytes_remaining_;//当前内存块还剩余多少内存供使用
    std::vector<char*> blocks_;//使用vector组织内存块，相当于一个内存池
    std::atomic<size_t> memory_usage_;//已经使用了多少内存，使用原子操作来保证并发
};

/**
 * 分配内存的接口，分配内存的逻辑：
 * 如果bytes小于还剩下的可分配的内存(alloc_bytes_remaining_),则直接从allo_ptr指向的内存取出bytres的内存，并更新相应的参数
 * 如果bytes大于剩余可用的内存，则调用AllocateFallback申请一个新的内存块，并将该内存块加入到内存池中
*/
inline char* Arena::Allocate(size_t bytes){
    assert(bytes>0);
    if(bytes <= alloc_bytes_remaining_){
        char *result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    return AllocateFallback(bytes);
}

Arena::Arena():alloc_ptr_(nullptr),alloc_bytes_remaining_(0),memory_usage_(0){}

Arena:: ~Arena(){
    for(size_t i = 0;i<blocks_.size();i++){
        delete[] blocks_[i];
    }
}

//根据bytes大小进行新的内存块申请
char* Arena::AllocateFallback(size_t bytes){
    if(bytes > KBlockSize / 4){
        //如果申请的内存超过了四分之一的block大小，则直接申请bytes的内存块，这样可以避免后面浪费太多空间
        char *result = AllocateNewBlock(bytes);
        return result;
    }

    //我们将浪费掉当前块中剩余的内存。因为allo_ptr将指向新的内存块，所以alloc_ptr_指向的当前
    //内存块的剩余的内存将不会再使用
    alloc_ptr_ = AllocateNewBlock(KBlockSize);
    alloc_bytes_remaining_= KBlockSize;
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* Arena:: AllocateAligned(size_t bytes){
    const int align = (sizeof(void*)>8) ? sizeof(void*) : 8;
    
    static_assert((align & (align -1))==0,"Pointer size should be a power of 2");
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) &(align-1);//将当前指针地址强转无符号整型后，对align取模
    
    size_t slop = (current_mod == 0?0:align-current_mod);
    size_t needed = bytes+slop;
    char* result;
    if(needed <= alloc_bytes_remaining_){
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    }
    else{
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1))==0);
    return result;
}

/*
*申请一个新的内存块
*/
char* Arena::AllocateNewBlock(size_t block_bytes){
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.fetch_add(block_bytes + sizeof(char*),std::memory_order_relaxed);
    return result;
}

} // namespace leveldb
