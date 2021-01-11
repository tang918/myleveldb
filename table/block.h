#pragma once
#include<stddef.h>
#include<stdint.h>
#include "table/iterator.h"
#include "util/coding.h"
#include "util/comparator.h"
#include"util/logging.h"
#include "table/format.h"
namespace leveldb
{
struct BlockContent;
class Comparator;

class Block{
public:
    explicit Block(const BlockContent& contents);
    Block(const Block&)=delete;
    Block& operator=(const Block&)=delete;
    ~Block();
    size_t size() const { return size_;}
    Iterator* NewEmptyIterator(const Comparator* comparator);
private:
    class Iter;
    uint32_t NumRestarts() const;
    const char* data_;//block数据指针
    size_t size_;//block数据大小
    uint32_t restart_offset_;//重启点数组的偏移位置
    bool owned_;//data_[]是否是Block拥有的
};

inline uint32_t Block::NumRestarts() const{
    assert(size_>= sizeof(uint32_t));
    return DecodeFixed32(data_+size_-sizeof(uint32_t));//最后四字节存放的是重启点的数量
}
Block::Block(const BlockContent& contents)
    :data_(contents.data.data()),size_(contents.data.size()),owned_(contents.heap_allocated){
        if(size_ <sizeof(uint32_t)){
            size_ = 0;//该block_data块出错
        }else{
            //size_能允许的最大的重启点数量，一个重启点占4个字节。所以除以4
            size_t max_restarts_allowed  =(size_-sizeof(uint32_t)) / sizeof(uint32_t);
            if(NumRestarts()>max_restarts_allowed){
                size_ = 0;
            }else{
                restart_offset_ = size_ - (1+NumRestarts())*sizeof(uint32_t);
            }
        }
}
Block::~Block(){
    if(owned_){
        delete[] data_;
    }
}

static inline const char* DecodeEntry(const char*p,const char*limit,uint32_t* shared,uint32_t* non_shared,uint32_t* value_length){
    if(limit-p<3) return nullptr;
    *shared = reinterpret_cast<const uint8_t*>(p)[0];
    *non_shared = reinterpret_cast<const uint8_t*>(p)[1];
    *value_length = reinterpret_cast<const uint8_t*>(p)[2];
    if((*shared | *non_shared | *value_length) < 128){
        p += 3;
    }else{
        if((p=GetVarint32Ptr(p,limit,shared))==nullptr) return nullptr;
        if((p=GetVarint32Ptr(p,limit,non_shared))==nullptr) return nullptr;
        if((p=GetVarint32Ptr(p,limit,value_length))==nullptr)return nullptr;
    }
    if(static_cast<uint32_t>(limit-p) <(*non_shared+*value_length)){
        return nullptr;
    }
    return p;
}

class Block::Iter:public Iterator{
private:
    const Comparator* const comparator_;
    const char* const data_;//block内容
    uint32_t const restarts_;//重启点在data_中的位移
    uint32_t const num_restarts_;//重启点个数
    uint32_t current_;//当前entry在data_中的偏移，》=restarts_表示非法
    uint32_t restart_index_;//current_所在的重启点的index
    std::string key_;
    Slice value_;
    Status status_;
    inline int Compare(const Slice& a,const Slice& b) const {
        return comparator_->Compare(a,b);
    }
    inline uint32_t NextEntryOffset()const{
        return (value_.data()+ value_.size())-data_;
    }

    uint32_t GetRestartPoint(uint32_t index){
        assert(index<num_restarts_);
        return DecodeFixed32(data_+restarts_+index*sizeof(uint32_t));
    }

    void SeekToRestartPoint(uint32_t index){
        key_.clear();
        restart_index_ = index;
        uint32_t offset = GetRestartPoint(index);
        //value_并不是记录value字段，而是一个指向记录起始位置的长0的指针，
        //这样后面的ParseNextkey函数将会解析出重启点的value字段，并赋值到value_字段中
        value_ = Slice(data_+offset,0);

    }
public:
    Iter(const Comparator* comparator,const char* data,uint32_t restarts,uint32_t num_restarts):
        comparator_(comparator),
        data_(data),
        restarts_(restarts_),
        num_restarts_(num_restarts),
        current_(restarts_),
        //创建一个Block:;Iter之后，它是处于invalid的状态，即不能Prev也不能Next,需要先Seek/SeekToXX之后，才能调用next/prev;
        restart_index_(num_restarts_-1){
         assert(num_restarts_>0);
    }
    bool Valid() const override{ return current_ < restarts_;}

    Status status() const override{return status_;}
    Slice key() const override{
        assert(Valid());
        return key_;
    }
    Slice value() const override{
        assert(Valid());
        return value_;
    }
    void Next()override{
        assert(Valid());
        ParseNextKey();
    }
    void Prev() override{
        assert(Valid());
        const uint32_t original = current_;
        while(GetRestartPoint(restart_index_)>=original){
            if(restart_index_==0){//不存在前一个实体
                current_ = restarts_;
                restart_index_ = num_restarts_;
                return;
            }
            restart_index_--;
        }
        SeekToRestartPoint(restart_index_);
        do{

        }while(ParseNextKey()&& NextEntryOffset()<original);
    }

    void Seek(const Slice& target) override{
        uint32_t left=0;
        uint32_t right = num_restarts_-1;
        while(left<right){
            uint32_t mid = (left+right+1)/2;
            uint32_t region_offset = GetRestartPoint(mid);
            uint32_t shared,non_shared,value_length;
            const char* key_ptr = DecodeEntry(data_+region_offset,data_+restarts_,&shared,&non_shared,&value_length);
            if(key_ptr==nullptr ||(shared!=0)){
                CorruptionError();
                return;
            } 
            Slice mid_key(key_ptr,non_shared);
            if(Compare(mid_key,target)<0){
                left=mid;
            }else{
                right=mid-1;
            }
        }
        SeekToRestartPoint(left);
        while(true){
            if(!ParseNextKey()){
                return;
            }
            if(Compare(key_,target)>=0){
                return;
            }
        }
    }
    void SeekToFirst()override{
        SeekToRestartPoint(0);
        ParseNextKey();
    }

    void SeekToLast() override{
        SeekToRestartPoint(num_restarts_-1);
        while(ParseNextKey()&& NextEntryOffset()<restarts_){}

    }


private:
    void CorruptionError(){
        current_ = restarts_;
        restart_index_ = num_restarts_;
        status_ = Status::Corruption("bad entry in block");
        key_.clear();
        value_.clear();
    }
    bool ParseNextKey(){
        current_ = NextEntryOffset();
        const char* p = data_+current_;
        const char* limit = data_+restarts_;//重启点数组所在位置
        if(p>=limit){
            //p不能超过重启点数组开始位置
            current_ = restarts_;
            restart_index_ = num_restarts_;
            return false;
        }

        //Decode next entry
        uint32_t shared,non_shared,value_length;
        p = DecodeEntry(p,limit,&shared,&non_shared,&value_length);
        if(p==nullptr || key_.size()<shared){
            CorruptionError();
            return false;
        }else{
            key_.resize(shared);
            key_.append(p,non_shared);
            value_ = Slice(p+non_shared,value_length);
            while(restart_index_+1<num_restarts_ && GetRestartPoint(restart_index_+1)<current_){
                ++ restart_index_;
            }
            return true;
        }
    }
};

Iterator* Block::NewEmptyIterator(const Comparator* comparator){
    if(size_ < sizeof(uint32_t)){
        return NewErrorIterator(Status::Corruption("bad block contents"));
    }
    const uint32_t num_restarts = NumRestarts();
    if(num_restarts==0){
        return NewEmptyIterator();
    }else{
        return new Iter(comparator,data_,restart_offset_,num_restarts);
    }
}
} // namespace leveldb
