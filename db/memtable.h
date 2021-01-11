#pragma once
#include<string>

#include"db/dbformat.h"
#include"db/skiplist.h"
#include"util/arena.h"
#include "table/iterator.h"

namespace leveldb
{
class InternalKeyComparator;
class MemTableIterator;
static Slice GetLengthPrefixedSlice(const char* data){
    uint32_t len;
    const char*p = data;
    p = GetVarint32Ptr(p,p+5,&len);
    return Slice(p,len);
}
class MemTable{
public:
    explicit MemTable(const InternalKeyComparator& comparator);
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Ref(){++refs_;}
    void Unref(){
        --refs_;
        assert(refs_>=0);
        if(refs_<=0){
            delete this;
        }
    }

    size_t ApprosimateMemoryUsage();
    Iterator* NewIterator();
    void Add(SequenceNumber seq,ValueType type,const Slice& key,const Slice& value);
    bool Get(const LookupKey& key, std::string* value,Status* s);

private:
    friend class MemTableIterator;
    friend class MemTableBackwardIterator;
    struct KeyComparator{
        const InternalComparator comparator;
        explicit KeyComparator(const InternalComparator& c): comparator(c){}
        int operator()(const char* a,const char* b) const;
    };
    typedef SkipList<const char*,KeyComparator> Table;
    ~MemTable();
    KeyComparator comparator_;
    int refs_;
    Arena arena_;
    Table table_;
};

MemTable::MemTable(const InternalKeyComparator& Comparator):comparator_(comparator),refs_(0),table_(comparator_,&arena_){}

MemTable::~MemTable(){assert(refs_==0);}
size_t MemTable::ApprosimateMemoryUsage(){return arena_.MemoryUsage();}

int MemTable:: KeyComparator::operator()(const char* aptr,const char* bptr) const{
    Slice a=GetLengthPrefixedSlice(aptr);
    Slice b = GetLengthPrefixedSlice(bptr);
    return comparator.Compare(a,b);
}

static const char* EncodeKey(std::string* scratch,const Slice& target){
    scratch->clear();
    PutVarint32(scratch,target.size());
    scratch->append(target.data(),target.size());
    return scratch->data();
}

class MemTableIterator: public Iterator{
public:
    explicit MemTableIterator(MemTable::Table* table):iter_(table){}
    MemTableIterator(const MemTableIterator&) = delete;
    MemTableIterator& operator=(const MemTableIterator&)=delete;
    ~MemTableIterator() override=default;
    bool Valid()const override { return iter_.Valid();}
    void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_,k));}
    void SeekToFirst() override { iter_.SeekToFirst();}
    void SeekToLast() override{ iter_.SeekToLast(); }
    void Next()override { iter_.Next();}
    void Prev() override { iter_.Prev();}
    Slice key() const override { return GetLengthPrefixedSlice(iter_.key());}
    Slice value() const override {
        Slice key_slice = GetLengthPrefixedSlice(iter_.key());
        return GetLengthPrefixedSlice(key_slice.data()+key_slice.size());
    }
    Status status() const override { return Status::OK();}
private:
    MemTable::Table::Iterator iter_;
    std::string tmp_;
}

Iterator* MemTable::NewIterator() {return new MemTableIterator(&table_);}

/*
* SkipList中的每个entry组成如下:
key_size: internal_key.size() varint32
key_bytes:char[internal_key.size()]
value_size: varint32 of value.size()
value_bytes: char[value.size()]
**/
void MemTable::Add(SequenceNumber s,ValueType type,const Slice& key,const Slice& value){
    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size =  key_size + 8;
    const size_t encoded_len = VarintLength(internal_key_size)+ internal_key_size+VarintLength(val_size)+val_size;
    char* buf = arena_.Allocate(encoded_len);
    char* p  = EncodeVarint32(buf,internal_key_size);
    memcpy(p,key.data(),key_size);
    p += key_size;
    EncodeFixed64(p,(s<<8) | type);
    p += 8;
    p = EncodeVarint32(p,val_size);
    memcpy(p,value.data(),val_size);
    assert(p+val_size == buf+ encoded_len);
    table_.Insert(buf);

}

bool MemTable::Get(const LookupKey& key,std::string* value,Status* s){
    Slice memkey = key.memtable_key();
    Table::Iterator iter(&table_);
    iter.Seek(memkey.data());
    if(iter.Valid()){
        const char* entry = iter.key();
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry,entry+5,&key_length);
        if(comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr,key_length-8),key.user_key())==0){
                
            const uint64_t tag = DecodeFixed64(key_ptr + key_length -8);
            switch (static_cast<ValueType>(tag &0xff))
            {
            case kTypeValue:{
                Slice v = GetLengthPrefixedSlice(key_ptr+key_length);
                value->assign(v.data(),v.size());
                return true;
            }
            case kTypeDeletion:
                *s = Status::NotFound(Slice());
                return true;
            }
        }
        return false;
    }
}



} // namespace leveldb
