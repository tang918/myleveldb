#pragma once

#include<cstddef>
#include<cstdint>
#include<string>
#include<sstream>
#include "util/slice.h"
#include "db/logging.h"
#include "util/coding.h"
#include "util/comparator.h"
namespace leveldb{


enum ValueType{ kTypeDeletion=0x0,kTypeValue = 0x1};
static const ValueType kValueTypeForSeek = kTypeValue;
typedef uint64_t SequenceNumber;
static const SequenceNumber kMaxSequenceNumber = ((0x1ull<<56)-1);

//InternalKey由user_key,sequence,type组成
struct ParsedInternalKey{
    Slice user_key; 
    SequenceNumber sequence;//7bytes,
    ValueType type;//1byte,表示该key是删除还是插入,0表示删除，1表示插入

    ParsedInternalKey(){}
    ParsedInternalKey(const Slice& u,const SequenceNumber& seq,ValueType t)
        :user_key(u),sequence(seq),type(t){}
    std::string DebugString() const{
        std::ostringstream ss;
        ss<< "\'"<<EscapeString(user_key.ToString())<<"'@"<<sequence<<":"
        <<static_cast<int>(type);
        return ss.str();
    }
};

inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key){
    return key.user_key.size()+8;
}

static uint64_t PackSequenceAndType(uint64_t seq,ValueType t){
    assert(seq<=kMaxSequenceNumber);
    assert(t<=kValueTypeForSeek);
    return seq<<8 | t;
}

void AppendInternalKey(std::string* result,const ParsedInternalKey& key){
    result->append(key.user_key.data(),key.user_key.size());
    PutFixed64(result,PackSequenceAndType(key.sequence,key.type));
}

inline Slice ExtractUserKey(const Slice& internal_key){
    assert(internal_key.size()>=8);
    return Slice(internal_key.data(),internal_key.size() - 8);
}

bool ParseInternalKey(const Slice& internal_key,ParsedInternalKey* result){
    const size_t n = internal_key.size();
    if(n<8) return false;
    uint64_t num = DecodeFixed64(internal_key.data()+n-8);
    uint8_t c = num && 0xff;
    result->sequence = num >> 8;
    result->type =  static_cast<ValueType>(c);
    result->user_key = Slice(internal_key.data(),n-8);
    return (c<=static_cast<uint8_t>(kTypeValue));
}

class InternalKey{

private:
    std::string rep_;

public:
    InternalKey(){}
    InternalKey(const Slice& user_key,SequenceNumber s,ValueType t){
        AppendInternalKey(&rep_,ParsedInternalKey(user_key,s,t));
    }
    bool DecodeFrom(const Slice& s){
        rep_.assign(s.data(),s.size());
        return !rep_.empty();
    }
    Slice Encode() const{
        assert(!rep_.empty());
        return rep_;
    }

    Slice user_key() const { return ExtractUserKey(rep_);}

    void SetFrom(const ParsedInternalKey& p){
        rep_.clear();
        AppendInternalKey(&rep_,p);
    }
    void Clear(){rep_.clear();}
    std::string DebugString() const{
        ParsedInternalKey parsed;
        if (ParseInternalKey(rep_, &parsed)) {
            return parsed.DebugString();
        }
         std::ostringstream ss;
        ss << "(bad)" << EscapeString(rep_);
        return ss.str();
    }
};

/**
 * LookupKey的格式为
 * size(int 变长，最多5字节,表示user_key的长度)|user_key|sequence number(7bytes)|type(1byte)
 * LookupKey是memtable查询接口传入的key
*/
class LookupKey{
public:
    LookupKey(const Slice& user_key,SequenceNumber sequence){
        size_t usize = user_key.size();
        size_t needed = usize+13;
        char* dst;
        if(needed<=sizeof(space_)){
            dst=space_;
        }else{
            dst = new char[needed];
        }
        start_ = dst;
        dst = EncodeVarint32(dst,usize+8);
        kstart_ = dst;
        memcpy(dst,user_key.data(),usize);
        dst += usize;
        EncodeFixed64(dst,PackSequenceAndType(s,kValueTypeForSeek));//定长，8字节
        dst += 8;
        end_ = dst;
    }
    LookupKey(const LookupKey&) = delete;
    LookupKey& operator=(const LookupKey&) = delete;
    ~LookupKey(){
        if(start_ != space_) delete[] start_;
    }

    Slice memtable_key() const { return Slice(start_,end_-start_);}
    Slice internal_key() const { return Slice(kstart_,end_-kstart_);}//返回internal_key
    Slice user_key() const { return Slice(kstart_,end_-kstart_-8);}//返回user_key
private:
    const char* start_;//LookupKey的开始位置
    const char* kstart_; //user_key的开始位置
    const char* end_;//LookupKey的结束位置
    char space_[200];//避免为较小的key分配内存
};

class InternalComparator: public Comparator{
private:
 const Comparator* user_comparator_;

public:
    explicit InternalComparator(const Comparator* c):user_comparator_(c){}
    const char* Name() const override{
        return "leveldb.InternalKeyComparator";
    }
    //比较规则：先按照user_key升序，若user_key相同，则按照sequence_num降序
    int Compare(const Slice& akey,const Slice& bkey)const override{
        int r = user_comparator_->Compare(ExtractUserKey(akey),ExtractUserKey(bkey));
        if(r==0){
            const uint64_t anum = DecodeFixed64(akey.data()+akey.size()-8);
            const uint64_t bnum = DecodeFixed64(bkey.data()+bkey.size()-8);
            if(anum>bnum){
                r=-1;
            }else if(anum<bnum){
                r=+1;
            }
        }
        return r;
    }
    void FindShortestSeparator(std::string* start,const Slice& limit)const override{
        Slice user_start = ExtractUserKey(*start);
        Slice user_limit = ExtractUserKey(limit);
        std::string tmp(user_start.data(),user_start.size());
        user_comparator_->FindShortestSeparator(&tmp,user_limit);
        if(tmp.size()<user_start.size() && user_comparator_->Compare(user_start,tmp)<0){
            PutFixed64(&tmp,PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
            assert(this->Compare(*start,tmp)<0);
            assert(this->Compare(tmp,limit)<0);
            start->swap(tmp);
        }
    }

    void FindShortSuccessor(std::string* key) const override{
        Slice user_key = ExtractUserKey(*key);
        std::string tmp(user_key.data(),user_key.size());
        user_comparator_->FindShortSuccessor(&tmp);
        if(tmp.size()<user_key.size() && user_comparator_->Compare(user_key,tmp)<0){
            PutFixed64(&tmp,PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
            assert(this->Compare(*key,tmp)<0);
            key->swap(tmp);
        }
    }

    const Comparator* user_comparator() const { return user_comparator_;}
    int Compare(const InternalKey& a,const InternalKey& b) const { return Compare(a.Encode(),b.Encode());}
};

}//leveldb