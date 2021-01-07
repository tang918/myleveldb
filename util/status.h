#pragma once

#include<algorithm>
#include<string>

#include "util/slice.h"

namespace leveldb
{

class Status{
public:
    Status()noexcept:state_(nullptr){}
    ~Status() {delete[] state_;}
    Status(const Status& rhs);
    Status& operator=(const Status& rhs);

    Status(Status&& rhs) noexcept: state_(rhs.state_){rhs.state_ = nullptr;}
    Status& operator=(Status&& rhs)noexcept;

    static Status OK() {return Status();}

    static Status NotFound(const Slice& msg,const Slice& msg2=Slice()){
        return Status(kNotFound,msg,msg2);
    }

    static Status Corruption(const Slice&msg,const Slice&msg2 = Slice()){
        return Status(kCorruption,msg,msg2);
    }
    static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kNotSupported, msg, msg2);
    }
    static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kInvalidArgument, msg, msg2);
    }
    static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kIOError, msg, msg2);
    }

    bool ok() const { return (state_ == nullptr); }
    bool IsNotFound() const { return code() == kNotFound; }
    bool IsCorruption() const {return code()==kCorruption;}
    bool IsIOError() const { return code() == kIOError; }
    bool IsNotSupportedError() const { return code() == kNotSupported; }
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    std::string ToString() const;



private:
    enum Code{
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5
    };

    Code code()const{
        return (state_ == nullptr)? kOk:static_cast<Code>(state_[4]);
    }

    Status(Code code,const Slice& msg,const Slice& msg2);
    static const char* CopyState(const char* s);

    /**
     * Ok status state_为null,否则state_的格式如下:
     * state_[0..3]==length of message;
     * state_[4] == code
     * state_[5..]==message;
     * **/
    const char* state_;
};

inline Status::Status(const Status& rhs){
    state_ = (rhs.state_==nullptr)? nullptr:CopyState(rhs.state_);
}
const char* Status::CopyState(const char* state){
    uint32_t size;
    memcpy(&size,state,sizeof(size));//前4个字节表示message的长度
    char* result = new char[size+5];
    memcpy(result,state,size+5);
    return result;

}
Status::Status(Code code,const Slice& msg,const Slice& msg2){
    assert(code !=kOk);
    const uint32_t len1 = static_cast<uint32_t>(msg.size());
    const uint32_t len2 = static_cast<uint32_t>(msg2.size());
    const uint32_t size = len1 + (len2 ? (2+len2):0);
    char* result = new char[size+5];
    memcpy(result,&size,sizeof(size));
    result[4] = static_cast<char>(code);
    memcpy(result+5,msg.data(),len1);
    if(len2){
        result[5+len1]=':';
        result[6+len1] = ' ';
        memcpy(result+7+len1,msg2.data(),len2);
    }
    state_ = result;
}

std::string Status::ToString() const{
    if(state_==nullptr) return "OK";
    else{
         char tmp[30];
         const char* type;
         switch (code()) {
            case kOk:
                type = "OK";
                break;
            case kNotFound:
                type = "NotFound: ";
                break;
            case kCorruption:
                type = "Corruption: ";
                break;
            case kNotSupported:
                type = "Not implemented: ";
                break;
            case kInvalidArgument:
                type = "Invalid argument: ";
                break;
            case kIOError:
                type = "IO error: ";
                break;
            default:
                snprintf(tmp, sizeof(tmp),
                 "Unknown code(%d): ", static_cast<int>(code()));
                type = tmp;
                break;
        }
         std::string result(type);
         uint32_t length;
         memcpy(&length,state_,sizeof(length));
         result.append(state_+5,length);
         return result;
    }
}

inline Status& Status::operator=(const Status& rhs){
    if(state_ != rhs.state_){
        delete[] state_;
        state_ = (rhs.state_==nullptr)? nullptr:CopyState(rhs.state_);
    }
    return *this;
}
inline Status& Status::operator=(Status&& rhs)noexcept{
    std::swap(state_,rhs.state_);
    return *this;
}

} // namespace leveldb
