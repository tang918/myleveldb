#include "util/coding.h"
#include "util/slice.h"
#include<iostream>
#include<string>

void fixed32Test(){
    std::string s;
    uint32_t value=24;
    leveldb::PutFixed32(&s,value);
    const char* p = s.data();
    auto num = leveldb::DecodeFixed32(p);
    std::cout<<num<<std::endl;

}

void varint32test(){
    std::string s;
    uint32_t value=31345;
    leveldb::PutVarint32(&s,value);
    leveldb::Slice slice(s);
    std::cout<<"s的长度为: "<<s.size()<<std::endl;
    std::cout<<"slice的长度为: "<<slice.size()<<std::endl;
    uint32_t value1=0;
    auto res= leveldb::GetVarint32(&slice,&value1);
    std::cout<<"slice的长度编程什么东西： "<<slice.size()<<std::endl;
    std::cout<<"value的值是什么玩意儿: "<<value1<<std::endl;

}
int main(){
   // fixed32Test();
    varint32test();
    return 0;
}