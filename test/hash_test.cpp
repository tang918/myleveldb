#include<iostream>
#include "util/hash.h"


void test(){
    const uint8_t data1[4] = {0xe1, 0x80, 0xb9, 0x32};
    auto res =leveldb:: Hash(reinterpret_cast<const char*>(data1),sizeof(data1),0xbc9f1d34);
    std::cout<<res<<std::endl;
}

int main(){
    test();
    return 0;
}