#include<iostream>
#include<string>
#include "util/slice.h"

int main(){
    std::string s("hello12");
    std::string s1("hello");
    leveldb::Slice slice1(s);
    leveldb::Slice slice2(s1);
    std::cout<< s.compare(s1)<<std::endl;
    return 0;
}