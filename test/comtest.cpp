#include<iostream>
#include<string>
#include "util/comparator.h"
#include "util/slice.h"

void test1(){
    leveldb::BytewiseComparatorImpl com;
    
    std::string s1("helloozld");
    std::string s2("hellozooe");
    leveldb::Slice slice1(s1);
    leveldb::Slice slice2(s2);
    std::cout<<com.Compare(slice1,slice2)<<std::endl;
    com.FindShortestSeparator(&s1,slice2);
    std::cout<<s1<<std::endl;
    com.FindShortSuccessor(&s2);
    std::cout<<s2<<std::endl;
}

void test2(){
    auto comparator = leveldb::BytewiseComparator();
    std::string s1("helloozld");
    std::string s2("hellozooe");
    leveldb::Slice slice1(s1);
    leveldb::Slice slice2(s2);
    std::cout<<comparator->Compare(slice1,slice2)<<std::endl;
}
int main(){
    test2();
    return 0;
}