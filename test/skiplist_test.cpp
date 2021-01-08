#include<iostream>
#include <string>
#include<set>
#include "util/slice.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include "util/random.h"
typedef uint64_t Key;
struct Comparator {
  int operator()(const Key& a, const Key& b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return +1;
    } else {
      return 0;
    }
  }
};
void test(){
   leveldb:: Arena arena;
    Comparator cmp;
    leveldb::SkipList<Key,Comparator>list(cmp,&arena);
    if(list.Contains(10)){
        std::cout<<"ok"<<std::endl;
    }
    else{
        std::cout<<"No"<<std::endl;
    }
}
void test1(){
    const int N=100;
    const int R=5000;
    leveldb::Random rnd(1000);
    std::set<Key> keys;
    leveldb::Arena arena;
    Comparator cmp;
    leveldb::SkipList<Key,Comparator> list(cmp,&arena);
    for(int i=0;i<N;i++){
        Key key = rnd.Next()% R;
        if(keys.insert(key).second){
            list.Insert(key);
        }
    }
    leveldb::SkipList<Key,Comparator>::Iterator iter(&list);
    iter.SeekToFirst();
    
    auto res=iter.key();

    std::cout<<res<<std::endl;
    iter.Next();
    std::cout<<iter.key()<<std::endl;


}
int main(){
    test1();
    return 0;
}