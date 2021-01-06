#include "util/arena.h"
#include <iostream>
void arena_test(){
    leveldb::Arena arena;
    arena.printArenaMessage();
    arena.Allocate(128);
    arena.printArenaMessage();
    arena.AllocateAligned(2123);
    arena.printArenaMessage();
    arena.Allocate(3134);
    arena.printArenaMessage();
    arena.Allocate(24);
    arena.printArenaMessage();
    
}
int main(){
    arena_test();
    return 0;
}