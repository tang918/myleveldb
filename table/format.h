#pragma once
#include<stdint.h>
#include<string>

#include "util/slice.h"
#include "util/status.h"

namespace leveldb{



struct BlockContents{
    Slice data;
    bool cachable;
    bool heap_allocated;
};
}//namespace leveldb