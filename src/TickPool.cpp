#include "TickPool.h"

// 显式实例化常用类型
template class TickPool<std::chrono::milliseconds>;
template class TickPool<std::chrono::seconds>;