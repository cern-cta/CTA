#pragma once

#include "ObjectOps.hpp"
#define USE_RADOS 1
#if USE_RADOS
typedef cta::objectstore::BackendRados myOS;
#else
typedef cta::objectstore::BackendVFS myOS;
#endif