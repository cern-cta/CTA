#pragma once

#include "ObjectOps.hpp"
#define USE_RADOS 1
#if USE_RADOS
typedef cta::objectstore::ObjectStoreRados myOS;
#else
typedef cta::objectstore::ObjectStoreVFS myOS;
#endif