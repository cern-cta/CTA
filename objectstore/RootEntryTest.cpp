#include <gtest/gtest.h>
#include "BackendVFS.hpp"
#include "exception/Exception.hpp"
#include "RootEntry.hpp"

TEST(RootEntry, BasicAccess) {
  cta::objectstore::BackendVFS be;
  // Try to create the root entry
  cta::objectstore::RootEntry re;
  re.fetch();
  cta::objectstore::RootEntry::init(be);
  // Delete the root entry
  be.remove("root");
}