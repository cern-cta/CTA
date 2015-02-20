#include <gtest/gtest.h>
#include "BackendVFS.hpp"
#include "exception/Exception.hpp"

namespace unitTests {
  TEST(BackendVFS, BasicReadWrite) {
    cta::objectstore::BackendVFS bvfs;
    const std::string testValue = "1234";
    const std::string testSecondValue = "1234";
    const std::string testObjectName = "testObject";
    // Check we can verify the absence of an object
    ASSERT_EQ(false, bvfs.exists(testObjectName));
    // Check that an update attempt fails on a non-existing object
    ASSERT_THROW(bvfs.atomicOverwrite(testObjectName, testSecondValue), cta::exception::Exception);
    // Check the creation of the obecjt
    bvfs.create(testObjectName, testValue);
    // Check we can validate the presence of the object
    ASSERT_EQ(true, bvfs.exists(testObjectName));
    // Check that we can read back after creation
    ASSERT_EQ(testValue, bvfs.read(testObjectName));
    bvfs.atomicOverwrite(testObjectName, testSecondValue);
    // Check that an update goes through
    ASSERT_EQ(testSecondValue, bvfs.read(testObjectName));
    // Check that we read back the value
    ASSERT_EQ(testSecondValue, bvfs.read(testObjectName));
    // Check we can delete the object
    ASSERT_NO_THROW(bvfs.remove(testObjectName));
    // Check that the object is actually gone
    ASSERT_EQ(false, bvfs.exists(testObjectName));
  }
}