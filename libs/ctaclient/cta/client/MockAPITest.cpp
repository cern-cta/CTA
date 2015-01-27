#include "cta/client/MockAPI.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockAPITest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockAPITest, createStorageClass_new) {
  using namespace cta::client;

  MockAPI api;

  ASSERT_TRUE(api.getStorageClasses().empty());

  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  ASSERT_NO_THROW(api.createStorageClass(name, nbCopies));

  ASSERT_EQ(1, api.getStorageClasses().size());
  cta::StorageClass storageClass;
  ASSERT_NO_THROW(storageClass = api.getStorageClasses().front());
  ASSERT_EQ(name, storageClass.name);
  ASSERT_EQ(nbCopies, storageClass.nbCopies);
}

TEST_F(cta_client_MockAPITest, createStorageClass_already_existing) {
  using namespace cta::client;

  MockAPI api;

  ASSERT_TRUE(api.getStorageClasses().empty());
  
  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  ASSERT_NO_THROW(api.createStorageClass(name, nbCopies));
  
  ASSERT_EQ(1, api.getStorageClasses().size());
  cta::StorageClass storageClass;
  ASSERT_NO_THROW(storageClass = api.getStorageClasses().front());
  ASSERT_EQ(name, storageClass.name);
  ASSERT_EQ(nbCopies, storageClass.nbCopies);

  ASSERT_THROW(api.createStorageClass(name, nbCopies), std::exception);
}

TEST_F(cta_client_MockAPITest, deleteStorageClass_existing) {
  using namespace cta::client;

  MockAPI api;

  ASSERT_TRUE(api.getStorageClasses().empty());
  
  const std::string name = "TestStorageClass";
  const uint8_t nbCopies = 2;
  ASSERT_NO_THROW(api.createStorageClass(name, nbCopies));
  
  ASSERT_EQ(1, api.getStorageClasses().size());
  cta::StorageClass storageClass;
  ASSERT_NO_THROW(storageClass = api.getStorageClasses().front());
  ASSERT_EQ(name, storageClass.name);
  ASSERT_EQ(nbCopies, storageClass.nbCopies);

  ASSERT_NO_THROW(api.deleteStorageClass(name));

  ASSERT_TRUE(api.getStorageClasses().empty());
}

TEST_F(cta_client_MockAPITest, deleteStorageClass_non_existing) {
  using namespace cta::client;

  MockAPI api;

  ASSERT_TRUE(api.getStorageClasses().empty());

  const std::string name = "TestStorageClass";
  ASSERT_THROW(api.deleteStorageClass(name), std::exception);

  ASSERT_TRUE(api.getStorageClasses().empty());
}

} // namespace unitTests
