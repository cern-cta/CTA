#include "BackendAbstractTests.hpp"
#include "BackendVFS.hpp"
#include "BackendRados.hpp"
#include "exception/Exception.hpp"

TEST_P(BackendAbstractTest, BasicReadWrite) {
  std::cout << "Type=" << m_os->typeName() << std::endl;
  const std::string testValue = "1234";
  const std::string testSecondValue = "1234";
  const std::string testObjectName = "testObject";
  // Check we can verify the absence of an object
  ASSERT_EQ(false, m_os->exists(testObjectName));
  // Check that an update attempt fails on a non-existing object
  ASSERT_THROW(m_os->atomicOverwrite(testObjectName, testSecondValue), cta::exception::Exception);
  // Check the creation of the object
  m_os->create(testObjectName, testValue);
  // Check that re-creating an existing object throws exception
  ASSERT_THROW(m_os->create(testObjectName, testValue), cta::exception::Exception);
  // Check we can validate the presence of the object
  ASSERT_EQ(true, m_os->exists(testObjectName));
  // Check that we can read back after creation
  ASSERT_EQ(testValue, m_os->read(testObjectName));
  m_os->atomicOverwrite(testObjectName, testSecondValue);
  // Check that an update goes through
  ASSERT_EQ(testSecondValue, m_os->read(testObjectName));
  // Check that we read back the value
  ASSERT_EQ(testSecondValue, m_os->read(testObjectName));
  // Check we can delete the object
  ASSERT_NO_THROW(m_os->remove(testObjectName));
  // Check that the object is actually gone
  ASSERT_EQ(false, m_os->exists(testObjectName));
}

cta::objectstore::BackendVFS osVFS;
cta::objectstore::BackendRados osRados("tapetest", "tapetest");
INSTANTIATE_TEST_CASE_P(BackendTest, BackendAbstractTest, ::testing::Values(&osVFS, &osRados));
