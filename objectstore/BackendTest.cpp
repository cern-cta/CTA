/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "BackendTest.hpp"
#include "BackendVFS.hpp"
#include "BackendRados.hpp"
#include "common/exception/Exception.hpp"

namespace unitTests {

TEST_P(BackendAbstractTest, BasicReadWrite) {
  //std::cout << "Type=" << m_os->typeName() << std::endl;
  const std::string testValue = "1234";
  const std::string testSecondValue = "12345";
  const std::string testObjectName = "testObject";
  // Check we can verify the absence of an object
  ASSERT_FALSE(m_os->exists(testObjectName));
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
  ASSERT_FALSE(m_os->exists(testObjectName));
}

TEST_P(BackendAbstractTest, LockingInterface) {
  //std::cout << "Type=" << m_os->typeName() << std::endl;
  const std::string testObjectName = "testObject";
  const std::string nonExistingObject = "thisObjectShouldNotExist";
  m_os->create(testObjectName, "");
  {
    // If we don't scope the object, the release will blow up after
    // removal of the file.
    std::unique_ptr<cta::objectstore::Backend::ScopedLock> lock( 
      m_os->lockExclusive(testObjectName));
  }
  {
    std::unique_ptr<cta::objectstore::Backend::ScopedLock> lock( 
      m_os->lockExclusive(testObjectName));
    lock->release();
  }
  // We should also tolerate releasing a lock taken on an object deleted
  // in the mean time
  std::unique_ptr<cta::objectstore::Backend::ScopedLock> lock( 
      m_os->lockExclusive(testObjectName));
  m_os->remove(testObjectName);
  ASSERT_NO_THROW(lock->release());
  // The object should be gone
  ASSERT_FALSE(m_os->exists(testObjectName));
  // Attempting to lock a non-existing object should fail
  {
    std::unique_ptr<cta::objectstore::Backend::ScopedLock> lock;
    ASSERT_THROW(lock.reset(m_os->lockExclusive(nonExistingObject)), cta::exception::Exception);
    ASSERT_THROW(lock.reset(m_os->lockShared(nonExistingObject)), cta::exception::Exception);
  }
  // The object should not be created as a side effect
  ASSERT_FALSE(m_os->exists(nonExistingObject));
}

TEST_P(BackendAbstractTest, ParametersInterface) {
  //std::cout << "Type=" << m_os->typeName() << std::endl;
  std::unique_ptr<cta::objectstore::Backend::Parameters> params(
    m_os->getParams());
  //std::cout << params->toStr() << std::endl;
}

cta::objectstore::BackendVFS osVFS;
#define TEST_RADOS 1
#if TEST_RADOS
cta::objectstore::BackendRados osRados("cta-eric", "tapetest", "cta-eric");
INSTANTIATE_TEST_CASE_P(BackendTestRados, BackendAbstractTest, ::testing::Values((cta::objectstore::Backend*)&osRados));
#endif
INSTANTIATE_TEST_CASE_P(BackendTestVFS, BackendAbstractTest, ::testing::Values((cta::objectstore::Backend*)&osVFS));

}
