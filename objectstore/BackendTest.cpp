/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "BackendTest.hpp"
#include "BackendVFS.hpp"
#include "BackendRados.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/exception/Exception.hpp"
#include "common/Timer.hpp"
#include "BackendRadosTestSwitch.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"
#include <atomic>
#include <future>

namespace unitTests {

TEST_P(BackendAbstractTest, BasicReadWrite) {
  //std::cout << "Type=" << m_os->typeName() << std::endl;
  const std::string testValue = "1234";
  const std::string testSecondValue = "12345";
  const std::string testObjectName = "testObject";
  // Make sure there is no leftover from previous runs
  try { m_os->remove(testObjectName); } catch (...) {}
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
  // Make sure we will recreate the object (no leftover from previous runs)
  try { m_os->remove(testObjectName); } catch (...) {}
  m_os->create(testObjectName, "X");
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

TEST_P(BackendAbstractTest, MultithreadLockingInterface) {
  // This test validates the locking of the object store
  // Launch many threads which will bump up a number stored in a file.
  // Similarly counted in memory
#ifdef LOOPING_TEST
  do {
#endif
  const std::string testObjectName = "testObject";
  uint64_t val=0;
  std::string valStr;
  valStr.append((char*)&val, sizeof(val));
  try { m_os->remove(testObjectName); } catch (...) {}
  m_os->create(testObjectName, valStr);
  auto os=m_os;
  std::atomic<uint64_t> counter(0);
  std::list<std::future<void>> insertCompletions;
  std::list<std::function<void()>> lambdas;
  const size_t threadCount=100;
  const size_t passCount=100;
  for (size_t i=0; i<threadCount; i++) {
    lambdas.emplace_back([&testObjectName,os,&passCount,&counter,i](){
      for (size_t pass=0; pass<passCount; pass++) {
        std::unique_ptr<cta::objectstore::Backend::ScopedLock> sl(os->lockExclusive(testObjectName));
        uint64_t val;
        os->read(testObjectName).copy((char*)&val,sizeof(val));
        val++;
        std::string valStr;
        valStr.append((char*)&val, sizeof(val));
        os->atomicOverwrite(testObjectName, valStr);
        counter++;
        //printf("%03ld ",(uint64_t)i); fflush(stdout);.
      }
      //printf("--- ");
      //fflush(stdout);
    });
    insertCompletions.emplace_back(std::async(std::launch::async, lambdas.back()));
  }
  for (auto &ic: insertCompletions) { ic.wait(); }
  insertCompletions.clear();
  lambdas.clear();
  m_os->read(testObjectName).copy((char*)&val, sizeof(val));
#ifdef LOOPING_TEST
  printf(".");
  if (counter != val) {
    std::cout << "counter=" << counter << " val=" << val << std::endl;
    std::cout << "ERROR!! *************************************************************" << std::endl;
    while (true) { sleep (1); }
  }
#endif
  ASSERT_EQ(counter, val);
  m_os->remove(testObjectName);
#ifdef LOOPING_TEST
  }while(true);
#endif
}

TEST_P(BackendAbstractTest, AsyncIOInterface) {
  // Create object to update.
  const std::string testValue = "1234";
  const std::string testSecondValue = "12345";
  const std::string testObjectName = "testObject";
  try {m_os->remove(testObjectName);}catch(...){}
  m_os->create(testObjectName, testValue);
  // Launch update of object via asynchronous IO
  std::function<std::string(const std::string &)> updaterCallback = [&testSecondValue](const std::string& s)->std::string { return testSecondValue; };
  std::unique_ptr<cta::objectstore::Backend::AsyncUpdater> updater(m_os->asyncUpdate(testObjectName,updaterCallback));
  updater->wait();
  ASSERT_EQ(testSecondValue, m_os->read(testObjectName));
  // Async re-read
  std::unique_ptr<cta::objectstore::Backend::AsyncLockfreeFetcher> reader(m_os->asyncLockfreeFetch(testObjectName));
  ASSERT_EQ(testSecondValue, reader->wait());
  m_os->remove(testObjectName);
}

TEST_P(BackendAbstractTest, AsyncIOInterfaceRemove) {
  // Create object to delete in async update.
  const std::string testValue = "1234";
  const std::string testObjectName = "testObject";
  try {m_os->remove(testObjectName);} catch(...){}
  m_os->create(testObjectName, testValue);
  // Launch update of object via asynchronous IO
  std::function<std::string(const std::string &)> updaterCallback =
    [](const std::string& s)->std::string { throw cta::objectstore::Backend::AsyncUpdateWithDelete(); };
  std::unique_ptr<cta::objectstore::Backend::AsyncUpdater> updater(m_os->asyncUpdate(testObjectName,updaterCallback));
  ASSERT_NO_THROW(updater->wait());
  ASSERT_FALSE(m_os->exists(testObjectName));
}

TEST_P(BackendAbstractTest, AsyncIOInterfaceMultithread) {
  // Create object to update.
  const std::string testValue = "1234";
  const std::string testSecondValue = "12345";
  const std::string testObjectNameRadix = "testObject";
  std::function<std::string(size_t)> testObjectName = [&testObjectNameRadix](size_t i) {
    std::stringstream tom;
    tom << testObjectNameRadix << i;
    return tom.str();
  };
  std::function<std::string(size_t)> value=[&testSecondValue](size_t i) {
    std::stringstream val;
    val << testSecondValue << i;
    return val.str();
  };
  for (size_t i=0; i<10; i++) { try {m_os->remove(testObjectName(i));}catch(...){} }
  std::list<std::unique_ptr<cta::objectstore::Backend::AsyncUpdater>> updaters;
  std::list<std::function<std::string(const std::string &)>> lambdas;
  for (size_t i=0; i<10; i++) {
    m_os->create(testObjectName(i), testValue);
    // Launch update of object via asynchronous IO
    lambdas.emplace_back([i,&value](const std::string &s)->std::string{return value(i);});
    updaters.emplace_back(m_os->asyncUpdate(testObjectName(i),lambdas.back()));
  }
  size_t i=0;
  for (auto & u: updaters) {
    u->wait();
    ASSERT_EQ(value(i), m_os->read(testObjectName(i)));
    m_os->remove(testObjectName(i));
    i++;
  }
}

TEST_P(BackendAbstractTest, ParametersInterface) {
  //std::cout << "Type=" << m_os->typeName() << std::endl;
  std::unique_ptr<cta::objectstore::Backend::Parameters> params(
    m_os->getParams());
  //std::cout << params->toStr() << std::endl;
}

static cta::objectstore::BackendVFS osVFS(__LINE__, __FILE__);
#ifdef TEST_RADOS
static cta::log::DummyLogger dl("", "");
static cta::objectstore::BackendRados osRados(dl, "tapetest", "tapetest");
INSTANTIATE_TEST_CASE_P(BackendTestRados, BackendAbstractTest, ::testing::Values((cta::objectstore::Backend*)&osRados));
#endif
INSTANTIATE_TEST_CASE_P(BackendTestVFS, BackendAbstractTest, ::testing::Values((cta::objectstore::Backend*)&osVFS));

}
