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

#include "common/exception/Exception.hpp"
#include "common/RemotePath.hpp"
#include "remotens/RemoteNS.hpp"
#include "remotens/RemoteNSDispatcher.hpp"

#include <algorithm>
#include <gtest/gtest.h>
#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace unitTests {

class cta_RemoteNsDispatcherTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  class TestRemoteNS: public cta::RemoteNS {
  public:
    ~TestRemoteNS() throw() {
    }

    cta::RemoteFileStatus statFile(const cta::RemotePath &path) const {
      throw cta::exception::Exception(std::string(__FUNCTION__) +
        " not implemented");
    }

    bool regularFileExists(const cta::RemotePath &path) const {
      incCallCounter("regularFileExists");
      auto itor = std::find(m_files.begin(), m_files.end(), path);
      return m_files.end() != itor;
    }

    bool dirExists(const cta::RemotePath &path) const {
      incCallCounter("dirExists");
      auto itor = std::find(m_dirs.begin(), m_dirs.end(), path);
      return m_dirs.end() != itor;
    }

    void rename(const cta::RemotePath &remoteFile,
      const cta::RemotePath &newRemoteFile) {
      incCallCounter("rename");
      {
        auto itor = std::find(m_files.begin(), m_files.end(), remoteFile);
        if(m_files.end() != itor) {
          *itor = newRemoteFile;
          return;
        }
      }
      {
        auto itor = std::find(m_dirs.begin(), m_dirs.end(), remoteFile);
        if(m_dirs.end() != itor) {
          *itor = newRemoteFile;
          return;
        }
      }
      std::ostringstream msg;
      msg << "Cannot rename " << remoteFile.getRawPath() <<
        " because it does not exist";
      throw cta::exception::Exception(msg.str());
    }

    uint64_t getCallCounter(const std::string &method) const {
      auto itor = m_callCounts.find(method);
      if(m_callCounts.end() == itor) {
        return 0;
      } else {
        return itor->second;
      }
    }

    std::list<cta::RemotePath> m_files;

    std::list<cta::RemotePath> m_dirs;

private:

    mutable std::map<std::string, uint64_t> m_callCounts;

    void incCallCounter(const std::string &method) const {
      auto itor = m_callCounts.find(method);
      if(m_callCounts.end() == itor) {
        m_callCounts[method] = 0;
      } else {
        itor->second++;
      }
    }
  }; // class TestRemoteNS

}; // class cta_RemoteNsDispatcherTest

TEST_F(cta_RemoteNsDispatcherTest, registerProtocolHandler) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  std::unique_ptr<RemoteNS> ns(new TestRemoteNS());
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns)));
}

TEST_F(cta_RemoteNsDispatcherTest, registerProtocolHandler_twice) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  std::unique_ptr<RemoteNS> ns1(new TestRemoteNS());
  std::unique_ptr<RemoteNS> ns2(new TestRemoteNS());
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns1)));
  ASSERT_THROW(dispatcher.registerProtocolHandler("test", std::move(ns2)),
    std::exception);
}

TEST_F(cta_RemoteNsDispatcherTest, regularFileExists_existing) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  const std::string regularFile = "testfile1";
  TestRemoteNS *testNs = new TestRemoteNS();
  std::unique_ptr<RemoteNS> ns(testNs);
  testNs->m_files.push_back(regularFile);
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns)));

  ASSERT_EQ(0, testNs->getCallCounter("regularFileExists"));
  bool regularFileExists = false;
  ASSERT_NO_THROW(regularFileExists = dispatcher.regularFileExists(regularFile));
  ASSERT_TRUE(regularFileExists);
  ASSERT_EQ(1, testNs->getCallCounter("regularFileExists"));
}

TEST_F(cta_RemoteNsDispatcherTest, regularFileExists_non_existing) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  const std::string regularFile = "testfile1";
  TestRemoteNS *testNs = new TestRemoteNS();
  std::unique_ptr<RemoteNS> ns(testNs);
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns)));

  ASSERT_EQ(0, testNs->getCallCounter("regularFileExists"));
  bool regularFileExists = false;
  ASSERT_NO_THROW(regularFileExists = dispatcher.regularFileExists(regularFile));
  ASSERT_FALSE(regularFileExists);
  ASSERT_EQ(1, testNs->getCallCounter("regularFileExists"));
}

} // namespace unitTests
