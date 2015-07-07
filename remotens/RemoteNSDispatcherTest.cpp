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
#include <sstream>
#include <stdint.h>
#include <string>

namespace unitTests {

class cta_RemoteNSDispatcherTest : public ::testing::Test {
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
      incCallCounter("statFile");
      auto itor = std::find(m_files.begin(), m_files.end(), path);
      if(m_files.end() == itor) {
        std::ostringstream msg;
        msg << "Remote file " << path.getRaw() << " does not exist";
        throw cta::exception::Exception(msg.str());
      }
      return cta::RemoteFileStatus();
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
      msg << "Cannot rename " << remoteFile.getRaw() <<
        " because it does not exist";
      throw cta::exception::Exception(msg.str());
    }

    std::string getFilename(const cta::RemotePath &remoteFile) const {
      incCallCounter("getFilename");
      return "always_the_same_filename";
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
        m_callCounts[method] = 1;
      } else {
        itor->second++;
      }
    }
  }; // class TestRemoteNS

}; // class cta_RemoteNSDispatcherTest

TEST_F(cta_RemoteNSDispatcherTest, registerProtocolHandler) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  std::unique_ptr<RemoteNS> ns(new TestRemoteNS());
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns)));
}

TEST_F(cta_RemoteNSDispatcherTest, registerProtocolHandler_twice) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  std::unique_ptr<RemoteNS> ns1(new TestRemoteNS());
  std::unique_ptr<RemoteNS> ns2(new TestRemoteNS());
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns1)));
  ASSERT_THROW(dispatcher.registerProtocolHandler("test", std::move(ns2)),
    std::exception);
}

TEST_F(cta_RemoteNSDispatcherTest, statFile_existing) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  const std::string regularFile = "test:///testfile1";
  TestRemoteNS *testNS = new TestRemoteNS();
  std::unique_ptr<RemoteNS> ns(testNS);
  testNS->m_files.push_back(regularFile);
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns)));

  ASSERT_EQ(0, testNS->getCallCounter("statFile"));
  ASSERT_NO_THROW(dispatcher.statFile(regularFile));
  ASSERT_EQ(1, testNS->getCallCounter("statFile"));
}

TEST_F(cta_RemoteNSDispatcherTest, statFile_non_existing) {
  using namespace cta;

  RemoteNSDispatcher dispatcher;
  const std::string regularFile = "test:///testfile1";
  TestRemoteNS *testNS = new TestRemoteNS();
  std::unique_ptr<RemoteNS> ns(testNS);
  ASSERT_NO_THROW(dispatcher.registerProtocolHandler("test", std::move(ns)));

  ASSERT_EQ(0, testNS->getCallCounter("statFile"));
  ASSERT_THROW(dispatcher.statFile(regularFile), std::exception);
}

} // namespace unitTests
