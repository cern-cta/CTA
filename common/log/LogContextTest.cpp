/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Interface to the CASTOR logging system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>

using namespace cta::log;

namespace unitTests {
  TEST(cta_log_LogContextTest, additionScopedRemove) {
    DummyLogger dl("cta_log_LogContextTest");
    LogContext lc(dl);
    lc.pushOrReplace(Param("MigrationRequestId", 123));
    ASSERT_EQ(1U, lc.size());
    {
      // Create an anonymous variable (for its scope)
      LogContext::ScopedParam sp(lc, Param("archiveFileID", 12345));
      ASSERT_EQ(2U, lc.size());
      lc.log(DEBUG, "Two params message");
      {
        // Test that we do not allow duplicate params
        LogContext::ScopedParam sp(lc, Param("archiveFileID", 123456));
        ASSERT_EQ(2U, lc.size());
        LogContext::ScopedParam sp2(lc, Param("TPVID", "T1234"));
        ASSERT_EQ(3U, lc.size());
      }
    }
    ASSERT_EQ(1U, lc.size());
    lc.log(DEBUG, "One param message");
    lc.erase("MigrationRequestId");
    ASSERT_EQ(0U, lc.size());
  }
  
  TEST(cta_log_LogContextTest, paramsFound) {
    StringLogger sl ("cta_log_LogContextTest", DEBUG);
    LogContext lc(sl);
    lc.pushOrReplace(Param("MigrationRequestId", 123));
    lc.log(INFO, "First log");
    std::string first = sl.getLog();
    ASSERT_NE(std::string::npos, first.find("MigrationRequestId"));
    {
      LogContext::ScopedParam sp(lc, Param("archiveFileID", 12345));
      lc.log(INFO, "Second log");
    }
    std::string second = sl.getLog();
    ASSERT_NE(std::string::npos, second.find("archiveFileID"));
    // We expect the NSFILEID parameter to show up only once (i.e, not after 
    // offset, which marks the end of its first occurrence).
    lc.log(INFO, "Third log");
    std::string third = sl.getLog();
    size_t offset  = third.find("archiveFileID") + strlen("archiveFileID");
    ASSERT_EQ(std::string::npos, third.find("archiveFileID", offset));
  }
}
