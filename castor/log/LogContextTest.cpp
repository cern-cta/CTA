/******************************************************************************
 *                      castor/log/LogContextTest.cpp
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
 * @author Eric.Cano@cern.ch
 *****************************************************************************/

#include "DummyLogger.hpp"
#include "LogContext.hpp"

#include <gtest/gtest.h>
#include <xfs/platform_defs.h>

using namespace castor::log;

namespace unitTests {
  TEST(castor_log_LogContextTest, additionScopedRemove) {
    DummyLogger dl("castor_log_LogContextTest");
    LogContext lc(dl);
    lc.pushOrReplace(Param("MigrationRequestId", 123));
    ASSERT_EQ(1U, lc.size());
    {
      LogContext::ScopedParam sp(lc, Param("NSFILEID", 12345));
      ASSERT_EQ(2U, lc.size());
      lc.log(LOG_DEBUG, "Two params message");
      {
        // Test that we do not allow duplicate params
        LogContext::ScopedParam sp2(lc, Param("NSFILEID", 123456));
        ASSERT_EQ(2U, lc.size());
        LogContext::ScopedParam sp3(lc, Param("VID", "T1234"));
        ASSERT_EQ(3U, lc.size());
      }
    }
    ASSERT_EQ(1U, lc.size());
    lc.log(LOG_DEBUG, "One param message");
    lc.erase("MigrationRequestId");
    ASSERT_EQ(0U, lc.size());
  }
}
