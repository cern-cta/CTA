/******************************************************************************
 *                      castor/log/StringLoggerTest.cpp
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

#include "StringLogger.hpp"

#include <gtest/gtest.h>

using namespace castor::log;

namespace unitTests {
  TEST(castor_log_StringLogger, basicTest) {
    std::string jat = "Just a test";
    StringLogger sl("castor_log_StringLogger");
    sl(LOG_INFO, jat);
    ASSERT_NE(std::string::npos, sl.getLog().find(jat));
  }
}
