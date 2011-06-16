/******************************************************************************
 *                test/rtcpd/RtcpdTapeTest.hpp
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
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef TEST_RTCPD_RTCPDTAPETEST_HPP
#define TEST_RTCPD_RTCPDTAPETEST_HPP 1

#include "h/rtcpd_TapeFileWaitingForFlushList.h"
#include "h/serrno.h"

#include <cppunit/extensions/HelperMacros.h>
#include <errno.h>

class RtcpdTapeTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  CPPUNIT_TEST_SUITE(RtcpdTapeTest);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(RtcpdTapeTest);

#endif // TEST_RTCPD_RTCPDTAPETEST_HPP
