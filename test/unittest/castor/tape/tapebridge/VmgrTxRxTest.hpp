/******************************************************************************
 *                test/unittest/castor/tape/tapebridge/VmgrTxRxTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_VMGRTXRXTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_VMGRTXRXTEST_HPP 1

#include "test/unittest/test_exception.hpp"
#include "castor/tape/tapebridge/VmgrTxRx.hpp"
#include "castor/tape/utils/utils.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdlib.h>

class VmgrTxRxTest: public CppUnit::TestFixture {
private:
  const char *const m_vid;

public:

  VmgrTxRxTest(): m_vid("I10486") {
  }

  void setUp() {
    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
  }

  void tearDown() {
  }

  void getTapeInfoFromVmgr_std_exception() throw(std::exception) {
    using namespace castor::tape;

    const uint32_t volReqId            =  0;
    const int      netReadWriteTimeout = 55;
    const uint32_t uid                 = getuid();
    const uint32_t gid                 = getgid();
    legacymsg::VmgrTapeInfoMsgBody tapeInfo;
    utils::setBytes(tapeInfo, '\0');

    try {
      tapebridge::VmgrTxRx::getTapeInfoFromVmgr(nullCuuid, volReqId,
        netReadWriteTimeout, uid, gid, m_vid, tapeInfo);
    } catch(castor::exception::Exception &ex) {
      std::string msg = ex.getMessage().str();
      test_exception tx(msg);

      throw tx;
    }

    CPPUNIT_ASSERT_MESSAGE("vid == vsn",
      strcmp(m_vid, tapeInfo.vsn) == 0);
  }

  void testGetTapeInfoFromVmgr() {
    CPPUNIT_ASSERT_NO_THROW_MESSAGE("getTapeInfoFromVmgr", 
      getTapeInfoFromVmgr_std_exception());
  }

  CPPUNIT_TEST_SUITE(VmgrTxRxTest);
  CPPUNIT_TEST(testGetTapeInfoFromVmgr);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(VmgrTxRxTest);

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_VMGRTXRXTEST_HPP
