/******************************************************************************
 *                test/tapebridge/SendTapeBridgeFlushedToTapeTest.hpp
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

#ifndef TEST_TAPEBRIDGE_SENDTAPEBRIDGEFLUSHEDTOTAPE_HPP
#define TEST_TAPEBRIDGE_SENDTAPEBRIDGEFLUSHEDTOTAPE_HPP 1

#include "test_exception.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/TapeBridgeMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "h/marshall.h"
#include "h/osdep.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"

#include <cppunit/extensions/HelperMacros.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>

class SendTapeBridgeFlushedToTapeTest: public CppUnit::TestFixture {
private:

  int createListenerSock_stdException(const char *addr,
    const unsigned short lowPort, const unsigned short highPort,
    unsigned short &chosenPort) {
    try {
      return castor::tape::net::createListenerSock(addr, lowPort, highPort,
        chosenPort);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());

      throw te;
    }
  }

public:
  void setUp() {
  }

  void tearDown() {
  }

  void testSend() {
  }

  CPPUNIT_TEST_SUITE(SendTapeBridgeFlushedToTapeTest);
  CPPUNIT_TEST(testSend);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(SendTapeBridgeFlushedToTapeTest);

#endif // TEST_TAPEBRIDGE_SENDTAPEBRIDGEFLUSHEDTOTAPE_HPP
