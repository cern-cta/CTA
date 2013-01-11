/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/TapeBridgeDaemonTest.hpp
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

#include "castor/tape/tapebridge/TapeBridgeDaemon.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <memory>

namespace castor     {
namespace tape       {
namespace tapebridge {

class TapeBridgeDaemonTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructor() {

    std::auto_ptr<TapeBridgeDaemon> daemon;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new TapeBridgeDaemon",
      daemon.reset(new TapeBridgeDaemon()));
  } // void testConstructor()

  CPPUNIT_TEST_SUITE(TapeBridgeDaemonTest);

  CPPUNIT_TEST(testConstructor);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TapeBridgeDaemonTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor
