/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/VdqmRequestHandlerTest.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, fileToMigrate.or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, fileToMigrate.write to the Free Software
 * Foundation, fileToMigrate.Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/tapebridge/VdqmRequestHandler.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <memory>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class VdqmRequestHandlerTest: public CppUnit::TestFixture {
public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testConstructor() {
    const BulkRequestConfigParams bulkRequestConfigParams;
    const TapeFlushConfigParams tapeFlushConfigParams;
    const uint32_t nbDrives = 0;
    std::auto_ptr<VdqmRequestHandler> handler;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new VdqmRequestHandler()",
      handler.reset(new VdqmRequestHandler(bulkRequestConfigParams,
        tapeFlushConfigParams, nbDrives)));
  }

  CPPUNIT_TEST_SUITE(VdqmRequestHandlerTest);

  CPPUNIT_TEST(testConstructor);

  CPPUNIT_TEST_SUITE_END();
}; // class VdqmRequestHandlerTest

CPPUNIT_TEST_SUITE_REGISTRATION(VdqmRequestHandlerTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor
