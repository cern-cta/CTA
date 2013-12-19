/******************************************************************************
 *                test/unittest/castor/log/LoggerImplementationTest.hpp
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

#include "castor/log/Constants.hpp"
#include "castor/log/LoggerImplementation.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <memory>

namespace castor {
namespace log {

class LoggerImplementationTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructorTooLongProgramName() {
    // Create a program name that is 1 character too long
    std::string tooLongProgname;
    for(size_t i = 0; i <= LOG_MAX_PROGNAMELEN; i++) {
      tooLongProgname += 'X';
    }

    {
      std::auto_ptr<Logger> logger;
      CPPUNIT_ASSERT_THROW_MESSAGE(
        "Checking a program name that is too long throws InvalidArgument",
        logger.reset(new LoggerImplementation(tooLongProgname)),
        castor::exception::InvalidArgument);
    }
  }

  CPPUNIT_TEST_SUITE(LoggerImplementationTest);
  CPPUNIT_TEST(testConstructorTooLongProgramName);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(LoggerImplementationTest);

} // namespace log
} // namespace castor
