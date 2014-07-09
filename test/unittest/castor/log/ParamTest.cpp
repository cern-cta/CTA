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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/log/Param.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <memory>

namespace castor {
namespace log {

class ParamTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructorWithAString() {
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", "Value")));
    CPPUNIT_ASSERT_EQUAL(std::string("Name"), param->getName());
    CPPUNIT_ASSERT_EQUAL(std::string("Value"), param->getValue());
  }

  void testConstructorWithAnInt() {
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(param.reset(new Param("Name", 1234)));
    CPPUNIT_ASSERT_EQUAL(std::string("Name"), param->getName());
    CPPUNIT_ASSERT_EQUAL(std::string("1234"), param->getValue());
  }

  CPPUNIT_TEST_SUITE(ParamTest);
  CPPUNIT_TEST(testConstructorWithAString);
  CPPUNIT_TEST(testConstructorWithAnInt);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(ParamTest);

} // namespace log
} // namespace castor
