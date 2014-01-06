/******************************************************************************
 *                test/unittest/castor/log/ParamTest.hpp
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

  void testConstructorForString() {
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", "Value")));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      std::string("Value"),
      param->getStrValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_STR,
      param->getType());
  }

  void testConstructorForUuid() {
    const Cuuid_t &value = nullCuuid;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_UUID,
      param->getType());
  }

  void testConstructorForSubRequestUuid() {
    const Cuuid_t &value = nullCuuid;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param(value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string(""),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_UUID,
      param->getType());
  }

  void testConstructorForLongInt() {
    const long int value = 1234;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
#if defined __x86_64__
    CPPUNIT_ASSERT_EQUAL(
      (u_signed64)value,
      param->getUint64Value());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_INT64,
      param->getType());
#else
    CPPUNIT_ASSERT_EQUAL(
      value,
      param->getIntValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_INT,
      param->getType());
#endif
  }

  void testConstructorForUnsignedLongInt() {
    const long unsigned int value = 1234;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
#if defined __x86_64__
    CPPUNIT_ASSERT_EQUAL(
      (u_signed64)value,
      param->getUint64Value());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_INT64,
      param->getType());
#else
    CPPUNIT_ASSERT_EQUAL(
      value,
      param->getIntValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_INT,
      param->getType());
#endif
  }

  void testConstructorForInt() {
    const int value = 1234;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      value,
      param->getIntValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_INT,
      param->getType());
  }

  void testConstructorForUnsignedInt() {
    const unsigned int value = 1234;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      (int)value,
      param->getIntValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_INT,
      param->getType());
  }

  void testConstructorForU_signed64() {
    const u_signed64 value = 1234;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      value,
      param->getUint64Value());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_INT64,
      param->getType());
  }

  void testConstructorForFloat() {
    const float value = 1234.5678;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      (double)value,
      param->getDoubleValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_DOUBLE,
      param->getType());
  }

  void testConstructorForDouble() {
    const double value = 1234.5678;
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      value,
      param->getDoubleValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_DOUBLE,
      param->getType());
  }

  void testConstructorForRawParams() {
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name1=Value1,Name2=Value2")));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name1=Value1,Name2=Value2"),
      param->getStrValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_RAW,
      param->getType());
  }

  void testConstructorForIPAddress() {
    IPAddress value(0x01020304);
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
   CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      std::string("1.2.3.4"),
      param->getStrValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_STR,
      param->getType());
  }

  void testConstructorForTimeStamp() {
    const TimeStamp value(0);
    std::auto_ptr<Param> param;

    CPPUNIT_ASSERT_NO_THROW(
      param.reset(new Param("Name", value)));
    CPPUNIT_ASSERT_EQUAL(
      std::string("Name"),
      param->getName());
    CPPUNIT_ASSERT_EQUAL(
      std::string(" 1/1 1:0:0"),
      param->getStrValue());
    CPPUNIT_ASSERT_EQUAL(
      LOG_MSG_PARAM_STR,
      param->getType());
  }

  CPPUNIT_TEST_SUITE(ParamTest);
  CPPUNIT_TEST(testConstructorForString);
  CPPUNIT_TEST(testConstructorForUuid);
  CPPUNIT_TEST(testConstructorForSubRequestUuid);
  CPPUNIT_TEST(testConstructorForLongInt);
  CPPUNIT_TEST(testConstructorForUnsignedLongInt);
  CPPUNIT_TEST(testConstructorForInt);
  CPPUNIT_TEST(testConstructorForUnsignedInt);
  CPPUNIT_TEST(testConstructorForU_signed64);
  CPPUNIT_TEST(testConstructorForFloat);
  CPPUNIT_TEST(testConstructorForDouble);
  CPPUNIT_TEST(testConstructorForRawParams);
  CPPUNIT_TEST(testConstructorForIPAddress);
  CPPUNIT_TEST(testConstructorForTimeStamp);
  //CPPUNIT_TEST(testConstructorForIObject);
  
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(ParamTest);

} // namespace log
} // namespace castor
