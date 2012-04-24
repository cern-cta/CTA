/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/ConfigParamTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMTEST_HPP 1

#include "castor/tape/tapebridge/ConfigParam.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <memory>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class ConfigParamTest: public CppUnit::TestFixture {
public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testValueAndSourceUNDEFINED() {
    const std::string category = "test_category";
    const std::string name = "test_name";
    const uint32_t value = 1234;
    const ConfigParamSource::Enum source = ConfigParamSource::UNDEFINED;
    std::auto_ptr<ConfigParam<uint32_t> > param;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ConfigParam()",
      param.reset(new ConfigParam<uint32_t>(category, name)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getCategory()",
      category,
      param->getCategory());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getName()",
      name,
      param->getName());

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check setValueAndSource()",
      param->setValueAndSource(value, source));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getValue()",
      value,
      param->getValue());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getSource()",
      source,
      param->getSource());
  }

  void testValueAndSourceENVIRONMENT_VARIABLE() {
    const std::string category = "test_category";
    const std::string name = "test_name";
    const uint32_t value = 1234;
    const ConfigParamSource::Enum source =
      ConfigParamSource::ENVIRONMENT_VARIABLE;
    std::auto_ptr<ConfigParam<uint32_t> > param;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ConfigParam()",
      param.reset(new ConfigParam<uint32_t>(category, name)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getCategory()",
      category,
      param->getCategory());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getName()",
      name,
      param->getName());

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check setValueAndSource()",
      param->setValueAndSource(value, source));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getValue()",
      value,
      param->getValue());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getSource()",
      source,
      param->getSource());
  }

  void testValueAndSourceCASTOR_CONF() {
    const std::string category = "test_category";
    const std::string name = "test_name";
    const uint32_t value = 1234;
    const ConfigParamSource::Enum source = ConfigParamSource::CASTOR_CONF;
    std::auto_ptr<ConfigParam<uint32_t> > param;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ConfigParam()",
      param.reset(new ConfigParam<uint32_t>(category, name)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getCategory()",
      category,
      param->getCategory());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getName()",
      name,
      param->getName());

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check setValueAndSource()",
      param->setValueAndSource(value, source));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getValue()",
      value,
      param->getValue());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getSource()",
      source,
      param->getSource());
  }

  void testValueAndSourceCOMPILE_TIME_DEFAULT() {
    const std::string category = "test_category";
    const std::string name = "test_name";
    const uint32_t value = 1234;
    const ConfigParamSource::Enum source =
      ConfigParamSource::COMPILE_TIME_DEFAULT;
    std::auto_ptr<ConfigParam<uint32_t> > param;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ConfigParam()",
      param.reset(new ConfigParam<uint32_t>(category, name)));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getCategory()",
      category,
      param->getCategory());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getName()",
      name,
      param->getName());

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check setValueAndSource()",
      param->setValueAndSource(value, source));

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getValue()",
      value,
      param->getValue());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check getSource()",
      source,
      param->getSource());
  }

  void testGetOfNoValue() {
    const std::string category = "test_category";
    const std::string name     = "test_name";
    std::auto_ptr<ConfigParam<uint32_t> > param;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ConfigParam()",
      param.reset(new ConfigParam<uint32_t>(category, name)));

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check getValue()",
      param->getValue(),
      castor::exception::NoValue);
  }

  void testGetOfNoSource() {
    const std::string category = "test_category";
    const std::string name     = "test_name";
    std::auto_ptr<ConfigParam<uint32_t> > param;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "Check new ConfigParam()",
      param.reset(new ConfigParam<uint32_t>(category, name)));

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Check getSource()",
      param->getSource(),
      castor::exception::NoValue);
  }

  CPPUNIT_TEST_SUITE(ConfigParamTest);

  CPPUNIT_TEST(testValueAndSourceUNDEFINED);
  CPPUNIT_TEST(testValueAndSourceENVIRONMENT_VARIABLE);
  CPPUNIT_TEST(testValueAndSourceCASTOR_CONF);
  CPPUNIT_TEST(testValueAndSourceCOMPILE_TIME_DEFAULT);
  CPPUNIT_TEST(testGetOfNoValue);
  CPPUNIT_TEST(testGetOfNoSource);

  CPPUNIT_TEST_SUITE_END();
}; // class ConfigParamTest

CPPUNIT_TEST_SUITE_REGISTRATION(ConfigParamTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMTEST_HPP
