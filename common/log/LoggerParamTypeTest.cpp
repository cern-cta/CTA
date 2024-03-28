/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/log/SyslogLogger.hpp"
#include "common/log/TestingSyslogLogger.hpp"
#include "common/log/Param.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <sys/time.h>

using namespace cta::log;

namespace unitTests {

class LoggerParamTypeTest: public ::testing::Test {};

TEST_F(LoggerParamTypeTest, TestParams_int64_t) {

  // All of these will be stored as `int64_t`
  // Keep a list of params and expected results
  std::list<std::pair<Param, int64_t>> params_results;
  params_results.push_back({Param("int8_t"  , static_cast<int8_t>(-10))   ,-10});
  params_results.push_back({Param("int16_t" , static_cast<int16_t>(-20))  ,-20});
  params_results.push_back({Param("int32_t" , static_cast<int32_t>(-30))  ,-30});
  params_results.push_back({Param("int64_t" , static_cast<int64_t>(-40))  ,-40});
  params_results.push_back({Param("int"     , static_cast<int>(50))       , 50});
  params_results.push_back({Param("short"   , static_cast<short>(60))     , 60});
  params_results.push_back({Param("long"    , static_cast<long>(70))      , 70});
  // Chars will be treated as integers, since they fulfil `std::is_integral_v<T>`.
  // Change them to be stored as strings only if there is a clear use-case.
  params_results.push_back({Param("char", 'a'), 97});
  // Test optional values too
  params_results.push_back({Param("std::optional<int64_t>" , std::optional<int64_t>(-456)),-456});

  for (auto & [param, expected_val] : params_results) {
    ASSERT_TRUE(std::holds_alternative<int64_t>(param.getValueVariant().value())) << "Failed testing Param type of '" << param.getName() << "'";
    ASSERT_EQ(std::get<int64_t>(param.getValueVariant().value()), expected_val) << "Failed testing Param value of '" << param.getName() << "'";
  }
}

TEST_F(LoggerParamTypeTest, TestParams_uint64_t) {

  // All of these will be stored as `uint64_t`
  // Keep a list of params and expected results
  std::list<std::pair<Param, int64_t>> params_results;
  params_results.push_back({Param("uint8_t"       , static_cast<uint8_t>(10u))  , 10u});
  params_results.push_back({Param("uint16_t"      , static_cast<uint16_t>(20u)) , 20u});
  params_results.push_back({Param("uint32_t"      , static_cast<uint32_t>(30u)) , 30u});
  params_results.push_back({Param("uint64_t"      , static_cast<uint64_t>(40u)) , 40u});
  params_results.push_back({Param("unsigned int"  , static_cast<unsigned int>(50))  , 50u});
  params_results.push_back({Param("unsigned short", static_cast<unsigned short>(60)), 60u});
  params_results.push_back({Param("unsigned long" , static_cast<unsigned long>(70)) , 70u});
  // Unsigned chars will be treated as integers, since they fulfil `std::is_integral_v<T>`.
  params_results.push_back({Param("unsigned char", static_cast<unsigned char>(0x7F)), 127u});
  // Test optional values too
  params_results.push_back({Param("std::optional<uint64_t>", std::optional<uint64_t>(123)), 123u});

  for (auto & [param, expected_val] : params_results) {
    ASSERT_TRUE(std::holds_alternative<uint64_t>(param.getValueVariant().value())) << "Failed testing Param type of '" << param.getName() << "'";
    ASSERT_EQ(std::get<uint64_t>(param.getValueVariant().value()), expected_val) << "Failed testing Param value of '" << param.getName() << "'";
  }
}

TEST_F(LoggerParamTypeTest, TestParams_float) {

  // All of these will be stored as `double`
  // Keep a list of params and expected results
  std::list<std::pair<Param, float>> params_results;
  params_results.push_back({Param("float" , static_cast<float>(1000.123)) , 1000.123});
  // Test optional values too
  params_results.push_back({Param("std::optional<float>" , std::optional<float>(123.456))  , 123.456});

  for (auto & [param, expected_val] : params_results) {
    ASSERT_TRUE(std::holds_alternative<float>(param.getValueVariant().value())) << "Failed testing Param type of '" << param.getName() << "'";
    ASSERT_FLOAT_EQ(std::get<float>(param.getValueVariant().value()), expected_val) << "Failed testing Param value of '" << param.getName() << "'";
  }
}

TEST_F(LoggerParamTypeTest, TestParams_double) {

  // All of these will be stored as `double`
  // Keep a list of params and expected results
  std::list<std::pair<Param, double>> params_results;
  params_results.push_back({Param("double"     , static_cast<double>(1000.123))     , 1000.123});
  params_results.push_back({Param("long double", static_cast<long double>(2000.234)), 2000.234});
  // Test optional values too
  params_results.push_back({Param("std::optional<double>"     , std::optional<double>(123.456))      , 123.456});
  params_results.push_back({Param("std::optional<long double>", std::optional<long double>(-456.789)),-456.789});

  for (auto & [param, expected_val] : params_results) {
    ASSERT_TRUE(std::holds_alternative<double>(param.getValueVariant().value())) << "Failed testing Param type of '" << param.getName() << "'";
    ASSERT_DOUBLE_EQ(std::get<double>(param.getValueVariant().value()), expected_val) << "Failed testing Param value of '" << param.getName() << "'";
  }
}

TEST_F(LoggerParamTypeTest, TestParams_bool) {

  // All of these will be stored as `bool`
  // Keep a list of params and expected results
  std::list<std::pair<Param, double>> params_results;
  params_results.push_back({Param("true" , true) , true });
  params_results.push_back({Param("false", false), false});
  // Test optional values too
  params_results.push_back({Param("std::optional<bool> true" , std::optional<bool>(true)) , true });
  params_results.push_back({Param("std::optional<bool> false", std::optional<bool>(false)), false});

  for (auto & [param, expected_val] : params_results) {
    ASSERT_TRUE(std::holds_alternative<bool>(param.getValueVariant().value())) << "Failed testing Param type of '" << param.getName() << "'";
    ASSERT_EQ(std::get<bool>(param.getValueVariant().value()), expected_val) << "Failed testing Param value of '" << param.getName() << "'";
  }
}

TEST_F(LoggerParamTypeTest, TestParams_string) {

  // All of these will be stored as `std::string`
  // Keep a list of params and expected results
  std::list<std::pair<Param, std::string>> params_results;
  params_results.push_back({Param("std::string"     , std::string("string"))          , "string"     });
  params_results.push_back({Param("std::string_view", std::string_view("string_view")), "string_view"});
  params_results.push_back({Param("char*"           , "char*")                        , "char*"      });
  // Test optional values too
  params_results.push_back({Param("std::optional<std::string>" , std::optional<std::string>("optional_str")) , "optional_str"});

  for (auto & [param, expected_val] : params_results) {
    ASSERT_TRUE(std::holds_alternative<std::string>(param.getValueVariant().value())) << "Failed testing Param type of '" << param.getName() << "'";
    ASSERT_EQ(std::get<std::string>(param.getValueVariant().value()), expected_val) << "Failed testing Param value of '" << param.getName() << "'";
  }
}

TEST_F(LoggerParamTypeTest, TestParams_nullopt) {

  // All of these will be stored as `std::nullopt_t`
  // Keep a list of params and expected results
  std::list<Param> params;
  params.push_back(Param("std::optional<int64_t>"     , std::optional<int64_t>()));
  params.push_back(Param("std::optional<double>"      , std::optional<double>()));
  params.push_back(Param("std::optional<bool>"        , std::optional<bool>()));
  params.push_back(Param("std::optional<std::string>" , std::optional<std::string>()));
  params.push_back(Param("std::nullopt"               , std::nullopt));

  for (auto & param : params) {
    ASSERT_FALSE(param.getValueVariant().has_value()) << "Failed testing Param type of empty '" << param.getName() << "'";
  }
}

} // namespace unitTests
