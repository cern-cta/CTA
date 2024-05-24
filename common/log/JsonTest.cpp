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

#include <gtest/gtest.h>
#include <memory>

#include "common/log/Param.hpp"
#include "common/log/StringLogger.hpp"
#include "common/log/LogContext.hpp"

#include "common/json/object/JSONCObject.hpp"
#include "common/json/object/JSONObjectException.hpp"

namespace unitTests {

class JSONCObjectProbe: public cta::utils::json::object::JSONCObject {
public:
  template<typename T>
  T jsonGetValueProbe(const std::string & key) {
    return jsonGetValue<T>(key);
  }
  json_type jsonGetValueType(const std::string & key) {
    return getJSONObjectType(key);
  }
};

class cta_log_JsonTest: public ::testing::Test {
protected:

  void SetUp() {
  }

  void TearDown() {
  }
}; // cta_log_ParamTest

TEST_F(cta_log_JsonTest, testJsonPrinting) {
  using namespace cta::log;

  // Prepare the logger for inspection
  StringLogger logger("dummy", "cta_log_JsonTest", cta::log::DEBUG);
  LogContext logContext(logger);
  logger.setLogFormat("json");

  {
    cta::log::ScopedParamContainer paramsA(logContext);
    paramsA.add("ParamsA_string", "test_value_A");
    paramsA.add("ParamsA_double", 1234.5678);
    paramsA.add("ParamsA_integer", -777);
    paramsA.add("ParamsA_bool", true);
    paramsA.add("ParamsA_null", std::optional<int>());
    logContext.log(INFO, "ParamsA message");
  }

  {
    cta::log::ScopedParamContainer paramsB(logContext);
    paramsB.add("ParamsB_string", "test_value_B");
    paramsB.add("ParamsB_double", 0.0);
    paramsB.add("ParamsB_integer", 999);
    paramsB.add("ParamsB_bool", false);
    paramsB.add("ParamsB_null", std::optional<double>());
    logContext.log(INFO, "ParamsB message");
  }

  std::string logsToCheck = logger.getLog();

  std::vector<std::string> logLines;
  std::stringstream ss(logsToCheck);

  {
    std::string tmpLine;
    while (std::getline(ss, tmpLine)) {
      logLines.push_back(tmpLine);
    }
  }

  ASSERT_EQ(2, logLines.size());

  JSONCObjectProbe jObjA, jObjB;
  jObjA.buildFromJSON(logLines[0]);
  jObjB.buildFromJSON(logLines[1]);

  // Check that JSON is parsed correctly
  ASSERT_NO_THROW(jObjA.getJSON());
  ASSERT_NO_THROW(jObjB.getJSON());

  // Check non-existing key
  ASSERT_THROW(jObjA.jsonGetValueProbe<int64_t>("missing_key"), cta::exception::JSONObjectException);
  ASSERT_THROW(jObjB.jsonGetValueProbe<int64_t>("missing_key"), cta::exception::JSONObjectException);

  // Check epoch time is a floating-point
  ASSERT_EQ(json_type::json_type_double, jObjA.jsonGetValueType("epoch_time"));
  ASSERT_EQ(json_type::json_type_double, jObjB.jsonGetValueType("epoch_time"));

  // Check hostname
  ASSERT_EQ(json_type::json_type_string, jObjA.jsonGetValueType("hostname"));
  ASSERT_EQ(json_type::json_type_string, jObjB.jsonGetValueType("hostname"));
  ASSERT_EQ("dummy", jObjA.jsonGetValueProbe<std::string>("hostname"));
  ASSERT_EQ("dummy", jObjB.jsonGetValueProbe<std::string>("hostname"));

  // Check programme
  ASSERT_EQ(json_type::json_type_string, jObjA.jsonGetValueType("program"));
  ASSERT_EQ(json_type::json_type_string, jObjB.jsonGetValueType("program"));
  ASSERT_EQ("cta_log_JsonTest", jObjA.jsonGetValueProbe<std::string>("program"));
  ASSERT_EQ("cta_log_JsonTest", jObjB.jsonGetValueProbe<std::string>("program"));

  // Check log level
  ASSERT_EQ(json_type::json_type_string, jObjA.jsonGetValueType("log_level"));
  ASSERT_EQ(json_type::json_type_string, jObjB.jsonGetValueType("log_level"));
  ASSERT_EQ("INFO", jObjA.jsonGetValueProbe<std::string>("log_level"));
  ASSERT_EQ("INFO", jObjB.jsonGetValueProbe<std::string>("log_level"));

  // Check parameters and expected types
  ASSERT_EQ(json_type::json_type_string, jObjA.jsonGetValueType("ParamsA_string"));
  ASSERT_EQ(json_type::json_type_string, jObjB.jsonGetValueType("ParamsB_string"));
  ASSERT_EQ("test_value_A", jObjA.jsonGetValueProbe<std::string>("ParamsA_string"));
  ASSERT_EQ("test_value_B", jObjB.jsonGetValueProbe<std::string>("ParamsB_string"));

  ASSERT_EQ(json_type::json_type_double, jObjA.jsonGetValueType("ParamsA_double"));
  ASSERT_EQ(json_type::json_type_double, jObjB.jsonGetValueType("ParamsB_double"));
  ASSERT_DOUBLE_EQ(1234.5678, jObjA.jsonGetValueProbe<double>("ParamsA_double"));
  ASSERT_DOUBLE_EQ(0.0, jObjB.jsonGetValueProbe<double>("ParamsB_double"));

  ASSERT_EQ(json_type::json_type_int, jObjA.jsonGetValueType("ParamsA_integer"));
  ASSERT_EQ(json_type::json_type_int, jObjB.jsonGetValueType("ParamsB_integer"));
  ASSERT_EQ(-777, jObjA.jsonGetValueProbe<int64_t>("ParamsA_integer"));
  ASSERT_EQ(999, jObjB.jsonGetValueProbe<int64_t>("ParamsB_integer"));

  ASSERT_EQ(json_type::json_type_boolean, jObjA.jsonGetValueType("ParamsA_bool"));
  ASSERT_EQ(json_type::json_type_boolean, jObjB.jsonGetValueType("ParamsB_bool"));
  ASSERT_EQ(true, jObjA.jsonGetValueProbe<int64_t>("ParamsA_bool"));
  ASSERT_EQ(false, jObjB.jsonGetValueProbe<int64_t>("ParamsB_bool"));

  ASSERT_EQ(json_type::json_type_null, jObjA.jsonGetValueType("ParamsA_null"));
  ASSERT_EQ(json_type::json_type_null, jObjB.jsonGetValueType("ParamsB_null"));
  ASSERT_EQ(0, jObjA.jsonGetValueProbe<int64_t>("ParamsA_null"));
  ASSERT_EQ(0, jObjB.jsonGetValueProbe<int64_t>("ParamsB_null"));
}

TEST_F(cta_log_JsonTest, testJsonStringEscape) {
  using namespace cta::log;

  // Prepare the logger for inspection
  StringLogger logger("dummy", "cta_log_JsonTest", cta::log::DEBUG);
  LogContext logContext(logger);
  logger.setLogFormat("json");

  {
    cta::log::ScopedParamContainer paramsA(logContext);
    paramsA.add("key_\"", "value_\"");
    paramsA.add("key_\\", "value_\\");
    paramsA.add("key_\b", "value_\b");
    paramsA.add("key_\n", "value_\n");
    paramsA.add("key_\f", "value_\f");
    paramsA.add("key_\r", "value_\r");
    paramsA.add("key_\t", "value_\t");
    paramsA.add("key_\x00", "value_\x00");
    paramsA.add("key_\x1f", "value_\x1f");
    paramsA.add("key_\x20", "value_\x20"); //This is a whitespace character
    logContext.log(INFO, "Testing escaped values");
  }

  std::string logLine = logger.getLog();

  JSONCObjectProbe jObj;
  jObj.buildFromJSON(logLine);

  // Check that JSON is parsed correctly
  ASSERT_NO_THROW(jObj.getJSON());

  // Check expected keys and values
  // Strings should be converted back to cpp with the escape characters correctly decoded
  ASSERT_EQ("value_\"", jObj.jsonGetValueProbe<std::string>("key_\""));
  ASSERT_EQ("value_\\", jObj.jsonGetValueProbe<std::string>("key_\\"));
  ASSERT_EQ("value_\b", jObj.jsonGetValueProbe<std::string>("key_\b"));
  ASSERT_EQ("value_\n", jObj.jsonGetValueProbe<std::string>("key_\n"));
  ASSERT_EQ("value_\f", jObj.jsonGetValueProbe<std::string>("key_\f"));
  ASSERT_EQ("value_\r", jObj.jsonGetValueProbe<std::string>("key_\r"));
  ASSERT_EQ("value_\t", jObj.jsonGetValueProbe<std::string>("key_\t"));
  ASSERT_EQ("value_\x00", jObj.jsonGetValueProbe<std::string>("key_\x00"));
  ASSERT_EQ("value_\x1f", jObj.jsonGetValueProbe<std::string>("key_\x1f"));
  ASSERT_EQ("value_ ", jObj.jsonGetValueProbe<std::string>("key_ "));
}

} // namespace unitTests
