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

#include "JSONCTestObject.hpp"
#include "common/json/object/JSONObjectException.hpp"

namespace unitTests {
  
using namespace cta::utils;

TEST(JSONCObjectTest, testJSONGenerationFromObject) {
  JSONCTestObject to;
  to.jsonSetValue("integer_number", 42);
  to.jsonSetValue("str","forty two");
  to.jsonSetValue("double_number", 42.234567);
  ASSERT_EQ("{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.234567}", to.getJSON());
}

TEST(JSONCObjectTest, testObjectGenerationFromJSON){
  std::string json = "{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}";
  JSONCTestObject to(json);

  auto keys = to.getJSONObjectKeys();
  ASSERT_EQ(3, keys.size());
  ASSERT_TRUE(keys.count("integer_number"));
  ASSERT_TRUE(keys.count("str"));
  ASSERT_TRUE(keys.count("double_number"));

  ASSERT_EQ(42, to.jsonGetValue<int>("integer_number"));
  ASSERT_EQ("forty two", to.jsonGetValue<std::string>("str"));
  ASSERT_DOUBLE_EQ(42.000000, to.jsonGetValue<double>("double_number"));
}

TEST(JSONCObjectTest, testJSONCParserSetJSONToBeParsedWrongJSONFormat){
  JSONCTestObject to;
  ASSERT_THROW(to.reset("WRONG_JSON_STRING"),cta::exception::JSONObjectException);
}

TEST(JSONCObjectTest, testJSONCParserRecursiveWithObject){
  std::string json = R"({"integer_number": 42, "str": "forty two", "double_number": 42.234567 , "inner_object": { "integer_number": 777, "str": "seven seven seven", "double_number": 777.777777} })";
  JSONCTestObject to_outer(json);
  auto to_inner = to_outer.jsonGetValue<JSONCTestObject>("inner_object");
  to_outer.reset("{}"); // Force outer object to be destructed

  auto keys = to_inner.getJSONObjectKeys();
  ASSERT_EQ(3, keys.size());
  ASSERT_TRUE(keys.count("integer_number"));
  ASSERT_TRUE(keys.count("str"));
  ASSERT_TRUE(keys.count("double_number"));

  ASSERT_EQ(777, to_inner.jsonGetValue<int>("integer_number"));
  ASSERT_EQ("seven seven seven", to_inner.jsonGetValue<std::string>("str"));
  ASSERT_DOUBLE_EQ(777.777777, to_inner.jsonGetValue<double>("double_number"));
}

TEST(JSONCObjectTest, testJSONCParserRecursiveWithArray){
  std::string json = R"({"integer_number": 42, "str": "forty two", "double_number": 42.234567 , "inner_array": [ 777, "seven seven seven", 777.777777 ] })";
  JSONCTestObject to_outer(json);
  auto to_inner = to_outer.jsonGetValue<std::vector<JSONCTestObject>>("inner_array");
  to_outer.reset("{}"); // Force outer object to be destructed
  ASSERT_EQ(3, to_inner.size());
  ASSERT_EQ(777, to_inner[0].jsonConvertValue<int>());
  ASSERT_EQ("seven seven seven", to_inner[1].jsonConvertValue<std::string>());
  ASSERT_DOUBLE_EQ(777.777777, to_inner[2].jsonConvertValue<double>());
}

} // namespace unitTests
