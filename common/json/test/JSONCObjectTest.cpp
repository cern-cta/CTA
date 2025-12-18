/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JSONCTestObject.hpp"
#include "common/json/object/JSONObjectException.hpp"

#include <gtest/gtest.h>

namespace unitTests {

using namespace cta::utils;

TEST(JSONCObjectTest, testJSONGenerationFromObject) {
  JSONCTestObject to;
  to.double_number = 42.234567;
  to.integer_number = 42;
  to.str = "forty two";
  ASSERT_EQ("{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.234567}", to.getJSON());
}

TEST(JSONCObjectTest, testObjectGenerationFromJSON) {
  std::string json = "{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}";
  JSONCTestObject to;
  to.buildFromJSON(json);
  ASSERT_EQ(42, to.integer_number);
  ASSERT_EQ("forty two", to.str);
  ASSERT_EQ(42.000000, to.double_number);
}

TEST(JSONCObjectTest, testJSONCParserGetJSONShouldReturnDefaultValues) {
  JSONCTestObject to;
  ASSERT_EQ("{\"integer_number\":0,\"str\":\"\",\"double_number\":0.000000}", to.getJSON());
}

TEST(JSONCObjectTest, testJSONCParserSetJSONToBeParsedWrongJSONFormat) {
  JSONCTestObject to;
  ASSERT_THROW(to.buildFromJSON("WRONG_JSON_STRING"), cta::exception::JSONObjectException);
}

}  // namespace unitTests
