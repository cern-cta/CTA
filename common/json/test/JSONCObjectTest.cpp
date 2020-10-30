/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include "JSONCTestObject.hpp"
#include "common/json/object/JSONObjectException.hpp"

namespace unitTests {
  
using namespace cta::utils;

TEST(JSONCObjectTest, testJSONGenerationFromObject) {
  JSONCTestObject to;
  to.double_number = 42.0;
  to.integer_number = 42;
  to.str = "forty two";
  ASSERT_EQ("{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}",to.getJSON());
}

TEST(JSONCObjectTest, testObjectGenerationFromJSON){
  std::string json = "{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}";
  JSONCTestObject to;
  to.setAttributesFromJSON(json);
  ASSERT_EQ(42,to.integer_number);
  ASSERT_EQ("forty two",to.str);
  ASSERT_EQ(42.000000,to.double_number);
}

TEST(JSONCObjectTest, testJSONCParserGetJSONShouldReturnDefaultValues){
  JSONCTestObject to;
  ASSERT_EQ("{\"integer_number\":0,\"str\":\"\",\"double_number\":0.000000}",to.getJSON());
}

TEST(JSONCObjectTest, testJSONCParserSetJSONToBeParsedWrongJSONFormat){
  JSONCTestObject to;
  ASSERT_THROW(to.setAttributesFromJSON("WRONG_JSON_STRING"),cta::exception::JSONObjectException);
}

}