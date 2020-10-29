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

#include "common/json/parser/JSONCParser.hpp"
#include "common/json/object/TestObject.hpp"
#include "JSONParserException.hpp"

namespace unitTests {
  
using namespace cta::utils;

TEST(JSONCParserTest, testJSONGenerationFromObject) {
  json::object::TestObject to;
  to.double_number = 42.0;
  to.integer_number = 42;
  to.str = "forty two";
  json::parser::JSONCParser parser;
  parser.generateJSONFromObject(to);
  ASSERT_EQ("{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}",parser.getJSON());
}

TEST(JSONCParserTest, testObjectGenerationFromJSON){
  std::string json = "{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}";
  json::parser::JSONCParser parser;
  parser.setJSONToBeParsed(json);
  json::object::TestObject to = parser.getObjectFromJSON<decltype(to)>();
  ASSERT_EQ(42,to.integer_number);
  ASSERT_EQ("forty two",to.str);
  ASSERT_EQ(42.000000,to.double_number);
}

TEST(JSONCParserTest, testJSONCParserGetObjectFromUninitializedJSON){
  json::parser::JSONCParser parser;
  ASSERT_THROW(parser.getObjectFromJSON<json::object::TestObject>(),cta::exception::JSONParserException);
}

TEST(JSONCParserTest, testJSONCParserGetJSONShouldReturnEmptyJSON){
  json::parser::JSONCParser parser;
  ASSERT_EQ("{}",parser.getJSON());
}

TEST(JSONCParserTest, testJSONCParserSetJSONToBeParsedWrongJSONFormat){
  json::parser::JSONCParser parser;
  parser.setJSONToBeParsed("WRONG_JSON_STRING");
  ASSERT_EQ("null",parser.getJSON());
  
  parser.setJSONToBeParsed("{\"test");
  ASSERT_EQ("null",parser.getJSON());
  ASSERT_THROW(parser.getObjectFromJSON<json::object::TestObject>(),cta::exception::JSONParserException);
}

}