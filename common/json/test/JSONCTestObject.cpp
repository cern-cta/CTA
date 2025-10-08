/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include "JSONCTestObject.hpp"

namespace unitTests {

JSONCTestObject::JSONCTestObject():JSONCObject(), TestObject()  { }

void JSONCTestObject::buildFromJSON(const std::string & json){
  JSONCObject::buildFromJSON(json);
  double_number = jsonGetValue<double>("double_number");
  integer_number = jsonGetValue<uint64_t>("integer_number");
  str = jsonGetValue<std::string>("str");
}

std::string JSONCTestObject::getJSON() {
  reinitializeJSONCObject();
  jsonSetValue("integer_number",integer_number);
  jsonSetValue("str",str);
  jsonSetValue("double_number",double_number);
  return JSONCObject::getJSON();
}

std::string JSONCTestObject::getExpectedJSONToBuildObject() const {
  return "{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}";
}

}
