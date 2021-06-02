/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "JSONCTestObject.hpp"

namespace unitTests {

JSONCTestObject::JSONCTestObject():JSONCObject(), TestObject()  {
  
}

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

JSONCTestObject::~JSONCTestObject() {
}

}