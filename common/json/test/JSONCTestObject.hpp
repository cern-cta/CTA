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

#pragma once

#include "common/json/object/JSONCObject.hpp"

using namespace cta::utils::json;

namespace unitTests {

/**
 * This class is only use to unit test the JSONCObject class
 */
class JSONCTestObject : public object::JSONCObject {
public:
  using JSONCObject::JSONCObject;
  explicit JSONCTestObject(JSONCObject && base) : JSONCObject(std::move(base)) {}

  template<typename T>
  T jsonGetValueProbe(const std::string & key) {
    return JSONCObject::jsonGetValue<T>(key);
  }

  template<typename T>
  void jsonSetValue(const std::string & key, const T & value) {
    return JSONCObject::jsonSetValue<T>(key,value);
  }

  template<typename T>
  T jsonGetValue(const std::string& key) {
    if constexpr (std::is_same_v<T, JSONCTestObject>) {
      return JSONCTestObject{ std::move(JSONCObject::jsonGetValue<JSONCObject>(key)) };
    } else if constexpr (std::is_same_v<T, std::vector<JSONCTestObject>>) {
      auto jsoncVector = JSONCObject::jsonGetValue<std::vector<JSONCObject>>(key);
      std::vector<JSONCTestObject> returnVector;
      returnVector.reserve(jsoncVector.size());
      for (auto & item : jsoncVector) {
        returnVector.emplace_back(std::move(item));
      }
      return returnVector;
    } else {
      return JSONCObject::jsonGetValue<T>(key);
    }
  }

  template<typename T>
  T jsonConvertValue() {
    return JSONCObject::jsonConvertValue<T>();
  }

  json_type jsonGetValueType(const std::string & key) {
    return JSONCObject::getJSONObjectType(key);
  }

  std::set<std::string> getJSONObjectKeys() {
    return JSONCObject::getJSONObjectKeys();
  }
};

}
