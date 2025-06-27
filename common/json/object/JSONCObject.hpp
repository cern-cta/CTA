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

#include "JSONObject.hpp"
#include <json-c/json.h>
#include <memory>
#include <vector>
#include <set>

namespace cta::utils::json::object {

/**
 * This class allows to build the inherited object or to generate the
 * JSON output of the inherited object by using the JSON-C library
 * https://github.com/json-c/json-c/wiki
 * 
 * The same json_object pointer is used to read from a JSON string
 * and to create a JSON string. Hence this pointer is reinitialized at each call
 * to buildFromJSON() and getJSON()
 */

struct JSONCDeleter {
  void operator()(json_object* obj) const noexcept {
    if (obj) json_object_put(obj);
  }
};

class JSONCObject : public JSONObject {
public:
  JSONCObject();
  explicit JSONCObject(const std::string& json);
  explicit JSONCObject(json_object * obj);
  /**
   * Constructs the inherited object from the json passed in parameter
   * @param json the json to build the object from
   * @throws JSONObjectException if the json provided does not allow to build this object
   */
  virtual void reset(const std::string & json);
  /**
   * Return the inherited object expected JSON structure allowing to set its attributes
   * via the buildFromJSON() method
   * @return an example of JSON allowing to build the object e.g {"freeSpace",42}
   */
  virtual std::string getExpectedJSONToBuildObject() const;
  /**
   * Returns the json representation of the inherited object
   * or null if the json cannot be generated from the inherited object attributes
   */
  virtual std::string getJSON();
  virtual std::string getJSONPretty();

protected:
  std::unique_ptr<json_object, JSONCDeleter> m_jsonObject;

  /**
   * Destroy then initialize the JSON-C representation
   */
  void resetJSONCObject();

  /**
   * This method allows to get JSON-C type of a object, provided a key
   * @param key the key to return the JSON-C type of the object
   * @return the JSON-C representation of the object associated to the key passed in parameter
   */
  json_type getJSONObjectTypeFromKey(const std::string& key) const;

  /**
   * This method allows to get JSON-C type of the object itself
   * @return the JSON-C representation of the object associated to the key passed in parameter
   */
  json_type getJSONObjectType() const;

  /**
   * This method allows to get all keys in a JSON-C object
   * @return the set of key names
   */
  std::set<std::string> getJSONObjectKeys() const;

  /**
   * Helper template
   */
  template<typename T>
  static constexpr bool always_false_v = false;

  /**
   * This method allows to get a value from the JSON-C representation of the object, given a key
   * @params key the key to get the object from it
   * T is the type of the value associated to the key
   */
  template<typename T>
  T jsonGetValue(const std::string& key){
    if constexpr (std::is_same_v<T,JSONCObject>) {
      json_object * jsonObj = getJSONObject(key);
      JSONCObject jsonc(jsonObj);
      return jsonc;
    } else {
      json_object * jsonObj = getJSONObject(key);
      JSONCObject jsonc;
      jsonc.reset(jsonObj);
      return jsonc.jsonConvertValue<T>();
    }
  }

  /**
   * This method allows to get the value from the JSON-C representation
   * T is the type of the value associated to the key
   */
  template<typename T>
  T jsonConvertValue() {
    if constexpr (std::is_same_v<T,bool>) {
      return json_object_get_boolean(m_jsonObject.get());
    } else if constexpr (std::is_integral_v<T>) {
      if constexpr (std::is_signed_v<T>) {
        return json_object_get_int64(m_jsonObject.get());
      } else {
        return json_object_get_uint64(m_jsonObject.get());
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      return json_object_get_double(m_jsonObject.get());
    } else if constexpr (std::is_convertible_v<T,std::string>) {
      return json_object_get_string(m_jsonObject.get());
    } else if constexpr (std::is_same_v<T, std::vector<JSONCObject>>) {
      const size_t size = json_object_array_length(m_jsonObject.get());
      std::vector<JSONCObject> result;
      result.reserve(size);
      for (size_t i = 0; i < size; i++) {
        result.emplace_back(json_object_array_get_idx(m_jsonObject.get(), i));
      }
      return result;
    } else {
      static_assert(always_false_v<T>, "Unsupported type for JSONCObject::jsonConvertValue");
      return T{};
    }
  }

  /**
   * This method allows to create or set an object on this JSON-C object representation
   * @param key the key to create
   * @param value the value associated to the key
   * T is the type of the value associated to the key
   */
  template<typename T>
  void jsonSetValue(const std::string & key, const T & value) {
    json_object * jsonObj = nullptr;
    if constexpr (std::is_same_v<T,bool>) {
      jsonObj = json_object_new_boolean(value);
    } else if constexpr (std::is_integral_v<T>) {
      if constexpr (std::is_signed_v<T>) {
        jsonObj = json_object_new_int64(value);
      } else {
        jsonObj = json_object_new_uint64(value);
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      char buffer[64];
      snprintf(buffer, sizeof(buffer), "%.6f", value);
      jsonObj = json_object_new_double_s(value, buffer);
    } else if constexpr (std::is_convertible_v<T,std::string>) {
      jsonObj = json_object_new_string(std::string(std::move(value)).c_str());
    } else {
      static_assert(always_false_v<T>, "Unsupported type for JSONCObject::jsonSetValue");
    }

    // Add object to JSON
    json_object_object_add(m_jsonObject.get(), key.c_str(), jsonObj);
  }
  
  /**
   * Returns a pointer to the JSON-C representation of the object associated to the key passed in parameter
   * @param key the key to return the JSON-C representation of the object associated to it
   * @return the JSON-C representation of the object associated to the key passed in parameter
   */
  json_object * getJSONObject(const std::string & key) const;

  /**
   * Constructs the inherited object from the json_object passed in parameter
   * @param obj the json_object to build the object from
   * @throws JSONObjectException if the json provided does not allow to build this object
   */
  virtual void reset(json_object * obj);
  
};

} // namespace cta::utils::json::object
