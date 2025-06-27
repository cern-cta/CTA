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

#include "JSONCObject.hpp"
#include "JSONObjectException.hpp"
#include <vector>

namespace cta::utils::json::object {

JSONCObject::JSONCObject() : m_jsonObject(json_object_new_object()) {}
JSONCObject::JSONCObject(const std::string& json) : m_jsonObject(json_tokener_parse(json.c_str())) {
  if (!m_jsonObject) {
    std::string errMsg = "In JSONCObject::reset(), the provided string is an invalid JSON.";
    throw cta::exception::JSONObjectException(errMsg);
  }
}
JSONCObject::JSONCObject(json_object * obj) : m_jsonObject(json_object_get(obj)) {}

void JSONCObject::reset(const std::string& json){
  m_jsonObject.reset(json_tokener_parse(json.c_str()));
  if (!m_jsonObject) {
    std::string errMsg = "In JSONCObject::reset(), the provided string is an invalid JSON.";
    throw cta::exception::JSONObjectException(errMsg);
  }
}

void JSONCObject::reset(json_object * obj) {
  m_jsonObject.reset(json_object_get(obj));
}

std::string JSONCObject::getExpectedJSONToBuildObject() const {
  return "{}";
}

std::string JSONCObject::getJSON() {
  return {json_object_to_json_string_ext(m_jsonObject.get(), JSON_C_TO_STRING_PLAIN)};
}

std::string JSONCObject::getJSONPretty() {
  return {json_object_to_json_string_ext(m_jsonObject.get(), JSON_C_TO_STRING_PRETTY)};
}

void JSONCObject::resetJSONCObject() {
  m_jsonObject.reset(json_object_new_object());
}

json_type JSONCObject::getJSONObjectType(const std::string& key) const {
  json_object * objectRet;
  if(json_object_object_get_ex(m_jsonObject.get(),key.c_str(),&objectRet)){
    return json_object_get_type(objectRet);
  }
  std::string errMsg = "In JSONCObject::getJSONObjectType(), the provided json does not contain any key named \""+key+"\".";
  throw cta::exception::JSONObjectException(errMsg);
}

json_object * JSONCObject::getJSONObject(const std::string& key) const {
  json_object * objectRet;
  if(json_object_object_get_ex(m_jsonObject.get(),key.c_str(),&objectRet)){
    return objectRet;
  }
  std::string errMsg = "In JSONCObject::getJSONObject(), the provided json does not contain any key named \""+key+"\".";
  throw cta::exception::JSONObjectException(errMsg);
}
/*
template<>
std::string JSONCObject::jsonConvertValue() {
  return {json_object_get_string(m_jsonObject.get())};
}

template<>
uint64_t JSONCObject::jsonConvertValue() {
  return json_object_get_uint64(m_jsonObject.get());
}

template<>
int64_t JSONCObject::jsonConvertValue() {
  return json_object_get_int64(m_jsonObject.get());
}

template<>
double JSONCObject::jsonConvertValue() {
  return json_object_get_double(m_jsonObject.get());
}

template<>
bool JSONCObject::jsonConvertValue() {
  return json_object_get_boolean(m_jsonObject.get());
}

template<>
std::vector<JSONCObject> JSONCObject::jsonConvertValue(){
  const size_t size = json_object_array_length(m_jsonObject.get());
  std::vector<JSONCObject> result;
  result.reserve(size);
  for (size_t i = 0; i < size; i++) {
    result.emplace_back(json_object_array_get_idx(m_jsonObject.get(), i));
  }
  return result;
}*/

} // namespace cta::utils::json::object
