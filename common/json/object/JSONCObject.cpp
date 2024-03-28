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

namespace cta::utils::json::object {

JSONCObject::JSONCObject():JSONObject() {
  initializeJSONCObject();
}

void JSONCObject::buildFromJSON(const std::string& json){
  //DO JSON_C deinitialization
  if(m_jsonObject != nullptr){
    destroyJSONCObject();
  }
  m_jsonObject = json_tokener_parse(json.c_str());
}

std::string JSONCObject::getExpectedJSONToBuildObject() const {
  return "{}";
}

std::string JSONCObject::getJSON() {
  return std::string(json_object_to_json_string_ext(m_jsonObject, JSON_C_TO_STRING_PLAIN));
}

void JSONCObject::initializeJSONCObject() {
  m_jsonObject = json_object_new_object();
}

void JSONCObject::destroyJSONCObject() {
  json_object_put(m_jsonObject);
  m_jsonObject = nullptr;
}

void JSONCObject::reinitializeJSONCObject() {
  destroyJSONCObject();
  initializeJSONCObject();
}

json_type JSONCObject::getJSONObjectType(const std::string& key){
  json_object * objectRet;
  if(json_object_object_get_ex(m_jsonObject,key.c_str(),&objectRet)){
    return json_object_get_type(objectRet);
  }
  std::string errMsg = "In JSONCParser::getJSONObject(), the provided json does not contain any key named \""+key+"\".";
  throw cta::exception::JSONObjectException(errMsg);
}

json_object * JSONCObject::getJSONObject(const std::string& key){
  json_object * objectRet;
  if(json_object_object_get_ex(m_jsonObject,key.c_str(),&objectRet)){
    return objectRet;
  }
  std::string errMsg = "In JSONCParser::getJSONObject(), the provided json does not contain any key named \""+key+"\".";
  throw cta::exception::JSONObjectException(errMsg);
}

template<>
std::string JSONCObject::jsonGetValue(const std::string& key){
  json_object * jsonObj = getJSONObject(key);
  return std::string(json_object_get_string(jsonObj));
}

template<>
uint64_t JSONCObject::jsonGetValue(const std::string & key){
  json_object * jsonObj = getJSONObject(key);
  return json_object_get_int64(jsonObj);
}

template<>
int64_t JSONCObject::jsonGetValue(const std::string & key){
  json_object * jsonObj = getJSONObject(key);
  return json_object_get_int64(jsonObj);
}

template<>
double JSONCObject::jsonGetValue(const std::string & key){
  json_object * jsonObj = getJSONObject(key);
  return json_object_get_double(jsonObj);
}

template<>
bool JSONCObject::jsonGetValue(const std::string & key){
  json_object * jsonObj = getJSONObject(key);
  return json_object_get_boolean(jsonObj);
}

template<>
void JSONCObject::jsonSetValue(const std::string& key, const std::string & value){
  json_object_object_add(m_jsonObject,key.c_str(),json_object_new_string(value.c_str()));
}

template<>
void JSONCObject::jsonSetValue(const std::string& key, const double & value){
#ifdef ALMA9
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "%.6f", value);
  json_object_object_add(m_jsonObject,key.c_str(),json_object_new_double_s(value, buffer));
#else
  json_object_object_add(m_jsonObject,key.c_str(),json_object_new_double(value));
#endif
}

template<>
void JSONCObject::jsonSetValue(const std::string& key, const uint64_t & value){
  json_object_object_add(m_jsonObject,key.c_str(),json_object_new_int64(value));
}

template<>
void JSONCObject::jsonSetValue(const std::string& key, const time_t & value){
  json_object_object_add(m_jsonObject,key.c_str(),json_object_new_int64(value));
}


JSONCObject::~JSONCObject() {
  //Free the JSON object if initialized
  if(m_jsonObject != nullptr){
    destroyJSONCObject();
  }
}

} // namespace cta::utils::json::object
