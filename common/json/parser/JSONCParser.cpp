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

#include <json-c/json.h>
#include <json/json_object.h>

#include "JSONCParser.hpp"
#include "common/json/object/SchedulerHints.hpp"
#include "common/json/object/TestObject.hpp"
#include "common/exception/Exception.hpp"
#include "JSONParserException.hpp"

namespace cta { namespace utils { namespace json { namespace parser {

JSONCParser::JSONCParser() {
  m_jsonObject = json_object_new_object();
}

void JSONCParser::setJSONToBeParsed(const std::string& json){
  //DO JSON_C deinitialization
  if(m_jsonObject != nullptr){
    json_object_put(m_jsonObject);
    m_jsonObject = nullptr;
  }
  m_jsonObject = json_tokener_parse(json.c_str());
}

std::string JSONCParser::getJSON() const {
  return std::string(json_object_to_json_string_ext(m_jsonObject, JSON_C_TO_STRING_PLAIN));
}

JSONCParser::~JSONCParser() {
  //Free the JSON object if initialized
  if(m_jsonObject != nullptr){
    json_object_put(m_jsonObject);
    m_jsonObject = nullptr;
  }
}

json_object * JSONCParser::getJSONObject(const std::string& key){
  json_object * objectRet;
  if(json_object_object_get_ex(m_jsonObject,key.c_str(),&objectRet)){
    return objectRet;
  }
  std::string errMsg = "In JSONCParser::getJSONObject(), the provided json does not contain any key named \""+key+"\".";
  throw cta::exception::JSONParserException(errMsg);
}

////////////////////////////////////////////////////////////////////
// JSONCParser::getValue() implementation START
////////////////////////////////////////////////////////////////////
template<>
std::string JSONCParser::getValue(const std::string& key){
  json_object * jsonObj = getJSONObject(key);
  return std::string(json_object_get_string(jsonObj));
}

template<>
uint64_t JSONCParser::getValue(const std::string & key){
  json_object * jsonObj = getJSONObject(key);
  return json_object_get_int64(jsonObj);
}

template<>
double JSONCParser::getValue(const std::string & key){
  json_object * jsonObj = getJSONObject(key);
  return json_object_get_double(jsonObj);
}
////////////////////////////////////////////////////////////////////
// JSONCParser::getValue() implementation END
////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////
// json::object::TestObject implementation START
////////////////////////////////////////////////////////////////////
template<>
void JSONCParser::generateJSONFromObject(const object::TestObject & value){
  json_object_object_add(m_jsonObject,"integer_number",json_object_new_int64(value.integer_number));
  json_object_object_add(m_jsonObject,"str",json_object_new_string(value.str.c_str()));
  json_object_object_add(m_jsonObject,"double_number",json_object_new_double(value.double_number));
}

template<>
object::TestObject JSONCParser::getObjectFromJSON(){
  object::TestObject ret;
  ret.str = getValue<std::string>("str");
  ret.integer_number = getValue<uint64_t>("integer_number");
  ret.double_number = getValue<double>("double_number");
  return ret;
}
////////////////////////////////////////////////////////////////////
// json::object::TestObject implementation END
////////////////////////////////////////////////////////////////////

template <>
void JSONCParser::generateJSONFromObject(const object::SchedulerHints & value){
  
}

}}}}