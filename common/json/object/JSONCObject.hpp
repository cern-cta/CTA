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

namespace cta { namespace utils { namespace json { namespace object { 

/**
 * This class allows to build the inherited object or to generate the
 * JSON output of the inherited object by using the JSON-C library
 * https://github.com/json-c/json-c/wiki
 * 
 * The same json_object pointer is used to read from a JSON string
 * and to create a JSON string. Hence this pointer is reinitialized at each call
 * to buildFromJSON() and getJSON()
 */
class JSONCObject : public JSONObject {
public:
  JSONCObject();
  /**
   * !!! This method must be called in the first line of same method in the inherited object
   * Constructs the inherited object from the json passed in parameter
   * @param json the json to build the object from
   * @throws JSONObjectException if the json provided does not allow to build this object
   */
  virtual void buildFromJSON(const std::string & json);
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
  virtual ~JSONCObject();
protected:
  json_object * m_jsonObject = nullptr;
  
  /**
   * Initialize the JSON representation of this object
   * 
   */
  void initializeJSONCObject();
  /**
   * Destroy the JSON representation of this object
   */
  void destroyJSONCObject();
  /**
   * Destroy then initialize the JSON-C representation
   */
  void reinitializeJSONCObject();
  
  /**
   * This method allows to get the value from the JSON-C representation of the object
   * @params key the key to get the object from it
   * T is the type of the value associated to the key
   */
  template<typename T>
  T jsonGetValue(const std::string & key);
  
  /**
   * This method allows to create or set an object on this JSON-C object representation
   * @param key the key to create
   * @param value the value associated to the key
   * T is the type of the value associated to the key
   */
  template<typename T>
  void jsonSetValue(const std::string & key, const T & value);
  
  /**
   * Returns a pointer to the JSON-C representation of the object associated to the key passed in parameter
   * @param key the key to return the JSON-C representation of the object associated to it
   * @return the JSON-C representation of the object associated to the key passed in parameter
   */
  json_object * getJSONObject(const std::string & key);
  
};

}}}}