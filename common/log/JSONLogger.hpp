#pragma once

#include "common/json/object/JSONCObject.hpp"

namespace cta::log {

/**
 *  * This class allows  JSON-represent a FreeSpace object that is only a uint64_t value
 *   * {"freeSpace",42}
 *    */
class JSONLogger : public cta::utils::json::object::JSONCObject{

public:
  JSONLogger();
  ~JSONLogger(){}; 
 void addToObject(const std::string& key, const std::string & value);  
};

}

