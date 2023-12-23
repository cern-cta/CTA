#include "common/log/JSONLogger.hpp"

namespace cta::log {

JSONLogger::JSONLogger(): cta::utils::json::object::JSONCObject() {
}


void JSONLogger::addToObject(const std::string& key, const std::string & value){
  this->jsonSetValue(key, value);
}

} 
