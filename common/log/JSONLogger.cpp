#include "common/log/JSONLogger.hpp"

namespace cta::log {

JSONLogger::JSONLogger() : cta::utils::json::object::JSONCObject() {
}

void JSONLogger::addToObject(std::string_view key, std::string_view value) {
  jsonSetValue(key, value);
}

}
