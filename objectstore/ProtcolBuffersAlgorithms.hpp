#pragma once

#include <google/protobuf/repeated_field.h>
#include "exception/Exception.hpp"

namespace cta { namespace objectstore { namespace serializers {
void removeString(::google::protobuf::RepeatedPtrField< ::std::string>* field, 
  const std::string & value);

class NotFound: public cta::exception::Exception {
  public:
    NotFound(const std::string & w): cta::exception::Exception(w) {}
  };
  
size_t findString(::google::protobuf::RepeatedPtrField< ::std::string>* field, 
  const std::string & value);

size_t findStringFrom(::google::protobuf::RepeatedPtrField< ::std::string>* field,
  size_t fromIndex, const std::string & value);

}}}