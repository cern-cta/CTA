#pragma once

#include <google/protobuf/repeated_field.h>

namespace cta { namespace objectstore { namespace serializers {
void removeString(::google::protobuf::RepeatedPtrField< ::std::string>* field, 
  const std::string & value);

}}}