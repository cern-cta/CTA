/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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