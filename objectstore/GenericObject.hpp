/**
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

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta {  namespace objectstore {

class GenericObject: public ObjectOps<serializers::GenericObject> {
public:
  GenericObject(const std::string & name, Backend & os):
    ObjectOps<serializers::GenericObject>(os, name) {};
    
  class ForbiddenOperation: public cta::exception::Exception {
  public:
    ForbiddenOperation(const std::string & w): cta::exception::Exception(w) {}
  };
    
  // Overload of ObjectOps's implementation: this special object tolerates all
  // types of objects
  void fetch();
  
  // Overload of ObjectOps's implementation: we will leave the payload transparently
  // untouched and only deal with header parameters
  void commit();
  
  // Get the object's type (type is forced implicitely in other classes)
  serializers::ObjectType type();
  
  // Overload of ObjectOps's implementation: this operation is forbidden. Generic
  // Object is only used to manipulate existing objects
  void insert();
  
  // Overload of ObjectOps's implementation: this operation is forbidden. Generic
  // Object is only used to manipulate existing objects
  void initialize();
  
};

}}

