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

