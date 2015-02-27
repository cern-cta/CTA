#include "ProtcolBuffersAlgorithms.hpp"

void cta::objectstore::serializers::removeString(::google::protobuf::RepeatedPtrField< ::std::string>* field, 
  const std::string & value) {
  bool found;
  do {
    found = false;
    for (size_t i=0; i<(size_t)field->size(); i++) {
      if (value == field->Get(i)) {
        found = true;
        field->SwapElements(i, field->size()-1);
        field->RemoveLast();
        break;
      }
    }
  } while (found);
}

size_t cta::objectstore::serializers::findString(
  ::google::protobuf::RepeatedPtrField< ::std::string>* field, 
  const std::string& value) {
  for (size_t i=0; i<(size_t)field->size(); i++) {
    if (value == field->Get(i)) {
      return i;
    }
  }
  throw NotFound("In cta::objectstore::serializers::findString: string not found");
}