#include "ProtcolBuffersAlgorithms.hpp"

void cta::objectstore::serializers::removeString(::google::protobuf::RepeatedPtrField< ::std::string>* field, 
  const std::string & value) {
  bool found;
  do {
    found = false;
    for (int i=0; i<field->size(); i++) {
      if (value == field->Get(i)) {
        found = true;
        field->SwapElements(i, field->size()-1);
        field->RemoveLast();
        break;
      }
    }
  } while (found);
}