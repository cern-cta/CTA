#include "cta/StorageClass.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClass::StorageClass(): nbCopies(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClass::StorageClass(const std::string &name,
  const uint8_t nbCopies): name(name), nbCopies(nbCopies) {
}
