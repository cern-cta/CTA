#include "FileAttribute.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileAttribute::FileAttribute() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileAttribute::FileAttribute(const std::string &name,
  const std::string &value): name(name), value(value) {
}
