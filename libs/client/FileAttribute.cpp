#include "FileAttribute.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileAtribute::FileAtribute() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileAtribute::FileAtribute(const std::string &name,
  const std::string &value): name(name), value(value) {
}
