#include "DirectoryEntry.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirectoryEntry::DirectoryEntry():
  ownerId(0),
  groupId(0),
  mode(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirectoryEntry::DirectoryEntry(const std::string &name):
  name(name),
  ownerId(0),
  groupId(0),
  mode(0) {
}
