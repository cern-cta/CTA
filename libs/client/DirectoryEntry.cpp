#include "DirectoryEntry.hpp"

//------------------------------------------------------------------------------
// entryTypeToStr
//------------------------------------------------------------------------------
const char *cta::DirectoryEntry::entryTypeToStr(const EntryType enumValue)
  throw() {
  switch(enumValue) {
  case ENTRYTYPE_FILE     : return "FILE";
  case ENTRYTYPE_DIRECTORY: return "DIRECTORY";
  default                 : return "UNKNOWN";
  }
}

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
cta::DirectoryEntry::DirectoryEntry(const EntryType entryType,
  const std::string &name, const std::string &storageClassName):
  entryType(entryType),
  name(name),
  ownerId(0),
  groupId(0),
  storageClassName(storageClassName) {
}
