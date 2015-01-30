#include "DirectoryEntry.hpp"

//------------------------------------------------------------------------------
// typeToString
//------------------------------------------------------------------------------
const char *cta::DirectoryEntry::typeToString(const EntryType enumValue)
  throw() {
  switch(enumValue) {
  case NONE           : return "NONE";
  case FILE_ENTRY     : return "FILE_ENTRY";
  case DIRECTORY_ENTRY: return "DIRECTORY_ENTRY";
  default             : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirectoryEntry::DirectoryEntry(): entryType(NONE) {
}
