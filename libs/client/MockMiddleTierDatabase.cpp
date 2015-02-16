#include "Exception.hpp"
#include "MockMiddleTierDatabase.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockMiddleTierDatabase::MockMiddleTierDatabase():
  fileSystemRoot(storageClasses,
    DirectoryEntry(DirectoryEntry::ENTRYTYPE_DIRECTORY, "/", "")) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockMiddleTierDatabase::~MockMiddleTierDatabase() throw() {
}
