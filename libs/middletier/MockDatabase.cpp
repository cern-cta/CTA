#include "Exception.hpp"
#include "MockDatabase.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockDatabase::MockDatabase():
  fileSystemRoot(storageClasses,
    DirectoryEntry(DirectoryEntry::ENTRYTYPE_DIRECTORY, "/", "")) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockDatabase::~MockDatabase() throw() {
}
