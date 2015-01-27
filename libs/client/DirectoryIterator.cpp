#include "DirectoryIterator.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirectoryIterator::DirectoryIterator(
  const std::list<DirectoryEntry> &entries): m_entries(entries) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::DirectoryIterator::~DirectoryIterator() throw() {
}
