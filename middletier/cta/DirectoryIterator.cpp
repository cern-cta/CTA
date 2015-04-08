#include "cta/DirectoryIterator.hpp"
#include "cta/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirectoryIterator::DirectoryIterator() {
}

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

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool cta::DirectoryIterator::hasMore() {
  return !m_entries.empty();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
const cta::DirectoryEntry cta::DirectoryIterator::next() {
  if(m_entries.empty()) {
    throw Exception("Out of bounds: There are no more directory entries");
  }

  DirectoryEntry entry = m_entries.front();
  m_entries.pop_front();
  return entry;
}
