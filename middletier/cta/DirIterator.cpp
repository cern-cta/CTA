#include "cta/DirIterator.hpp"
#include "cta/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirIterator::DirIterator() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirIterator::DirIterator(
  const std::list<DirEntry> &entries): m_entries(entries) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::DirIterator::~DirIterator() throw() {
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool cta::DirIterator::hasMore() {
  return !m_entries.empty();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
const cta::DirEntry cta::DirIterator::next() {
  if(m_entries.empty()) {
    throw Exception("Out of bounds: There are no more directory entries");
  }

  DirEntry entry = m_entries.front();
  m_entries.pop_front();
  return entry;
}
