#include "cta/ArchivalMount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalMount::ArchivalMount(const std::string &id, const std::string &vid,
  const uint64_t nbFilesOnTapeBeforeMount):
  Mount(id, vid),
  m_nbFilesOnTapeBeforeMount(nbFilesOnTapeBeforeMount) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchivalMount::~ArchivalMount() throw() {
}

//------------------------------------------------------------------------------
// getNbFilesOnTapeBeforeMount
//------------------------------------------------------------------------------
uint64_t cta::ArchivalMount::getNbFilesOnTapeBeforeMount() const throw() {
  return m_nbFilesOnTapeBeforeMount;
}
