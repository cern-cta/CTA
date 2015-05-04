#include "cta/MountCriteria.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountCriteria::MountCriteria():
  m_nbBytes(0),
  m_nbFiles(0),
  m_ageInSecs(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountCriteria::MountCriteria(const uint64_t nbBytes, const uint64_t nbFiles,
  const uint64_t ageInSecs):
  m_nbBytes(nbBytes),
  m_nbFiles(nbFiles),
  m_ageInSecs(ageInSecs) {
}

//------------------------------------------------------------------------------
// getNbBytes
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getNbBytes() const throw() {
  return m_nbBytes;
}

//------------------------------------------------------------------------------
// getNbFiles
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getNbFiles() const throw() {
  return m_nbFiles;
}

//------------------------------------------------------------------------------
// getAgeInSecs
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getAgeInSecs() const throw() {
  return m_ageInSecs;
}
