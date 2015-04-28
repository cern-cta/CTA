#include "cta/TapePool.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapePool::TapePool():
  m_nbDrives(0),
  m_nbPartialTapes(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::TapePool::~TapePool() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapePool::TapePool(
  const std::string &name,
  const uint16_t nbDrives,
  const uint32_t nbPartialTapes,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_name(name),
  m_nbDrives(nbDrives),
  m_nbPartialTapes(nbPartialTapes) {
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::TapePool::operator<(const TapePool &rhs) const throw() {
  return m_name < rhs.m_name;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::TapePool::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getNbDrives
//------------------------------------------------------------------------------
uint16_t cta::TapePool::getNbDrives() const throw() {
  return m_nbDrives;
}

//------------------------------------------------------------------------------
// getNbPartialTapes
//------------------------------------------------------------------------------
uint32_t cta::TapePool::getNbPartialTapes() const throw() {
  return m_nbPartialTapes;
}
