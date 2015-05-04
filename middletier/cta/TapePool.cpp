#include "cta/TapePool.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapePool::TapePool():
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
  const uint32_t nbPartialTapes,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_name(name),
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
// getNbPartialTapes
//------------------------------------------------------------------------------
uint32_t cta::TapePool::getNbPartialTapes() const throw() {
  return m_nbPartialTapes;
}
