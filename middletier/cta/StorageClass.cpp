#include "cta/StorageClass.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClass::StorageClass():
  m_nbCopies(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::StorageClass::~StorageClass() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClass::StorageClass(
  const std::string &name,
  const uint16_t nbCopies,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_name(name),
  m_nbCopies(nbCopies) {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::StorageClass::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getNbCopies
//------------------------------------------------------------------------------
uint16_t cta::StorageClass::getNbCopies() const throw() {
  return m_nbCopies;
}
