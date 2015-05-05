#include "cta/AdminHost.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminHost::AdminHost() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminHost::~AdminHost() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminHost::AdminHost(
  const std::string &name,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_name(name) {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::AdminHost::getName() const throw() {
  return m_name;
}
