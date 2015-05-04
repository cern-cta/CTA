#include "cta/LogicalLibrary.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::LogicalLibrary::LogicalLibrary() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::LogicalLibrary::~LogicalLibrary() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::LogicalLibrary::LogicalLibrary(
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
const std::string &cta::LogicalLibrary::getName() const throw() {
  return m_name;
}
