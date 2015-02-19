#include "Exception.hpp"
#include "MockLogicalLibraryDatabase.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockLogicalLibraryDatabase::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
  checkLogicalLibraryDoesNotAlreadyExist(name);
  LogicalLibrary logicalLibrary(name, requester.user, comment);
  m_libraries[name] = logicalLibrary;
}

//------------------------------------------------------------------------------
// checkLogicalLibraryDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockLogicalLibraryDatabase::checkLogicalLibraryDoesNotAlreadyExist(
  const std::string &name) const {
  std::map<std::string, LogicalLibrary>::const_iterator itor =
    m_libraries.find(name);
  if(itor != m_libraries.end()) {
    std::ostringstream message;
    message << "Logical library " << name << " already exists";
    throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockLogicalLibraryDatabase::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  for(std::map<std::string, LogicalLibrary>::iterator itor =
    m_libraries.begin(); itor != m_libraries.end();
    itor++) {
    if(name == itor->first) {
      m_libraries.erase(itor);
      return;
    }
  }

  // Reaching this point means the logical library to be deleted does not
  // exist
  std::ostringstream message;
  message << "Logical library " << name << " does not exist";
  throw Exception(message.str());
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::MockLogicalLibraryDatabase::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  std::list<LogicalLibrary> libraries;

  for(std::map<std::string, LogicalLibrary>::const_iterator itor =
    m_libraries.begin(); itor != m_libraries.end();
    itor++) {
    libraries.push_back(itor->second);
  }
  return libraries;
}
