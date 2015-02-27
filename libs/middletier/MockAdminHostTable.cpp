#include "Exception.hpp"
#include "MockAdminHostTable.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::MockAdminHostTable::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  checkAdminHostDoesNotAlreadyExist(hostName);
  AdminHost adminHost(hostName, requester.user, time(NULL), comment);
  m_adminHosts[hostName] = adminHost;
}

//------------------------------------------------------------------------------
// checkAdminHostDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockAdminHostTable::checkAdminHostDoesNotAlreadyExist(
  const std::string &hostName) const {
  std::map<std::string, AdminHost>::const_iterator itor =
    m_adminHosts.find(hostName);
  if(itor != m_adminHosts.end()) {
    std::ostringstream message;
    message << "Administration host " << hostName << " already exists";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::MockAdminHostTable::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
  for(std::map<std::string, AdminHost>::iterator itor = m_adminHosts.begin();
    itor != m_adminHosts.end(); itor++) {
    if(hostName == itor->first) {
      m_adminHosts.erase(itor);
      return;
    }
  }

  // Reaching this point means the administration host to be deleted does not
  // exist
  std::ostringstream message;
  message << "Administration host " << hostName << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::MockAdminHostTable::getAdminHosts(
  const SecurityIdentity &requester) const {
  std::list<cta::AdminHost> hosts;
  for(std::map<std::string, AdminHost>::const_iterator itor =
    m_adminHosts.begin(); itor != m_adminHosts.end(); itor++) {
    hosts.push_back(itor->second);
  }
  return hosts;
}
