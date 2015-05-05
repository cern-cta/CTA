#include "cta/RetrieveToDirRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToDirRquest::RetrieveToDirRquest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveToDirRquest::~RetrieveToDirRquest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToDirRquest::RetrieveToDirRquest(
  const std::string &remoteDir,
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user,
  const time_t creationTime):
  RetrievalRequest(id, priority, user, creationTime),
  m_remoteDir(remoteDir) {
}

//------------------------------------------------------------------------------
// getRemoteDir
//------------------------------------------------------------------------------
const std::string &cta::RetrieveToDirRquest::getRemoteDir() const throw() {
  return m_remoteDir;
}

//------------------------------------------------------------------------------
// getRetrieveToFileRequests
//------------------------------------------------------------------------------
const std::list<cta::RetrieveToFileRequest> &cta::RetrieveToDirRquest::
  getRetrieveToFileRequests() const throw() {
  return m_retrieveToFileRequests;
}

//------------------------------------------------------------------------------
// getRetrieveToFileRequests
//------------------------------------------------------------------------------
std::list<cta::RetrieveToFileRequest> &cta::RetrieveToDirRquest::
  getRetrieveToFileRequests() throw() {
  return m_retrieveToFileRequests;
}
