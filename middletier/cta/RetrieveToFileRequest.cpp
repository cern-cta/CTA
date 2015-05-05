#include "cta/RetrieveToFileRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToFileRequest::RetrieveToFileRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveToFileRequest::~RetrieveToFileRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToFileRequest::RetrieveToFileRequest(
  const std::string &archiveFile,
  const std::string &remoteFile,
  const std::string &id, 
  const uint64_t priority,
  const SecurityIdentity &user, 
  const time_t creationTime):
  RetrievalRequest(id, priority, user, creationTime),
  m_archiveFile(archiveFile),
  m_remoteFile(remoteFile) {
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
const std::string &cta::RetrieveToFileRequest::getArchiveFile() const throw() {
  return m_archiveFile;
}

//------------------------------------------------------------------------------
// getRemoteFile
//------------------------------------------------------------------------------
const std::string &cta::RetrieveToFileRequest::getRemoteFile() const throw() {
  return m_remoteFile;
}
