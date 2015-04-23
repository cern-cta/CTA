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
cta::RetrieveToFileRequest::RetrieveToFileRequest(const std::string &archiveFile,
  const std::string &remoteFile):
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
