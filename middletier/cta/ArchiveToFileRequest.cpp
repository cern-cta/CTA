#include "cta/ArchiveToFileRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest::ArchiveToFileRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest::~ArchiveToFileRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest::ArchiveToFileRequest(const std::string &remoteFile,
  const std::string &archiveFile, const uint32_t nbCopies):
  m_remoteFile(remoteFile),
  m_archiveFile(archiveFile),
  m_nbCopies(nbCopies) {
}

//------------------------------------------------------------------------------
// getRemoteFile
//------------------------------------------------------------------------------
const std::string &cta::ArchiveToFileRequest::getRemoteFile() const throw() {
  return m_remoteFile;
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
const std::string &cta::ArchiveToFileRequest::getArchiveFile() const throw() {
 return m_archiveFile;
}

//------------------------------------------------------------------------------
// getNbCopies
//------------------------------------------------------------------------------
uint32_t cta::ArchiveToFileRequest::getNbCopies() const throw() {
  return m_nbCopies;
}
