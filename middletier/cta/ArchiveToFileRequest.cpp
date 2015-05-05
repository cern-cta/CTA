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
cta::ArchiveToFileRequest::ArchiveToFileRequest(
  const std::string &remoteFile,
  const std::string &archiveFile,
  const uint32_t nbCopies,
  const std::string &storageClassName,
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user, 
  const time_t creationTime):
  ArchivalRequest(storageClassName, id, priority, user, creationTime),
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
