#include "cta/ArchiveToDirRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToDirRquest::ArchiveToDirRquest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveToDirRquest::~ArchiveToDirRquest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToDirRquest::ArchiveToDirRquest(
  const std::string &archiveDir,
  const std::string &storageClassName,
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user,
  const time_t creationTime):
  ArchivalRequest(storageClassName, id, priority, user, creationTime),
  m_archiveDir(archiveDir) {
}

//------------------------------------------------------------------------------
// getArchiveToFileRequests
//------------------------------------------------------------------------------
const std::list<cta::ArchiveToFileRequest> &cta::ArchiveToDirRquest::
  getArchiveToFileRequests() const throw() {
  return m_archiveToFileRequests;
}

//------------------------------------------------------------------------------
// getArchiveToFileRequests
//------------------------------------------------------------------------------
std::list<cta::ArchiveToFileRequest> &cta::ArchiveToDirRquest::
  getArchiveToFileRequests() throw() {
  return m_archiveToFileRequests;
}
