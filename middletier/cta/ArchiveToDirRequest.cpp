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
cta::ArchiveToDirRquest::ArchiveToDirRquest(const std::string &archiveDir):
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
