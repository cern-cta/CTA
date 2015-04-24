#include "cta/ArchivalFileTransfer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalFileTransfer::ArchivalFileTransfer() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchivalFileTransfer::~ArchivalFileTransfer() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalFileTransfer::ArchivalFileTransfer(const std::string &remoteFile,
  const std::string &vid):
  m_remoteFile(remoteFile),
  m_vid(vid) {
}

//------------------------------------------------------------------------------
// getRemoteFile
//------------------------------------------------------------------------------
const std::string &cta::ArchivalFileTransfer::getRemoteFile() const throw() {
  return m_remoteFile;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::ArchivalFileTransfer::getVid() const throw() {
  return m_vid;
}
