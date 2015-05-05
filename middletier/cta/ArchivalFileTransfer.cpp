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
cta::ArchivalFileTransfer::ArchivalFileTransfer(
  const std::string &tapePoolName,
  const std::string &id,
  const std::string &userRequestId,
  const uint32_t copyNb,
  const std::string &remoteFile):
  FileTransfer(id, userRequestId, copyNb, remoteFile),
  m_tapePoolName(tapePoolName) {
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::ArchivalFileTransfer::getTapePoolName() const throw() {
  return m_tapePoolName;
}
