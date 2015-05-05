#include "cta/FileTransfer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileTransfer::FileTransfer():
  m_copyNb(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::FileTransfer::~FileTransfer() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileTransfer::FileTransfer(
  const std::string &id,
  const std::string &userRequestId,
  const uint32_t copyNb,
  const std::string &remoteFile):
  m_id(id),
  m_userRequestId(userRequestId),
  m_copyNb(copyNb),
  m_remoteFile(remoteFile) {
}

//------------------------------------------------------------------------------
// getId
//------------------------------------------------------------------------------
const std::string &cta::FileTransfer::getId() const throw() {
  return m_id;
}

//------------------------------------------------------------------------------
// getUserRequestId
//------------------------------------------------------------------------------
const std::string &cta::FileTransfer::getUserRequestId() const throw() {
  return m_userRequestId;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint32_t cta::FileTransfer::getCopyNb() const throw() {
  return m_copyNb;
}

//------------------------------------------------------------------------------
// getRemoteFile 
//------------------------------------------------------------------------------
const std::string &cta::FileTransfer::getRemoteFile() const throw() {
  return m_remoteFile;
}
