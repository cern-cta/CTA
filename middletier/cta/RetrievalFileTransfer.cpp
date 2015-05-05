#include "cta/RetrievalFileTransfer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalFileTransfer::RetrievalFileTransfer() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrievalFileTransfer::~RetrievalFileTransfer() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalFileTransfer::RetrievalFileTransfer(
  const TapeFileLocation &tapeFile,
  const std::string &id, 
  const std::string &userRequestId,
  const uint32_t copyNb,
  const std::string &remoteFile):
  FileTransfer(id, userRequestId, copyNb, remoteFile),
  m_tapeFile(tapeFile) {
}

//------------------------------------------------------------------------------
// getTapeFile
//------------------------------------------------------------------------------
const cta::TapeFileLocation &cta::RetrievalFileTransfer::getTapeFile() const
  throw() {
  return m_tapeFile;
}
