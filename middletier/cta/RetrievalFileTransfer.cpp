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
  const TapeFileLocation &tapeFile, const std::string &remoteFile):
  m_tapeFile(tapeFile),
  m_remoteFile(remoteFile) {
}

//------------------------------------------------------------------------------
// getTapeFile
//------------------------------------------------------------------------------
const cta::TapeFileLocation &cta::RetrievalFileTransfer::getTapeFile() const
  throw() {
  return m_tapeFile;
}

//------------------------------------------------------------------------------
// getRemoteFile
//------------------------------------------------------------------------------
const std::string &cta::RetrievalFileTransfer::getRemoteFile() const throw() {
  return m_remoteFile;
}
