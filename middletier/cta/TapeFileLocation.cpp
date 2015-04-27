#include "cta/TapeFileLocation.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeFileLocation::TapeFileLocation():
  m_fseq(0),
  m_blockId(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeFileLocation::TapeFileLocation(const std::string &vid,
  const uint64_t fseq, const uint64_t blockId):
  m_vid(vid),
  m_fseq(0),
  m_blockId(0) {
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::TapeFileLocation::getVid() const throw() {
  return m_vid;
}

//------------------------------------------------------------------------------
// getFseq
//------------------------------------------------------------------------------
uint64_t cta::TapeFileLocation::getFseq() const throw() {
  return m_fseq;
}

//------------------------------------------------------------------------------
// getBlockId
//------------------------------------------------------------------------------
uint64_t cta::TapeFileLocation::getBlockId() const throw() {
  return m_blockId;
}
