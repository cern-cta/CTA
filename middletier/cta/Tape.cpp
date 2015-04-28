#include "cta/Tape.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Tape::Tape():
    m_capacityInBytes(0),
    m_dataOnTapeInBytes(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Tape::~Tape() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Tape::Tape(
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const uint64_t dataOnTapeInBytes,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime):
    ConfigurationItem(creator, comment, creationTime),
    m_vid(vid),
    m_logicalLibraryName(logicalLibraryName),
    m_tapePoolName(tapePoolName),
    m_capacityInBytes(capacityInBytes),
    m_dataOnTapeInBytes(dataOnTapeInBytes) {
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::Tape::operator<(const Tape &rhs) const throw() {
  return m_vid < rhs.m_vid;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::Tape::getVid() const throw() {
  return m_vid;
}

//------------------------------------------------------------------------------
// getLogicalLibraryName
//------------------------------------------------------------------------------
const std::string &cta::Tape::getLogicalLibraryName() const throw() {
  return m_logicalLibraryName;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::Tape::getTapePoolName() const throw() {
  return m_tapePoolName;
}

//------------------------------------------------------------------------------
// getCapacityInBytes
//------------------------------------------------------------------------------
uint64_t cta::Tape::getCapacityInBytes() const throw() {
  return m_capacityInBytes;
}

//------------------------------------------------------------------------------
// getDataOnTapeInBytes
//------------------------------------------------------------------------------
uint64_t cta::Tape::getDataOnTapeInBytes() const throw() {
  return m_dataOnTapeInBytes;
}
