#include "Exception.hpp"
#include "MockTapeTable.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::MockTapeTable::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const std::string &comment) {
  checkTapeDoesNotAlreadyExist(vid);
  Tape tape(
    vid,
    logicalLibraryName,
    tapePoolName,
    capacityInBytes,
    0,
    requester.user,
    time(NULL),
    comment);
  m_tapes[vid] = tape;
}

//------------------------------------------------------------------------------
// checkTapeDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockTapeTable::checkTapeDoesNotAlreadyExist(
  const std::string &vid) const {
  std::map<std::string, Tape>::const_iterator itor =
    m_tapes.find(vid);
  if(itor != m_tapes.end()) {
    std::ostringstream message;
    message << "Tape with vid " << vid << " already exists";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::MockTapeTable::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  for(std::map<std::string, Tape>::iterator itor = m_tapes.begin();
    itor != m_tapes.end(); itor++) {
    if(vid == itor->first) {
      m_tapes.erase(itor);
      return;
    }
  }

  // Reaching this point means the tape to be deleted does not
  // exist
  std::ostringstream message;
  message << "Tape with volume identifier " << vid << " does not exist";
  throw Exception(message.str());
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::MockTapeTable::getTapes(
  const SecurityIdentity &requester) const {
  std::list<cta::Tape> tapes;

  for(std::map<std::string, Tape>::const_iterator itor = m_tapes.begin();
    itor != m_tapes.end(); itor++) {
    tapes.push_back(itor->second);
  }

  return tapes;
}
