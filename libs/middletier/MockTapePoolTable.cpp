#include "Exception.hpp"
#include "MockTapePoolTable.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::MockTapePoolTable::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbDrives,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
  checkTapePoolDoesNotAlreadyExist(name);
  TapePool tapePool(name, nbDrives, nbPartialTapes,requester.user, comment);
  m_tapePools[name] = tapePool;
}

//------------------------------------------------------------------------------
// checkTapePoolDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockTapePoolTable::checkTapePoolDoesNotAlreadyExist(
  const std::string &name) const {
  std::map<std::string, TapePool>::const_iterator itor =
    m_tapePools.find(name);
  if(itor != m_tapePools.end()) {
    std::ostringstream message;
    message << "The " << name << " tape pool already exists";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// checkTapePoolExists
//------------------------------------------------------------------------------
void cta::MockTapePoolTable::checkTapePoolExists(
  const std::string &name) const {
  std::map<std::string, TapePool>::const_iterator itor =
    m_tapePools.find(name);
  if(itor == m_tapePools.end()) {
    std::ostringstream message;
    message << "The " << name << " tape pool does not exist";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::MockTapePoolTable::deleteTapePool(const SecurityIdentity &requester,
  const std::string &name) {
  std::map<std::string, TapePool>::iterator itor = m_tapePools.find(name);
  if(itor == m_tapePools.end()) {
    std::ostringstream message;
    message << "The " << name << " tape pool does not exist";
    throw Exception(message.str());
  }
  m_tapePools.erase(itor);
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::MockTapePoolTable::getTapePools(
  const SecurityIdentity &requester) const {
  std::list<cta::TapePool> tapePools;

  for(std::map<std::string, TapePool>::const_iterator itor =
    m_tapePools.begin(); itor != m_tapePools.end(); itor++) {
    tapePools.push_back(itor->second);
  }
  return tapePools;
}
