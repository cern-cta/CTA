#include "ArchivalJob.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalJob::ArchivalJob():
  m_state(ArchivalJobState::NONE),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalJob::ArchivalJob(
  const ArchivalJobState::Enum state,
  const std::string &srcUrl,
  const std::string &dstPath,
  const UserIdentity &creator,
  const time_t creationTime):
  m_state(state),
  m_srcUrl(srcUrl),
  m_dstPath(dstPath),
  m_creationTime(creationTime),
  m_creator(creator) {
}

//------------------------------------------------------------------------------
// setState
//------------------------------------------------------------------------------
void cta::ArchivalJob::setState(const ArchivalJobState::Enum state) {
  m_state = state;
}

//------------------------------------------------------------------------------
// getState
//------------------------------------------------------------------------------
cta::ArchivalJobState::Enum cta::ArchivalJob::getState() const throw() {
  return m_state;
}

//------------------------------------------------------------------------------
// getStateStr
//------------------------------------------------------------------------------
const char *cta::ArchivalJob::getStateStr() const throw() {
  return ArchivalJobState::toStr(m_state);
}

//------------------------------------------------------------------------------
// getSrcUrl
//------------------------------------------------------------------------------
const std::string &cta::ArchivalJob::getSrcUrl() const throw() {
  return m_srcUrl;
}

//------------------------------------------------------------------------------
// getDstPath
//------------------------------------------------------------------------------
const std::string &cta::ArchivalJob::getDstPath() const throw() {
  return m_dstPath;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::ArchivalJob::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::ArchivalJob::getCreator() const throw() {
  return m_creator;
}
