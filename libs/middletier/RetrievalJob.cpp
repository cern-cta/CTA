#include "RetrievalJob.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalJob::RetrievalJob():
  m_state(RetrievalJobState::NONE),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalJob::RetrievalJob(
  const RetrievalJobState::Enum state,
  const std::string &srcPath,
  const std::string &dstUrl,
  const UserIdentity &creator,
  const time_t creationTime):
  m_srcPath(srcPath),
  m_dstUrl(dstUrl),
  m_creationTime(creationTime),
  m_creator(creator) {
}

//------------------------------------------------------------------------------
// setState
//------------------------------------------------------------------------------
void cta::RetrievalJob::setState(const RetrievalJobState::Enum state) {
  m_state = state;
}

//------------------------------------------------------------------------------
// getState
//------------------------------------------------------------------------------
cta::RetrievalJobState::Enum cta::RetrievalJob::getState() const throw() {
  return m_state;
}

//------------------------------------------------------------------------------
// getStateStr
//------------------------------------------------------------------------------
const char *cta::RetrievalJob::getStateStr() const throw() {
  return RetrievalJobState::toStr(m_state);
}

//------------------------------------------------------------------------------
// getSrcPath
//------------------------------------------------------------------------------
const std::string &cta::RetrievalJob::getSrcPath() const throw() {
  return m_srcPath;
}

//------------------------------------------------------------------------------
// getDstUrl
//------------------------------------------------------------------------------
const std::string &cta::RetrievalJob::getDstUrl() const throw() {
  return m_dstUrl;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::RetrievalJob::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::RetrievalJob::getCreator() const throw() {
  return m_creator;
}
