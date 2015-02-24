#include "FileRetrievalJob.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileRetrievalJob::FileRetrievalJob():
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileRetrievalJob::FileRetrievalJob(
  const std::string &srcPath,
  const std::string &dstUrl,
  const UserIdentity &creator):
  m_srcPath(srcPath),
  m_dstUrl(dstUrl),
  m_creationTime(time(NULL)),
  m_creator(creator) {
}

//------------------------------------------------------------------------------
// getSrcPath
//------------------------------------------------------------------------------
const std::string &cta::FileRetrievalJob::getSrcPath() const throw() {
  return m_srcPath;
}

//------------------------------------------------------------------------------
// getDstUrl
//------------------------------------------------------------------------------
const std::string &cta::FileRetrievalJob::getDstUrl() const throw() {
  return m_dstUrl;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::FileRetrievalJob::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::FileRetrievalJob::getCreator() const throw() {
  return m_creator;
}
