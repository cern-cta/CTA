#include "FileArchivalJob.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileArchivalJob::FileArchivalJob():
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileArchivalJob::FileArchivalJob(
  const std::string &srcUrl,
  const std::string &dstPath,
  const UserIdentity &creator):
  m_srcUrl(srcUrl),
  m_dstPath(dstPath),
  m_creationTime(time(NULL)),
  m_creator(creator) {
}

//------------------------------------------------------------------------------
// getSrcUrl
//------------------------------------------------------------------------------
const std::string &cta::FileArchivalJob::getSrcUrl() const throw() {
  return m_srcUrl;
}

//------------------------------------------------------------------------------
// getDstPath
//------------------------------------------------------------------------------
const std::string &cta::FileArchivalJob::getDstPath() const throw() {
  return m_dstPath;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::FileArchivalJob::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::FileArchivalJob::getCreator() const throw() {
  return m_creator;
}
