#include "ArchiveJob.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob():
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob(
  const std::string &id,
  const UserIdentity &creator,
  const std::string &comment):
  m_id(id),
  m_creationTime(time(NULL)),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// getId
//------------------------------------------------------------------------------
const std::string &cta::ArchiveJob::getId() const throw() {
  return m_id;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::ArchiveJob::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::ArchiveJob::getCreator()
  const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::ArchiveJob::getComment() const throw() {
  return m_comment;
}
