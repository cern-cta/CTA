#include "ArchiveJob.hpp"

//------------------------------------------------------------------------------
// JobStateToStr
//------------------------------------------------------------------------------
const char *cta::ArchiveJob::JobStateToStr(const JobState enumValue) throw() {
  switch(enumValue) {
  case JOBSTATE_NONE   : return "NONE";
  case JOBSTATE_PENDING: return "PENDING";
  case JOBSTATE_SUCCESS: return "SUCCESS";
  case JOBSTATE_ERROR  : return "ERROR";
  default              : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob():
  m_state(JOBSTATE_NONE),
  m_totalNbFileTransfers(0),
  m_nbFailedFileTransfers(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob(
  const std::string &id,
  const JobState state,
  const uint32_t totalNbFileTransfers,
  const UserIdentity &creator,
  const std::string &comment):
  m_id(id),
  m_state(state),
  m_totalNbFileTransfers(totalNbFileTransfers),
  m_nbFailedFileTransfers(0),
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
// getState
//------------------------------------------------------------------------------
cta::ArchiveJob::JobState cta::ArchiveJob::getState() const throw() {
  return m_state;
} 

//------------------------------------------------------------------------------
// getTotalNbFileTransfers
//------------------------------------------------------------------------------
uint32_t cta::ArchiveJob::getTotalNbFileTransfers() const throw() {
  return m_totalNbFileTransfers;
}

//------------------------------------------------------------------------------
// getNbFailedFileTransfers
//------------------------------------------------------------------------------
uint32_t cta::ArchiveJob::getNbFailedFileTransfers() const throw() {
  return m_nbFailedFileTransfers;
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
