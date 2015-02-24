#include "ArchivalJob.hpp"

//------------------------------------------------------------------------------
// JobStateToStr
//------------------------------------------------------------------------------
const char *cta::ArchivalJob::JobStateToStr(const JobState enumValue) throw() {
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
cta::ArchivalJob::ArchivalJob():
  m_state(JOBSTATE_NONE),
  m_totalNbFileTransfers(0),
  m_nbFailedFileTransfers(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalJob::ArchivalJob(
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
const std::string &cta::ArchivalJob::getId() const throw() {
  return m_id;
}

//------------------------------------------------------------------------------
// getState
//------------------------------------------------------------------------------
cta::ArchivalJob::JobState cta::ArchivalJob::getState() const throw() {
  return m_state;
} 

//------------------------------------------------------------------------------
// getTotalNbFileTransfers
//------------------------------------------------------------------------------
uint32_t cta::ArchivalJob::getTotalNbFileTransfers() const throw() {
  return m_totalNbFileTransfers;
}

//------------------------------------------------------------------------------
// getNbFailedFileTransfers
//------------------------------------------------------------------------------
uint32_t cta::ArchivalJob::getNbFailedFileTransfers() const throw() {
  return m_nbFailedFileTransfers;
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
const cta::UserIdentity &cta::ArchivalJob::getCreator()
  const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::ArchivalJob::getComment() const throw() {
  return m_comment;
}
