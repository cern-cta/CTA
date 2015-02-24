#include "MockArchivalJobTable.hpp"

#include "Exception.hpp"

//------------------------------------------------------------------------------
// createArchivalJob
//------------------------------------------------------------------------------
void cta::MockArchivalJobTable::createArchivalJob(
  const SecurityIdentity &requester,
  const std::string &srcUrl,
  const std::string &dstPath) {
}

//------------------------------------------------------------------------------
// checkArchivalJobDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockArchivalJobTable::checkArchivalJobDoesNotAlreadyExist(
  const std::string &dstPath) const {
}

//------------------------------------------------------------------------------
// deleteArchivalJob
//------------------------------------------------------------------------------
void cta::MockArchivalJobTable::deleteArchivalJob(
  const SecurityIdentity &requester,
  const std::string &dstPath) {
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
const std::map<cta::TapePool, std::list<cta::ArchivalJob> >
  cta::MockArchivalJobTable::getArchivalJobs(
  const SecurityIdentity &requester) const {
  std::map<cta::TapePool, std::list<cta::ArchivalJob> > jobs;
  return jobs;
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::list<cta::ArchivalJob> cta::MockArchivalJobTable::getArchivalJobs(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
  std::list<cta::ArchivalJob> jobs;
  return jobs;
}
