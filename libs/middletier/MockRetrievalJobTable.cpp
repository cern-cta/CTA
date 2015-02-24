#include "Exception.hpp"
#include "MockRetrievalJobTable.hpp"

//------------------------------------------------------------------------------
// createRetrievalJob
//------------------------------------------------------------------------------
void cta::MockRetrievalJobTable::createRetrievalJob(
  const SecurityIdentity &requester,
  const std::string &srcPath,
  const std::string &dstUrl) {
}

//------------------------------------------------------------------------------
// checkRetrievalJobDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockRetrievalJobTable::checkRetrievalJobDoesNotAlreadyExist(
  const std::string &dstUrl) const {
}

//------------------------------------------------------------------------------
// deleteRetrievalJob
//------------------------------------------------------------------------------
void cta::MockRetrievalJobTable::deleteRetrievalJob(
  const SecurityIdentity &requester,
  const std::string &dstPath) {
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
const std::map<cta::Tape, std::list<cta::RetrievalJob> >
  cta::MockRetrievalJobTable::getRetrievalJobs(
  const SecurityIdentity &requester) const {
  std::map<cta::Tape, std::list<cta::RetrievalJob> > jobs;
  return jobs;
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
std::list<cta::RetrievalJob> cta::MockRetrievalJobTable::getRetrievalJobs(
  const SecurityIdentity &requester,
  const std::string &vid) const {
  std::list<cta::RetrievalJob> jobs;
  return jobs;
}
