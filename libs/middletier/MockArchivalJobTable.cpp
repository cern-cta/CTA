#include "Exception.hpp"
#include "MockArchivalJobTable.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createArchivalJob
//------------------------------------------------------------------------------
void cta::MockArchivalJobTable::createArchivalJob(
  const SecurityIdentity &requester,
  const std::string &tapePoolName,
  const std::string &srcUrl,
  const std::string &dstPath) {
  checkArchivalJobDoesNotAlreadyExist(dstPath);

  ArchivalJob job(ArchivalJobState::PENDING, srcUrl, dstPath, requester.user,
    time(NULL));

  std::map<std::string, std::map<time_t, ArchivalJob> >::iterator poolItor =
    m_jobsTree.find(tapePoolName);

  if(poolItor == m_jobsTree.end()) {
    std::map<time_t, ArchivalJob> jobs;
    jobs[job.getCreationTime()] = job;
    m_jobsTree[tapePoolName] = jobs;
  } else {
    poolItor->second[job.getCreationTime()] = job;
  }
}

//------------------------------------------------------------------------------
// checkArchivalJobDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockArchivalJobTable::checkArchivalJobDoesNotAlreadyExist(
  const std::string &dstPath) const {
  for(std::map<std::string, std::map<time_t, ArchivalJob> >::const_iterator
    poolItor = m_jobsTree.begin(); poolItor != m_jobsTree.end(); poolItor++) {
    const std::map<time_t, ArchivalJob> &jobs = poolItor->second;
    for(std::map<time_t, ArchivalJob>::const_iterator jobItor =
      jobs.begin(); jobItor != jobs.end(); jobItor++) {
      const ArchivalJob &job = jobItor->second;
      if(dstPath == job.getDstPath()) {
        std::ostringstream message;
        message << "An archival job for destination path " << dstPath <<
          " already exists";
        throw(Exception(message.str()));
      }
    }
  }
}

//------------------------------------------------------------------------------
// deleteArchivalJob
//------------------------------------------------------------------------------
void cta::MockArchivalJobTable::deleteArchivalJob(
  const SecurityIdentity &requester,
  const std::string &dstPath) {
  for(std::map<std::string, std::map<time_t, ArchivalJob> >::iterator
    poolItor = m_jobsTree.begin(); poolItor != m_jobsTree.end(); poolItor++) {
    std::map<time_t, ArchivalJob> &jobs = poolItor->second;
    for(std::map<time_t, ArchivalJob>::iterator jobItor =
      jobs.begin(); jobItor != jobs.end(); jobItor++) {
      const ArchivalJob &job = jobItor->second;
      if(dstPath == job.getDstPath()) {
        jobs.erase(jobItor);
        if(jobs.empty()) {
          m_jobsTree.erase(poolItor);
        }
        return;
      }
    }
  } 

  std::ostringstream message;
  message << "An archival job for destination path " << dstPath <<
    " does not exist";
  throw(Exception(message.str()));
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
const std::map<std::string, std::map<time_t, cta::ArchivalJob> >
  &cta::MockArchivalJobTable::getArchivalJobs(
  const SecurityIdentity &requester) const {
  return m_jobsTree;
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::list<cta::ArchivalJob> cta::MockArchivalJobTable::getArchivalJobs(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
  std::list<cta::ArchivalJob> jobs;
  const std::map<std::string, std::map<time_t, ArchivalJob> >::const_iterator
    poolItor = m_jobsTree.find(tapePoolName);

  if(poolItor != m_jobsTree.end()) {
    const std::map<time_t, ArchivalJob> &jobMap = poolItor->second;
    for(std::map<time_t, ArchivalJob>::const_iterator jobItor =
      jobMap.begin(); jobItor != jobMap.end(); jobItor++) {
      jobs.push_back(jobItor->second);
    }
  }

  return jobs;
}
