#include "Exception.hpp"
#include "MockArchivalJobTable.hpp"

#include <iostream>
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

  std::map<std::string, std::list<ArchivalJob> >::iterator poolItor =
    m_jobsTree.find(tapePoolName);

  if(poolItor == m_jobsTree.end()) {
    std::list<ArchivalJob> jobs;
    jobs.push_back(job);
    m_jobsTree[tapePoolName] = jobs;
  } else {
    std::list<ArchivalJob> &jobs = poolItor->second;
    jobs.push_back(job);
  }
}

//------------------------------------------------------------------------------
// checkArchivalJobDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockArchivalJobTable::checkArchivalJobDoesNotAlreadyExist(
  const std::string &dstPath) const {
  for(std::map<std::string, std::list<ArchivalJob> >::const_iterator
    poolItor = m_jobsTree.begin(); poolItor != m_jobsTree.end(); poolItor++) {
    const std::list<ArchivalJob> &jobs = poolItor->second;
    for(std::list<ArchivalJob>::const_iterator jobItor = jobs.begin();
      jobItor != jobs.end(); jobItor++) {
      const ArchivalJob &job = *jobItor;
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
  for(std::map<std::string, std::list<ArchivalJob> >::iterator
    poolItor = m_jobsTree.begin(); poolItor != m_jobsTree.end(); poolItor++) {
    std::list<ArchivalJob> &jobs = poolItor->second;
    for(std::list<ArchivalJob>::iterator jobItor = jobs.begin();
      jobItor != jobs.end(); jobItor++) {
      const ArchivalJob &job = *jobItor;
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
const std::map<std::string, std::list<cta::ArchivalJob> >
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
  const std::map<std::string, std::list<ArchivalJob> >::const_iterator
    poolItor = m_jobsTree.find(tapePoolName);
  return poolItor->second;
}
