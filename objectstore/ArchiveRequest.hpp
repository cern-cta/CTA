
/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/Timer.hpp"
#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class EntryLogSerDeser;

class ArchiveRequest: public ObjectOps<serializers::ArchiveRequest, serializers::ArchiveRequest_t> {
public:
  ArchiveRequest(const std::string & address, Backend & os);
  ArchiveRequest(Backend & os);
  ArchiveRequest(GenericObject & go);
  void initialize();
  // Job management ============================================================
  void addJob(uint16_t copyNumber, const std::string & tapepool,
    const std::string & archivequeueaddress, uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries);
  void setJobSelected(uint16_t copyNumber, const std::string & owner);
  void setJobPending(uint16_t copyNumber);
  bool setJobSuccessful(uint16_t copyNumber); //< returns true if this is the last job
  bool addJobFailure(uint16_t copyNumber, uint64_t sessionId, log::LogContext &lc); //< returns true the job is failed
  struct RetryStatus {
    uint64_t retriesWithinMount = 0;
    uint64_t maxRetriesWithinMount = 0;
    uint64_t totalRetries = 0;
    uint64_t maxTotalRetries = 0;
  };
  RetryStatus getRetryStatus(uint16_t copyNumber);
  serializers::ArchiveJobStatus getJobStatus(uint16_t copyNumber);
  std::string statusToString(const serializers::ArchiveJobStatus & status);
  bool finishIfNecessary(log::LogContext & lc);/**< Handling of the consequences of a job status change for the entire request.
                                                * This function returns true if the request got finished. */
  // Mark all jobs as pending mount (following their linking to a tape pool)
  void setAllJobsLinkingToArchiveQueue();
  // Mark all the jobs as being deleted, in case of a cancellation
  void setAllJobsFailed();
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
  // Set a job ownership
  void setJobOwner(uint16_t copyNumber, const std::string & owner);
  // An asynchronous job ownership updating class.
  class AsyncJobOwnerUpdater {
    friend class ArchiveRequest;
  public:
    void wait();
    const common::dataStructures::ArchiveFile & getArchiveFile();
    const std::string & getSrcURL();
    const std::string & getArchiveReportURL();
    const std::string & getArchiveErrorReportURL();
    struct TimingsReport {
      double lockFetchTime = 0;
      double processTime = 0;
      double commitUnlockTime = 0;
    };
    TimingsReport getTimeingsReport();
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    common::dataStructures::ArchiveFile m_archiveFile;
    std::string m_srcURL;
    std::string m_archiveReportURL;
    std::string m_archiveErrorReportURL;
    utils::Timer m_timer;
    TimingsReport m_timingReport;
  };
  // An job owner updater factory. The owner MUST be previousOwner for the update to be executed.
  CTA_GENERATE_EXCEPTION_CLASS(WrongPreviousOwner);
  AsyncJobOwnerUpdater * asyncUpdateJobOwner(uint16_t copyNumber, const std::string & owner, const std::string &previousOwner);

  // An asynchronous job updating class for success.
  class AsyncJobSuccessfulUpdater {
    friend class ArchiveRequest;
  public:
    void wait();
    bool m_isLastJob;
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
  };
  AsyncJobSuccessfulUpdater * asyncUpdateJobSuccessful(uint16_t copyNumber);

  // Get a job owner
  std::string getJobOwner(uint16_t copyNumber);
  // Request management ========================================================
  void setSuccessful();
  void setFailed();
  // ===========================================================================
  // TODO: ArchiveFile comes with extraneous information. 
  void setArchiveFile(const cta::common::dataStructures::ArchiveFile& archiveFile);
  cta::common::dataStructures::ArchiveFile getArchiveFile();
  
  void setArchiveReportURL(const std::string &URL);
  std::string getArchiveReportURL();
  
  void setArchiveErrorReportURL(const std::string &URL);
  std::string getArchiveErrorReportURL();

  void setRequester(const cta::common::dataStructures::UserIdentity &requester);
  cta::common::dataStructures::UserIdentity getRequester();

  void setSrcURL(const std::string &srcURL);
  std::string getSrcURL();

  void setEntryLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getEntryLog();
  
  void setMountPolicy(const cta::common::dataStructures::MountPolicy &mountPolicy);
  cta::common::dataStructures::MountPolicy getMountPolicy();
  
  class JobDump {
  public:
    uint16_t copyNb;
    std::string tapePool;
    std::string owner;
    serializers::ArchiveJobStatus status;
  };
  
  std::list<JobDump> dumpJobs();
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  std::string  dump();
};

}}
