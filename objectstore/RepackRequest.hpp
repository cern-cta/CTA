/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "scheduler/SchedulerDatabase.hpp"

namespace cta::objectstore {

class Agent;
class GenericObject;

class RepackRequest: public ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t> {
public:
  RepackRequest(const std::string & address, Backend & os);
  RepackRequest(Backend & os);
  RepackRequest(GenericObject & go);
  void initialize() override;

  // Parameters interface
  void setVid(const std::string & vid);
  void setType(common::dataStructures::RepackInfo::Type repackType);
  void setStatus(common::dataStructures::RepackInfo::Status repackStatus);
  common::dataStructures::RepackInfo getInfo();
  void setBufferURL(const std::string & bufferURL);
  void setExpandFinished(const bool expandFinished);
  bool isExpandFinished();
  void setExpandStarted(const bool expandStarted);
  void setTotalStats(const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles& totalStatsFiles);
  cta::SchedulerDatabase::RepackRequest::TotalStatsFiles getTotalStatsFile();
  void setMountPolicy(const common::dataStructures::MountPolicy &mp);
  common::dataStructures::MountPolicy getMountPolicy();
  void deleteAllSubrequests();
  void setIsComplete(const bool complete);
  void updateRepackDestinationInfos(const common::dataStructures::ArchiveFile & archiveFile, const std::string & destinationVid);
  std::list<common::dataStructures::RepackInfo::RepackDestinationInfo> getRepackDestinationInfos();
  void setCreationLog(const common::dataStructures::EntryLog & creationLog);
  common::dataStructures::EntryLog getCreationLog();

  /**
   * Set the flag noRecall to only inject files that are in the buffer. This will prevent
   * to retrieve the files from the repacked tape.
   * @param noRecall if true, the repack expansion will only look at what is on the buffer
   * if false, the expansion will create Retrieve subrequests for files that are not in the buffer
   */
  void setNoRecall(const bool noRecall);
  bool getNoRecall();

  /**
   * Automatically set the new status of the Repack Request
   * regarding multiple parameters
   */
  void setStatus();
  // Sub request management
  struct SubrequestInfo {
    std::string address;
    uint64_t fSeq;
    bool subrequestDeleted;  ///< A boolean set to true before deleting a request. Covers the race between request creation recording and request
    typedef std::set<SubrequestInfo> set;
    bool operator< (const SubrequestInfo & o) const { return fSeq < o.fSeq; }
  };
  /**
   * Provide a list of addresses for a set or fSeqs. For expansion of repack requests.
   * The addresses could be provided from the repack request if previously recorded, or
   * generated if not. The repack request should then be committed (not done here) before the
   * sub requests are actually created. Sub requests could also be already present, and this
   * would not be an error case (the previous process doing the expansion managed to create them),
   * yet not update the object to reflect the last fSeq created.
   * This function implicitly records the information it generates (commit up t the caller);
   */
  SubrequestInfo::set getOrPrepareSubrequestInfo (std::set<uint64_t> fSeqs, AgentReference & agentRef);

  /**
   * Remove this request from its owner ownership
   */
  void removeFromOwnerAgentOwnership();

private:
  struct RepackSubRequestPointer {
    std::string address;
    uint64_t fSeq;
    bool retrieveAccounted;
    std::set<uint32_t> archiveCopyNbsAccounted;
    bool subrequestDeleted;
    typedef std::map<uint64_t, RepackSubRequestPointer> Map;
    void serialize (serializers::RepackSubRequestPointer & rsrp);
    void deserialize (const serializers::RepackSubRequestPointer & rsrp);
  };

public:
  /// Set the last fully created sub-requests address
  void setLastExpandedFSeq(uint64_t lastExpandedFSeq);
  uint64_t getLastExpandedFSeq();

  void setTotalFileToRetrieve(const uint64_t nbFilesToRetrieve);
  void setTotalBytesToRetrieve(const uint64_t nbBytesToRetrieve);
  void setTotalFileToArchive(const uint64_t nbFilesToArchive);
  void setTotalBytesToArchive(const uint64_t nbBytesToArchive);
  void setUserProvidedFiles(const uint64_t userProvidedFiles);

  struct SubrequestStatistics {
    uint64_t fSeq;
    uint64_t files = 1;
    uint64_t bytes;
    /// CopyNb is needed to record archive jobs statistics (we can have several archive jobs for the same fSeq)
    uint32_t copyNb = 0;
    bool subrequestDeleted = false;
    bool hasUserProvidedFile = false;
    std::string destinationVid;
    typedef std::list<SubrequestStatistics> List;
    bool operator< (const SubrequestStatistics & o) const { return fSeq < o.fSeq; }
  };
  void reportRetriveSuccesses (SubrequestStatistics::List & retrieveSuccesses);
  void reportRetriveFailures (SubrequestStatistics::List & retrieveFailures);
  serializers::RepackRequestStatus reportArchiveSuccesses (SubrequestStatistics::List & archiveSuccesses);
  serializers::RepackRequestStatus reportArchiveFailures (SubrequestStatistics::List & archiveFailures);
  void reportSubRequestsForDeletion (std::list<uint64_t>& fSeqs);
  enum class StatsType: uint8_t {
    UserProvided,
    RetrieveSuccess,
    RetrieveFailure,
    RetrieveTotal,
    ArchiveSuccess,
    ArchiveFailure,
    ArchiveTotal,
  };
  struct StatsValues {
    uint64_t files = 0;
    uint64_t bytes = 0;
  };
  std::map<StatsType, StatsValues> getStats();

  void reportRetrieveCreationFailures(const std::list<cta::SchedulerDatabase::RepackRequest::Subrequest>& notCreatedSubrequests);

  void reportArchiveCreationFailures(uint64_t nbFailedToCreateArchiveRequests);

  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;

  std::string dump();

  // An asynchronous request ownership updating class.
  class AsyncOwnerAndStatusUpdater {
    friend class RepackRequest;
  public:
    void wait();
    common::dataStructures::RepackInfo getInfo();
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    log::TimingList m_timingReport;
    utils::Timer m_timer;
    common::dataStructures::RepackInfo m_repackInfo;
  };
  // An owner updater factory. The owner MUST be previousOwner for the update to be executed.
  AsyncOwnerAndStatusUpdater *asyncUpdateOwnerAndStatus(const std::string &owner, const std::string &previousOwner,
      std::optional<serializers::RepackRequestStatus> newStatus);
  };

} // namespace cta::objectstore
