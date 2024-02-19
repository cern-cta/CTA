/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "scheduler/Scheduler.hpp"
#include "scheduler/PostgresSchedDB/RepackRequest.hpp"
#include "scheduler/PostgresSchedDB/RetrieveRequest.hpp"
#include "scheduler/PostgresSchedDB/Helpers.hpp"
#include "common/log/LogContext.hpp"
#include "rdbms/UniqueConstraintError.hpp"


namespace cta::postgresscheddb {

uint64_t RepackRequest::getLastExpandedFSeq()
{
   throw cta::exception::Exception("Not implemented");
}

void RepackRequest::setLastExpandedFSeq(uint64_t fseq)
{
   throw cta::exception::Exception("Not implemented");
}

void RepackRequest::setTotalStats(const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles& totalStatsFiles)
{
  repackInfo.totalFilesOnTapeAtStart = totalStatsFiles.totalFilesOnTapeAtStart;
  repackInfo.totalBytesOnTapeAtStart = totalStatsFiles.totalBytesOnTapeAtStart;
  repackInfo.allFilesSelectedAtStart = totalStatsFiles.allFilesSelectedAtStart;
  repackInfo.totalFilesToRetrieve = totalStatsFiles.totalFilesToRetrieve;
  repackInfo.totalFilesToArchive  = totalStatsFiles.totalFilesToArchive;
  repackInfo.totalBytesToArchive  = totalStatsFiles.totalBytesToArchive;
  repackInfo.totalBytesToRetrieve = totalStatsFiles.totalBytesToRetrieve;
  repackInfo.userProvidedFiles    = totalStatsFiles.userProvidedFiles;
}

[[noreturn]] void RepackRequest::reportRetrieveCreationFailures([[maybe_unused]] const std::list<Subrequest> &notCreatedSubrequests) const {
   throw cta::exception::Exception("Not implemented");
}

uint64_t RepackRequest::addSubrequestsAndUpdateStats(
  std::list<Subrequest>                              &repackSubrequests,
  cta::common::dataStructures::ArchiveRoute::FullMap &archiveRoutesMap,
  uint64_t                                           maxFSeqLowBound,
  const uint64_t                                     maxAddedFSeq,
  const TotalStatsFiles                              &totalStatsFiles,
  disk::DiskSystemList                               diskSystemList,
  log::LogContext                                    &lc
)
{
  // refactor this entire method and remove goto statements wherever possible !
  if (!m_txn) {
    throw cta::ExpandRepackRequestException(
      "In postgresscheddb::RepackRequest::addSubrequestsAndUpdateStats(), "
      "no active database transaction");
  }

  // We need to prepare retrieve requests and insert them.
  uint64_t nbRetrieveSubrequestsCreated = 0;

  // use a map of fSeq -> subreuest*
  std::map<uint64_t, SubrequestPointer *> srmap;
  for(auto &r: m_subreqp) {
    srmap[r.fSeq] = &r;
  }

  // go through and if needed add them to our
  // subrequest (retrieve or archive request) pointers
  bool newSubsCreated = false;
  for(const auto &r: repackSubrequests) {
    if (srmap.find(r.fSeq) == srmap.end()) {
      newSubsCreated = true;
      m_subreqp.emplace_back();
      m_subreqp.back().address = "????"; // todo
      m_subreqp.back().fSeq = r.fSeq;
      srmap[r.fSeq] = &m_subreqp.back();
    }
  }

  if (newSubsCreated) {
      // update row: todo //
  }

  setTotalStats(totalStatsFiles);
  uint64_t fSeq = std::max(maxFSeqLowBound + 1, maxAddedFSeq + 1);
  bool noRecall = repackInfo.noRecall;

  // todo: we need to ensure the deleted boolean of each subrequest does
  // not change while we attempt creating them (or we would face double creation).

  StatsValues failedCreationStats;
  std::list<Subrequest> notCreatedSubrequests;

  //We will insert the jobs by batch of 500
  auto subReqItor = repackSubrequests.begin();
  while(subReqItor != repackSubrequests.end()) {
    uint64_t nbSubReqProcessed = 0;

    while(subReqItor != repackSubrequests.end() && nbSubReqProcessed < 500){
      auto & rsr = *subReqItor;

      // Requests marked as deleted are guaranteed to have already been created => we will not re-attempt.
      if (!srmap.at(rsr.fSeq)->isSubreqDeleted) {
        // We need to try and create the subrequest.
        // Create the sub request (it's a retrieve request now).
        auto rr=std::make_shared<postgresscheddb::RetrieveRequest>(m_connPool, m_lc /*srmap.at(rsr.fSeq)->address*/);

        // Set the file info
        common::dataStructures::RetrieveRequest schedReq;
        schedReq.archiveFileID = rsr.archiveFile.archiveFileID;
        schedReq.dstURL = rsr.fileBufferURL;
        schedReq.appendFileSizeToDstURL(rsr.archiveFile.fileSize);
        schedReq.diskFileInfo = rsr.archiveFile.diskFileInfo;

        // dsrr.errorReportURL:  We leave this bank as the reporting will be done to the repack request,
        // stored in the repack info.
        rr->setSchedulerRequest(schedReq);

        // Add the disk system information if needed.
        try {
          auto dsName = diskSystemList.getDSName(schedReq.dstURL);
          rr->setDiskSystemName(dsName);
        } catch (std::out_of_range &) {}

        // Set the repack info.
        RetrieveRequest::RetrieveReqRepackInfo rRRepackInfo;
        try {
          for (const auto & [first, second]: archiveRoutesMap.at(rsr.archiveFile.storageClass)) {
            rRRepackInfo.archiveRouteMap[second.copyNb] = second.tapePoolName;
          }

          //Check that we do not have the same destination tapepool for two different copyNb
          for(const auto & currentCopyNbTapePool: rRRepackInfo.archiveRouteMap){
            int nbTapepool = std::count_if(
               rRRepackInfo.archiveRouteMap.begin(), rRRepackInfo.archiveRouteMap.end(),
               [&currentCopyNbTapePool](const std::pair<uint32_t,std::string> & copyNbTapepool)
                 {
                   return copyNbTapepool.second == currentCopyNbTapePool.second;
                 });
            if (nbTapepool != 1) {
              throw cta::ExpandRepackRequestException(
                "In postgresscheddb::RepackRequest::addSubrequestsAndUpdateStats(), "
                "found the same destination tapepool for different copyNb.");
            }
          }
	} catch (std::out_of_range &) {

          notCreatedSubrequests.emplace_back(rsr);
          failedCreationStats.files++;
          failedCreationStats.bytes += rsr.archiveFile.fileSize;

          log::ScopedParamContainer params(lc);
          params.add("fileID", rsr.archiveFile.archiveFileID)
                .add("diskInstance", rsr.archiveFile.diskInstance)
                .add("storageClass", rsr.archiveFile.storageClass);

          std::stringstream storageClassList;
          bool first=true;
          for (auto & sc: archiveRoutesMap) {
            std::string storageClass;
            storageClass = sc.first;
            storageClassList << (first?"":" ") << " sc=" << storageClass << " rc=" << sc.second.size();
          }

          params.add("storageClassList", storageClassList.str());
          lc.log(log::ERR, "In postgresscheddb::RepackRequest::addSubrequests(): not such archive route.");
          goto nextSubrequest;
        }
	rRRepackInfo.copyNbsToRearchive   = rsr.copyNbsToRearchive;
        rRRepackInfo.fileBufferURL        = rsr.fileBufferURL;
        rRRepackInfo.fSeq                 = rsr.fSeq;
        rRRepackInfo.isRepack             = true;
        rRRepackInfo.repackRequestId      = 0; // todo: m_repackRequest.getAddressIfSet();
        if (rsr.hasUserProvidedFile) {
          rRRepackInfo.hasUserProvidedFile = true;
        }
	rr->setRepackInfo(rRRepackInfo);

        // Set the queueing parameters
        common::dataStructures::RetrieveFileQueueCriteria fileQueueCriteria;
        fileQueueCriteria.archiveFile = rsr.archiveFile;
        fileQueueCriteria.mountPolicy = m_mountPolicy;
        rr->setRetrieveFileQueueCriteria(fileQueueCriteria);

        // Decide of which vid we are going to retrieve from. Here, if we can retrieve from the repack VID, we
        // will set the initial recall on it. Retries will we requeue to best VID as usual if needed.
        std::string bestVid;
        uint32_t activeCopyNumber = 0;
        if (noRecall) {
          bestVid = repackInfo.vid;
        } else {
          //No --no-recall flag, make sure we have a copy on the vid we intend to repack.
          for (auto & tc: rsr.archiveFile.tapeFiles) {
            if (tc.vid == repackInfo.vid) {
              try {
                // Try to select the repack VID from a one-vid list.
                Helpers::selectBestVid4Retrieve({repackInfo.vid}, m_catalogue, *m_txn, true);
                bestVid = repackInfo.vid;
                activeCopyNumber = tc.copyNb;
              } catch (Helpers::NoTapeAvailableForRetrieve &) {}
              break;
            }
          }
        }

        // The repack vid was not appropriate, let's try all candidates.
        if (bestVid.empty()) {
          std::set<std::string> candidateVids;
          for (auto & tc: rsr.archiveFile.tapeFiles) candidateVids.insert(tc.vid);
          try {
            bestVid = Helpers::selectBestVid4Retrieve(candidateVids, m_catalogue, *m_txn, true);
          } catch (Helpers::NoTapeAvailableForRetrieve &) {
            // Count the failure for this subrequest.
            notCreatedSubrequests.emplace_back(rsr);
            failedCreationStats.files++;
            failedCreationStats.bytes += rsr.archiveFile.fileSize;
            log::ScopedParamContainer params(lc);
            params.add("fileId", rsr.archiveFile.archiveFileID)
                  .add("repackVid", repackInfo.vid);
            lc.log(log::ERR,
                "In postgresscheddb::RepackRequest::addSubrequests(): could not "
                "queue a retrieve subrequest. Subrequest failed. Maybe the tape "
                "to repack is disabled ?");
            goto nextSubrequest;
          }
	}
	for (auto &tc: rsr.archiveFile.tapeFiles)
          if (tc.vid == bestVid) {
            activeCopyNumber = tc.copyNb;
            goto copyNbFound;
          }
	{
          // Count the failure for this subrequest.
          notCreatedSubrequests.emplace_back(rsr);
          failedCreationStats.files++;
          failedCreationStats.bytes += rsr.archiveFile.fileSize;

          log::ScopedParamContainer params(lc);
          params.add("fileId", rsr.archiveFile.archiveFileID)
                .add("repackVid", repackInfo.vid)
                .add("chosenVid", bestVid);
          lc.log(log::ERR,
              "In postgresscheddb::RepackRequest::addSubrequests(): could not "
              "find the copyNb for the chosen VID. Subrequest failed.");
          goto nextSubrequest;
        }
      copyNbFound:;
        if (rsr.hasUserProvidedFile) {
            /**
             * As the user has provided the file through the Repack buffer folder,
             * we will not Retrieve the file from the tape. We create the Retrieve
             * Request but directly with the status RJS_ToReportToRepackForSuccess so that
             * this retrieve request is queued and then transformed into an ArchiveRequest.
             */
            rr->setJobStatus(activeCopyNumber,RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
        }

	// We have the best VID. The request is ready to be created after comleting its information.
        rr->setActiveCopyNumber(activeCopyNumber);

        // We can now try to insert the request. It could alredy have been created (in which case it must exist).
        try {
          rr->insert();
          nbRetrieveSubrequestsCreated++;
        } catch (rdbms::UniqueConstraintError &objExists){
          //The retrieve subrequest already exists and is not deleted, we log and don't do anything
          log::ScopedParamContainer params(lc);
          params.add("copyNb",activeCopyNumber)
                .add("repackVid",repackInfo.vid)
                .add("bestVid",bestVid)
                .add("fileId",rsr.archiveFile.archiveFileID);
          lc.log(log::ERR,
            "In postgresscheddb::RepackRequest::addSubrequests(): could not "
            "insert the subrequest because it already exists, continuing expansion");
          goto nextSubrequest;
        } catch (exception::Exception & ex) {
          // We can fail to serialize here...
          // Count the failure for this subrequest.
          notCreatedSubrequests.emplace_back(rsr);
          failedCreationStats.files++;
          failedCreationStats.bytes += rsr.archiveFile.fileSize;

          log::ScopedParamContainer params(lc);
          params.add("fileId", rsr.archiveFile.archiveFileID)
                .add("repackVid", repackInfo.vid)
                .add("bestVid", bestVid)
                .add("ExceptionMessage", ex.getMessageValue());
          lc.log(log::ERR,
              "In postgresscheddb::RepackRequest::addSubrequests(): could not "
              "insert the subrequest.");
        }
      }
    nextSubrequest:
      nbSubReqProcessed++;
      subReqItor++;
    }

    if (notCreatedSubrequests.size()) {
      log::ScopedParamContainer params(lc);
      params.add("files", failedCreationStats.files);
      params.add("bytes", failedCreationStats.bytes);
      reportRetrieveCreationFailures(notCreatedSubrequests);
      update();
      commit();
      lc.log(log::ERR,
        "In postgresscheddb::RepackRequest::addSubRequests(), reported the "
        "failed creation of Retrieve Requests to the Repack request");
    }
  }

  setLastExpandedFSeq(fSeq);
  update();
  commit();
  return nbRetrieveSubrequestsCreated;
}

void RepackRequest::expandDone()
{
   throw cta::exception::Exception("Not implemented");
}

void RepackRequest::fail()
{
   throw cta::exception::Exception("Not implemented");
}

void RepackRequest::requeueInToExpandQueue(log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

void RepackRequest::setExpandStartedAndChangeStatus()
{
   throw cta::exception::Exception("Not implemented");
}

void RepackRequest::fillLastExpandedFSeqAndTotalStatsFile(uint64_t &fSeq, TotalStatsFiles &totalStatsFiles)
{
   throw cta::exception::Exception("Not implemented");
}

void RepackRequest::setVid(const std::string &vid)
{
  repackInfo.vid = vid;
}

void RepackRequest::setType(common::dataStructures::RepackInfo::Type repackType)
{
  typedef common::dataStructures::RepackInfo::Type RepackType;
  switch (repackType) {
  case RepackType::MoveAndAddCopies:
    // Nothing to do, this is the default case.
    break;
  case RepackType::AddCopiesOnly:
    m_isMove = false;
    break;
  case RepackType::MoveOnly:
    m_addCopies = false;
    break;
  default:
    throw exception::Exception("In RepackRequest::setType(): unexpected type.");
  }
}

void RepackRequest::setBufferURL(const std::string & bufferURL)
{
  repackInfo.repackBufferBaseURL = bufferURL;
}

void RepackRequest::setMountPolicy(const common::dataStructures::MountPolicy &mp)
{
  m_mountPolicy = mp;
}

void RepackRequest::setNoRecall(const bool noRecall)
{
  m_noRecall = noRecall;
}

void RepackRequest::setCreationLog(const common::dataStructures::EntryLog & creationLog)
{
  m_creationLog = creationLog;
}

void RepackRequest::update() const
{
  throw cta::exception::Exception("Not implemented");
}

void RepackRequest::insert()
{
  cta::postgresscheddb::sql::RepackJobQueueRow rjr;

  rjr.vid             = repackInfo.vid;
  rjr.bufferUrl       = repackInfo.repackBufferBaseURL;
  rjr.mountPolicyName = m_mountPolicy.name;
  rjr.isNoRecall      = m_noRecall;
  rjr.createLog       = m_creationLog;
  rjr.isMove          = m_isMove;
  rjr.isAddCopies     = m_addCopies;

  /* [Protobuf to-be-replaced] keeping this logic in a comment to facilitate
   * future rewrite using DB columns directly instead of inserting Protobuf objects
   *
   * // when request is inserted we expect there to be no subrequests or destinatioInfos
   * // set yet; just check
   * postgresscheddb::blobser::RepackSubRequestPointers srp;
   * postgresscheddb::blobser::RepackDestinationInfos di;
   */
  if (repackInfo.destinationInfos.size()>0 || m_subreqp.size()>0)
    throw cta::exception::Exception(
      "RepackRequest::insert expected zero subreqs and desintionInfos in new request");

  /* [Protobuf to-be-replaced] keeping this logic in a comment to facilitate
   * future rewrite using DB columns directly instead of inserting Protobuf objects
   *
   * srp.SerializeToString(&rjr.subReqProtoBuf);
   * di.SerializeToString(&rjr.destInfoProtoBuf);
   */
  log::ScopedParamContainer params(m_lc);
  rjr.addParamsToLogContext(params);

  m_txn.reset(new postgresscheddb::Transaction(m_connPool));

  try {
    rjr.insert(*m_txn);
  } catch(exception::Exception &ex) {
    params.add("exeptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In RepackRequest::insert(): failed to queue request.");
    throw;
  }

  m_lc.log(log::INFO, "In RepackRequest::insert(): added request to queue.");
}

void RepackRequest::commit()
{
  if (m_txn) {
    m_txn->commit();
  }
  m_txn.reset();
}

RepackRequest& RepackRequest::operator=(const postgresscheddb::sql::RepackJobQueueRow &row)
{
  /* [Protobuf to-be-replaced] keeping this logic in a comment to facilitate
   * future rewrite using DB columns directly instead of inserting Protobuf objects
   *
   * postgresscheddb::blobser::RepackSubRequestPointers srp;
   * postgresscheddb::blobser::RepackDestinationInfos di;
   * srp.ParseFromString(row.subReqProtoBuf);
   * di.ParseFromString(row.destInfoProtoBuf);
   */
  repackInfo.vid = row.vid;
  repackInfo.repackBufferBaseURL = row.bufferUrl;
  repackInfo.type = common::dataStructures::RepackInfo::Type::Undefined;
  if (row.isMove) {
    if (row.isAddCopies) {
      repackInfo.type = common::dataStructures::RepackInfo::Type::MoveAndAddCopies;
    } else {
      repackInfo.type = common::dataStructures::RepackInfo::Type::MoveOnly;
    }
  } else if (row.isAddCopies) {
    repackInfo.type = common::dataStructures::RepackInfo::Type::AddCopiesOnly;
  }

  switch(row.status) {
    case RepackJobStatus::RRS_Pending:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Pending;
      break;
    case RepackJobStatus::RRS_ToExpand:
      repackInfo.status = common::dataStructures::RepackInfo::Status::ToExpand;
      break;
    case RepackJobStatus::RRS_Starting:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Starting;
      break;
    case RepackJobStatus::RRS_Running:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Running;
      break;
    case RepackJobStatus::RRS_Complete:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Complete;
      break;
    case RepackJobStatus::RRS_Failed:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Failed;
      break;
    default:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Undefined;
      break;
  }

  repackInfo.totalFilesOnTapeAtStart = row.totalFilesOnTapeAtStart;
  repackInfo.totalBytesOnTapeAtStart = row.totalBytesOnTapeAtStart;
  repackInfo.allFilesSelectedAtStart = row.allFilesSelectedAtStart;
  repackInfo.totalFilesToArchive   = row.totalFilesToArchive;
  repackInfo.totalBytesToArchive   = row.totalBytesToArchive;
  repackInfo.totalFilesToRetrieve  = row.totalFilesToRetrieve;
  repackInfo.totalBytesToRetrieve  = row.totalBytesToRetrieve;
  repackInfo.failedFilesToArchive  = row.failedToArchiveFiles;
  repackInfo.failedBytesToArchive  = row.failedToArchiveBytes;
  repackInfo.failedFilesToRetrieve = row.failedToRetrieveFiles;
  repackInfo.failedBytesToRetrieve = row.failedToRetrieveBytes;
  repackInfo.lastExpandedFseq      = row.lastExpandedFseq;
  repackInfo.userProvidedFiles     = row.userProvidedFiles;
  repackInfo.retrievedFiles        = row.retrievedFiles;
  repackInfo.retrievedBytes        = row.retrievedBytes;
  repackInfo.archivedFiles         = row.archivedFiles;
  repackInfo.archivedBytes         = row.archivedBytes;
  repackInfo.isExpandFinished      = row.isExpandFinished;
  repackInfo.noRecall              = row.isNoRecall;
  repackInfo.creationLog           = row.createLog;
  repackInfo.repackFinishedTime    = row.repackFinishedTime;

  repackInfo.destinationInfos.clear();
  /* [Protobuf to-be-replaced] keeping this logic in a comment to facilitate
   * future rewrite using DB columns directly instead of inserting Protobuf objects
   *
   * for(auto &d: di.infos()) {
   *   repackInfo.destinationInfos.emplace_back();
   *   repackInfo.destinationInfos.back().vid   = d.vid();
   *   repackInfo.destinationInfos.back().files = d.files();
   *   repackInfo.destinationInfos.back().bytes = d.bytes();
   *   }
   */

  m_subreqp.clear();
  /* [Protobuf to-be-replaced] keeping this logic in a comment to facilitate
   * future rewrite using DB columns directly instead of inserting Protobuf objects
   *
   * for(auto &s: srp.reqs()) {
   *   m_subreqp.emplace_back();
   *   m_subreqp.back().fSeq                = s.fseq();
   *   m_subreqp.back().address             = s.address();
   *   m_subreqp.back().isRetrieveAccounted = s.retrieve_accounted();
   *   for(auto &re: s.archive_copynb_accounted()) {
   *     m_subreqp.back().archiveCopyNbAccounted.insert(re);
   *   }
   *   m_subreqp.back().isSubreqDeleted     = s.subrequest_deleted();
   * }
   */
  return *this;
}

} // namespace cta::postgresscheddb
