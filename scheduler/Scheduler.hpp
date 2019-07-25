/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

#include "catalogue/Catalogue.hpp"

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/TestSourceType.hpp"
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/UpdateFileStorageClassRequest.hpp"
#include "common/dataStructures/WriteTestResult.hpp"
#include "common/dataStructures/QueueAndMountSummary.hpp"
#include "common/Timer.hpp"

#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/TimingList.hpp"
#include "scheduler/TapeMount.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/RepackRequest.hpp"
#include "objectstore/RetrieveRequest.hpp"
#include "objectstore/ArchiveRequest.hpp"

#include "disk/DiskReporter.hpp"
#include "disk/DiskReporterFactory.hpp"

#include <list>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

namespace cta {

class ArchiveJob;
class RetrieveJob;

/**
 * Class implementing a tape resource scheduler. This class is the main entry point
 * for most of the operations on both the tape file catalogue and the object store for 
 * queues. An exception is although used for operations that would trivially map to
 * catalogue operations.
 * The scheduler is the unique entry point to the central storage for taped. It is 
 * 
 */
class Scheduler {
  
public:

  /**
   * Constructor.
   */
  Scheduler(
    cta::catalogue::Catalogue &catalogue,
    SchedulerDatabase &db, const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount); 
    //TODO: we have out the mount policy parameters here temporarily we will remove them once we know where to put them

  /**
   * Destructor.
   */
  ~Scheduler() throw();
  
  /**
   * Validates that the underlying storages are accessible
   * Lets the exception through in case of failure.
   */
  void ping(log::LogContext & lc);
  
  /**
   * Waits for all scheduler db threads to complete (mostly for unit tests).
   */
  void waitSchedulerDbSubthreadsComplete();

  /**
   * Checks the specified archival could take place and returns a new and
   * unique archive file identifier that can be used by a new archive file
   * within the catalogue.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param storageClassName The name of the storage class of the file to be
   * archived.  The storage class name is only guaranteed to be unique within
   * its disk instance.  The storage class name will be used by the Catalogue
   * to determine the destination tape pool for each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The new archive file identifier.
   */
  uint64_t checkAndGetNextArchiveFileId(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const common::dataStructures::RequesterIdentity &user,
    log::LogContext &lc);

  /** 
   * Queue the specified archive request. 
   * Throws a UserError exception in case of wrong request parameters (ex. no route to tape)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   * @param archiveFileId The archive file indentifier to be associated with the new archive file.
   * @param instanceName name of the EOS instance
   * @param request the archive request
   * @param lc a log context allowing logging from within the scheduler routine.
   * @return
   */
  void queueArchiveWithGivenId(const uint64_t archiveFileId, const std::string &instanceName,
    const cta::common::dataStructures::ArchiveRequest &request, log::LogContext &lc);
  
  /**
   * Queue the ArchiveRequests that have been transformed from Repack RetrieveRequests 
   * @param archiveRequests the list of the ArchiveRequests to queue into the ArchiveQueueToTransferForRepack queue.
   * @param lc a log context allowing logging from within the scheduler routine.
   */
  void queueArchiveRequestForRepackBatch(std::list<cta::objectstore::ArchiveRequest> &archiveRequests,log::LogContext &lc);
  
  /**
   * Queue a retrieve request. 
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  void queueRetrieve(const std::string &instanceName, cta::common::dataStructures::RetrieveRequest &request,
    log::LogContext &lc);
  
  /** 
   * Delete an archived file or a file which is in the process of being archived.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  void deleteArchive(const std::string &instanceName,
    const cta::common::dataStructures::DeleteArchiveRequest &request,
    log::LogContext & lc);
  
  /** 
   * Cancel an ongoing retrieval.
   * Throws a UserError exception in case of wrong request parameters (ex. file not being retrieved)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  void cancelRetrieve(const std::string &instanceName, 
    const cta::common::dataStructures::CancelRetrieveRequest &request);
  
  /** 
   * Update the file information of an archived file.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  void updateFileInfo(const std::string &instanceName, 
    const cta::common::dataStructures::UpdateFileInfoRequest &request);
  
  /** 
   * Update the storage class of an archived file.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown storage class)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  void updateFileStorageClass(const std::string &instanceName, 
    const cta::common::dataStructures::UpdateFileStorageClassRequest &request);
  
  /** 
   * List the storage classes that a specific user is allowed to use (the ones belonging to the instance from where
   * the command was issued)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  std::list<cta::common::dataStructures::StorageClass> listStorageClass(const std::string &instanceName,
    const cta::common::dataStructures::ListStorageClassRequest &request);

  /**
   * Labeling is treated just like archivals and retrievals (no drive dedication is required). When an admin issues a 
   * labeling command, the operation gets queued just like a normal user operation, and the first drive that can 
   * accomplish it will dequeue it.
   */
  void queueLabel(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid,
    const bool force);

  void queueRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, 
    const std::string & bufferURL, const common::dataStructures::RepackInfo::Type repackType, const common::dataStructures::MountPolicy &mountPolicy, log::LogContext & lc);
  void cancelRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, log::LogContext & lc);
  std::list<cta::common::dataStructures::RepackInfo> getRepacks();
  cta::common::dataStructures::RepackInfo getRepack(const std::string &vid);

  std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > getPendingArchiveJobs(log::LogContext &lc) const;
  std::list<cta::common::dataStructures::ArchiveJob> getPendingArchiveJobs(const std::string &tapePoolName, log::LogContext &lc) const;
  std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > getPendingRetrieveJobs(log::LogContext &lc) const;
  std::list<cta::common::dataStructures::RetrieveJob> getPendingRetrieveJobs(const std::string &vid, log::LogContext &lc) const;
  
  /*============== Drive state management ====================================*/
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchDrive);
  
  /**
   * Gets the desired drive state from object store. Used by the tape drive, when scheduling.
   * @param driveName
   * @return The structure representing the desired states
   */
  common::dataStructures::DesiredDriveState getDesiredDriveState(const std::string &driveName, log::LogContext & lc);
  
  /**
   * Sets the desired drive state. This function is used by the front end to pass instructions to the 
   * object store for the requested drive status. The status is reset to down by the drives
   * on hardware failures.
   * @param cliIdentity The identity of the user requesting the drive to put up of down.
   * @param driveName The drive name
   * @param up indicates whether the drive should be put up or down.
   * @param force indicates whether we want to force the drive to be up.
   */ //TODO: replace the 2 bools with a structure.
  void setDesiredDriveState(const cta::common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &driveName, const bool up, const bool force, log::LogContext & lc);
  
  /**
   * Remove drive from the drive register.
   * 
   * @param cliIdentity The identity of the user requesting the drive removal.
   * @param driveName The drive name
   */
  void removeDrive(const cta::common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &driveName, log::LogContext & lc);
  
  /**
   * Reports the state of the drive to the object store. This information is then reported
   * to the user through the command line interface, via getDriveStates(). This function
   * any necessary field in the drive's state. The drive entry will be created if necessary.
   * @param defaultState a drive state containing all the default values 
   * @param type the type of the session, if any
   * @param status the state of the drive. Reporting the state to down will also 
   * mean that  the desired state should be reset to down following an hardware
   * error encountered by the drive.
   */
  void reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo, cta::common::dataStructures::MountType type, 
    cta::common::dataStructures::DriveStatus status, log::LogContext & lc);

  /**
   * Dumps the states of all drives for display
   * @param cliIdentity
   * @return A list of drive state structures.
   */
  std::list<cta::common::dataStructures::DriveState> getDriveStates(
    const cta::common::dataStructures::SecurityIdentity &cliIdentity, log::LogContext & lc) const;

  /*============== Actual mount scheduling and queue status reporting ========*/
private:
  const size_t c_defaultMaxNbFilesForRepack = 500;
  /**
   * This time is used to limitate the time an expansion of a RepackRequest will take
   * If the RepackRequest has not finished its expansion before this time limit,
   * it will be requeued in the RepackQueueToExpand queue.
   */
  double m_repackRequestExpansionTimeLimit = 30;
  
  typedef std::pair<std::string, common::dataStructures::MountType> TapePoolMountPair;
  struct MountCounts {
    uint32_t totalMounts = 0;
    struct AutoZeroUint32_t {
      uint32_t value = 0;
    };
    std::map<std::string, AutoZeroUint32_t> activityMounts;
  };
  typedef std::map<TapePoolMountPair, MountCounts> ExistingMountSummary;
  
  /**
   * Common part to getNextMountDryRun() and getNextMount() to populate mount decision info.
   * The structure should be pre-loaded by the calling function.
   */
  void sortAndGetTapesForMountInfo(std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> &mountInfo, 
    const std::string & logicalLibraryName, const std::string & driveName, utils::Timer & timer, 
    ExistingMountSummary & existingMountsSummary, std::set<std::string> & tapesInUse, std::list<catalogue::TapeForWriting> & tapeList,
    double & getTapeInfoTime, double & candidateSortingTime, double & getTapeForWriteTime, log::LogContext & lc);
  
  /**
   * Checks wether the tape is full before repacking
   * @param vid the vid of the tape to check
   * @throws a UserError exception if the vid does not exist or if
   * the tape is not full
   */
  void checkTapeFullBeforeRepack(std::string vid);
  
public:
  /**
   * Run the mount decision logic lock free, so we have no contention in the 
   * most usual case where there is no mount to create.
   * @param logicalLibraryName library for the drive we are scheduling
   * @param driveName name of the drive we are scheduling
   * @param lc log context
   * @return true if a valid mount would have been found.
   */
  bool getNextMountDryRun(const std::string &logicalLibraryName, const std::string &driveName, log::LogContext & lc);
  /**
   * Actually decide which mount to do next for a given drive.
   * @param logicalLibraryName library for the drive we are scheduling
   * @param driveName name of the drive we are scheduling
   * @param lc log context
   * @return unique pointer to the tape mount structure. Next step for the user will be find which type of mount this is.
   */
  std::unique_ptr<TapeMount> getNextMount(const std::string &logicalLibraryName, const std::string &driveName, log::LogContext & lc);
  /**
   * A function returning 
   * @param lc
   * @return 
   */
  std::list<common::dataStructures::QueueAndMountSummary> getQueuesAndMountSummaries(log::LogContext & lc);
  
  /*======================== Archive reporting support =======================*/
  /**
   * Batch job factory
   * 
   * @param filesRequested the number of files requested
   * @param logContext
   * @return a list of unique_ptr to the next successful archive jobs to report. 
   * The list is empty when no more jobs can be found. Will return jobs (if 
   * available) up to specified number.
   */
  std::list<std::unique_ptr<ArchiveJob>> getNextArchiveJobsToReportBatch(uint64_t filesRequested,
    log::LogContext &logContext);
  
  void reportArchiveJobsBatch(std::list<std::unique_ptr<ArchiveJob>> & archiveJobsBatch,
      cta::disk::DiskReporterFactory & reporterFactory, log::TimingList&, utils::Timer &, log::LogContext &);
  
  /*============== Repack support ===========================================*/
  // Promotion of requests
  void promoteRepackRequestsToToExpand(log::LogContext & lc);
  // Expansion support
  std::unique_ptr<RepackRequest> getNextRepackRequestToExpand();
  void expandRepackRequest(std::unique_ptr<RepackRequest> & repqckRequest, log::TimingList& , utils::Timer &, log::LogContext &);
  // Scheduler level will not distinguish between report types. It will just do a getnext-report cycle.
  class RepackReportBatch {
    friend Scheduler;
  private:
    std::unique_ptr<SchedulerDatabase::RepackReportBatch> m_DbBatch;
  public:
    void report(log::LogContext & lc);
    bool empty() { return nullptr == m_DbBatch; }
  };
  RepackReportBatch getNextRepackReportBatch(log::LogContext & lc);
  std::list<Scheduler::RepackReportBatch> getRepackReportBatches(log::LogContext &lc);
  
  RepackReportBatch getNextSuccessfulRetrieveRepackReportBatch(log::LogContext &lc);
  RepackReportBatch getNextFailedRetrieveRepackReportBatch(log::LogContext &lc);
  RepackReportBatch getNextSuccessfulArchiveRepackReportBatch(log::LogContext &lc);
  RepackReportBatch getNextFailedArchiveRepackReportBatch(log::LogContext &lc);
  
  /*======================= Failed archive jobs support ======================*/
  SchedulerDatabase::JobsFailedSummary getArchiveJobsFailedSummary(log::LogContext &lc);

  /*======================= Retrieve reporting support =======================*/
  /*!
   * Batch job factory
   * 
   * @param filesRequested    the number of files requested
   * @param logContext
   *
   * @returns    A list of unique_ptr to the next successful retrieve jobs to report. The list
   *             is empty when no more jobs can be found. Will return jobs (if available) up
   *             to specified number.
   */
  std::list<std::unique_ptr<RetrieveJob>> getNextRetrieveJobsToReportBatch(uint64_t filesRequested,
    log::LogContext &logContext);

  void reportRetrieveJobsBatch(std::list<std::unique_ptr<RetrieveJob>> & retrieveJobsBatch,
      disk::DiskReporterFactory & reporterFactory, log::TimingList&, utils::Timer&, log::LogContext&);

  /*!
   * Batch job factory
   * 
   * @param filesRequested    The number of files requested. Returns available jobs up to the specified
   *                          number.
   * @param logContext        Log Context
   *
   * @returns                 A list of unique_ptr to the next batch of failed retrieve jobs. The list
   *                          is empty when no more jobs can be found.
   */
  std::list<std::unique_ptr<RetrieveJob>>
  getNextRetrieveJobsFailedBatch(uint64_t filesRequested, log::LogContext &logContext);

  /*====================== Failed retrieve jobs support ======================*/
  SchedulerDatabase::JobsFailedSummary getRetrieveJobsFailedSummary(log::LogContext &lc);

  /*======================== Administrator management ========================*/
  void authorizeAdmin(const cta::common::dataStructures::SecurityIdentity &cliIdentity, log::LogContext & lc);
  
  void setRepackRequestExpansionTimeLimit(const double &time);
  
  double getRepackRequestExpansionTimeLimit() const;

private:

  /**
   * The catalogue.
   */
  cta::catalogue::Catalogue &m_catalogue;

  /**
   * The scheduler database.
   */
  SchedulerDatabase &m_db;
  
  const uint64_t m_minFilesToWarrantAMount;
  const uint64_t m_minBytesToWarrantAMount;
}; // class Scheduler

} // namespace cta
