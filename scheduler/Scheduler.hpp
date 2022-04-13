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

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "catalogue/TapeDrivesCatalogueState.hpp"

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/QueueAndMountSummary.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/TestSourceType.hpp"
#include "common/dataStructures/UpdateFileStorageClassRequest.hpp"
#include "common/dataStructures/WriteTestResult.hpp"
#include "common/Timer.hpp"

#include "disk/DiskFile.hpp"
#include "disk/DiskReporter.hpp"
#include "disk/DiskReporterFactory.hpp"

#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/TimingList.hpp"
#include "scheduler/RepackRequest.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"

#include "tapeserver/daemon/TapedConfiguration.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

class ArchiveJob;
class RetrieveJob;

namespace common {
namespace dataStructures {
struct LogicalLibrary;
}
}
/**
 * Class implementing a tape resource scheduler. This class is the main entry point
 * for most of the operations on both the tape file catalogue and the object store for
 * queues. An exception is although used for operations that would trivially map to
 * catalogue operations.
 * The scheduler is the unique entry point to the central storage for taped. It is
 *
 */
CTA_GENERATE_EXCEPTION_CLASS(ExpandRepackRequestException);

class Scheduler {
public:
  /**
   * Constructor.
   */
  Scheduler(
    cta::catalogue::Catalogue &catalogue,
    SchedulerDatabase &db, const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount);
    // TODO: we have out the mount policy parameters here temporarily we will remove them once we know where to put them

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
  std::string queueArchiveWithGivenId(const uint64_t archiveFileId, const std::string &instanceName,
    const cta::common::dataStructures::ArchiveRequest &request, log::LogContext &lc);

  /**
   * Queue a retrieve request.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   * return an opaque id (string) that can be used to cancel the retrieve request.
   */
  std::string queueRetrieve(const std::string &instanceName, cta::common::dataStructures::RetrieveRequest &request,
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
  void abortRetrieve(const std::string &instanceName,
    const cta::common::dataStructures::CancelRetrieveRequest &request, log::LogContext & lc);

  /**
   * Delete a job from the failed queue.
   */
  void deleteFailed(const std::string &objectId, log::LogContext & lc);

  void queueRepack(const common::dataStructures::SecurityIdentity &cliIdentity, const SchedulerDatabase::QueueRepackRequest & repackRequest, log::LogContext & lc);
  void cancelRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, log::LogContext & lc);
  std::list<cta::common::dataStructures::RepackInfo> getRepacks();
  cta::common::dataStructures::RepackInfo getRepack(const std::string &vid);
  bool isBeingRepacked(const std::string &vid);

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
   * @param desiredState, the structure that contains the desired state informations
   */
  void setDesiredDriveState(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string & driveName,
    const common::dataStructures::DesiredDriveState & desiredState, log::LogContext & lc);

  bool checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo & driveInfo, log::LogContext & lc);

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
   * Creates a Table in the Database for a new Tape Drives
   * @param driveInfo default info of the Tape Drive
   * @param desiredState the structure that contains the desired state informations
   * @param type the type of the session, if any
   * @param status the state of the drive. Reporting the state to down will also
   * @param status The identity of the user requesting the drive to put up or down.
   */
  void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
    const common::dataStructures::DesiredDriveState & desiredState, const common::dataStructures::MountType& type,
    const common::dataStructures::DriveStatus& status, const tape::daemon::TpconfigLine& tpConfigLine,
    const common::dataStructures::SecurityIdentity& identity, log::LogContext & lc);

  /**
   * Reports the configuration of the drive to the objectstore.
   * @param driveName the name of the drive to report the config to the objectstore
   * @param tapedConfig the config of the drive to report to the objectstore.
   */
  void reportDriveConfig(const cta::tape::daemon::TpconfigLine& tpConfigLine, const cta::tape::daemon::TapedConfiguration& tapedConfig, log::LogContext& lc);

  /**
   * Dumps the state of an specifig drive
   * @param tapeDriveName
   * @return An optional drive state structures.
   */
  std::optional<cta::common::dataStructures::TapeDrive> getDriveState(const std::string& tapeDriveName,
    log::LogContext* lc) const;

  /**
   * Dumps the states of all drives for display
   * @param cliIdentity
   * @return A list of drive state structures.
   */
  std::list<cta::common::dataStructures::TapeDrive> getDriveStates(
    const cta::common::dataStructures::SecurityIdentity &cliIdentity, log::LogContext & lc) const;

  /*============== Actual mount scheduling and queue status reporting ========*/
private:
  typedef std::pair<std::string, common::dataStructures::MountType> TapePoolMountPair;
  typedef std::pair<std::string, common::dataStructures::MountType> VirtualOrganizationMountPair;

  struct MountCounts {
    uint32_t totalMounts = 0;
    struct AutoZeroUint32_t {
      uint32_t value = 0;
    };
    std::map<std::string, AutoZeroUint32_t> activityMounts;
  };
  typedef std::map<TapePoolMountPair, MountCounts> ExistingMountSummaryPerTapepool;
  typedef std::map<VirtualOrganizationMountPair, MountCounts> ExistingMountSummaryPerVo;

  const std::set<std::string> c_mandatoryEnvironmentVariables = {"XrdSecPROTOCOL", "XrdSecSSSKT"};

  /**
   * Common part to getNextMountDryRun() and getNextMount() to populate mount decision info.
   * The structure should be pre-loaded by the calling function.
   */
  void sortAndGetTapesForMountInfo(std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo> &mountInfo,
    const std::string & logicalLibraryName, const std::string & driveName, utils::Timer & timer,
    ExistingMountSummaryPerTapepool & existingMountsDistinctTypeSummaryPerTapepool, ExistingMountSummaryPerVo & existingMountBasicTypeSummaryPerVo, std::set<std::string> & tapesInUse, std::list<catalogue::TapeForWriting> & tapeList,
    double & getTapeInfoTime, double & candidateSortingTime, double & getTapeForWriteTime, log::LogContext & lc);

  /**
   * Checks wether the tape can be repacked of not.
   * A tape can be repacked if it exists, it is full, not BROKEN and not DISABLED unless there the --disabledtape flag has been provided by the user.
   * @param vid the vid of the tape to check
   * @param repackRequest the associated repackRequest to check the tape can be repacked
   * @throws a UserError exception if the tape cannot be repacked
   */
  void checkTapeCanBeRepacked(const std::string & vid, const SchedulerDatabase::QueueRepackRequest & repackRequest);

  std::optional<common::dataStructures::LogicalLibrary> getLogicalLibrary(const std::string &libraryName, double &getLogicalLibraryTime);

  void deleteRepackBuffer(std::unique_ptr<cta::disk::Directory> repackBuffer, cta::log::LogContext & lc);

  uint64_t getNbFilesAlreadyArchived(const common::dataStructures::ArchiveFile& archiveFile);

  /**
   * Checks that the environment variables needed by the tapeserver to work
   * are set correctly.
   */
  void checkNeededEnvironmentVariables();

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

  /**
  * Modify the state of the specified tape. Intermediate states may be temporarily applied
  * until the final desired state is achieved.
  * @param admin, the person or the system who modified the state of the tape
  * @param vid the VID of the tape to change the state
  * @param state the desired final state
  * @param stateReason the reason why the state changes, if the state is ACTIVE and the stateReason is std::nullopt, the state will be reset to null
  */
  void triggerTapeStateChange(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const std::optional<std::string> & stateReason, log::LogContext& logContext);

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

  cta::catalogue::Catalogue & getCatalogue();

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

  std::unique_ptr<TapeDrivesCatalogueState> m_tapeDrivesState;
};  // class Scheduler

}  // namespace cta
