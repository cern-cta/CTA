/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

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
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/TimingList.hpp"
#include "common/utils/Timer.hpp"
#include "disk/DiskFile.hpp"
#include "disk/DiskReporter.hpp"
#include "disk/DiskReporterFactory.hpp"
#include "scheduler/IScheduler.hpp"
#include "scheduler/RepackRequest.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

namespace cta {

namespace catalogue {
class Catalogue;
}

class ArchiveJob;
class RetrieveJob;

namespace common::dataStructures {
struct LogicalLibrary;
struct PhysicalLibrary;
}  // namespace common::dataStructures

/**
 * Class implementing a tape resource scheduler. This class is the main entry point
 * for most of the operations on both the tape file catalogue and the object store for
 * queues. An exception is although used for operations that would trivially map to
 * catalogue operations.
 * The scheduler is the unique entry point to the central storage for taped. It is
 *
 */
CTA_GENERATE_EXCEPTION_CLASS(ExpandRepackRequestException);

class Scheduler : public IScheduler {
public:
  /**
   * Constructor.
   */
  Scheduler(cta::catalogue::Catalogue& catalogue,
            SchedulerDatabase& db,
            const std::string& schedulerBackendName,
            const uint64_t minFilesToWarrantAMount = 5,
            const uint64_t minBytesToWarrantAMount = 2000000);
  // TODO: we have out the mount policy parameters here temporarily we will
  // remove them once we know where to put them

  /**
   * Destructor.
   */
  ~Scheduler() override = default;

  /**
   * Validates that the underlying storages are accessible
   * Lets the exception through in case of failure.
   */
  void ping(log::LogContext& lc) override;

  /*
   * Get scheduler backend instance name
   * passed to m_schedulerBackendName via init method from
   * "cta.scheduler_backend_name" frontend
   * or "general SchedulerBackendName" taped configuration file
   *
   * @return name of the scheduler backend instance name specified by the operator in the cfg file
   */
  const std::string& getSchedulerBackendName() const { return m_schedulerBackendName; }

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
  uint64_t checkAndGetNextArchiveFileId(const std::string& diskInstanceName,
                                        const std::string& storageClassName,
                                        const common::dataStructures::RequesterIdentity& user,
                                        log::LogContext& lc);

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
  std::string queueArchiveWithGivenId(const uint64_t archiveFileId,
                                      const std::string& instanceName,
                                      const cta::common::dataStructures::ArchiveRequest& request,
                                      log::LogContext& lc);

  /**
   * Queue a retrieve request.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   * return an opaque id (string) that can be used to cancel the retrieve request.
   */
  std::string queueRetrieve(const std::string& instanceName,
                            cta::common::dataStructures::RetrieveRequest& request,
                            log::LogContext& lc);

  /**
   * Delete an archived file or a file which is in the process of being archived.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  void deleteArchive(std::string_view instanceName,
                     const cta::common::dataStructures::DeleteArchiveRequest& request,
                     log::LogContext& lc);

  /**
   * Cancel an ongoing retrieval.
   * Throws a UserError exception in case of wrong request parameters (ex. file not being retrieved)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  void abortRetrieve(const std::string& instanceName,
                     const cta::common::dataStructures::CancelRetrieveRequest& request,
                     log::LogContext& lc);

  /**
   * Delete a job from the failed queue.
   */
  void deleteFailed(const std::string& objectId, log::LogContext& lc);

  void queueRepack(const common::dataStructures::SecurityIdentity& cliIdentity,
                   const SchedulerDatabase::QueueRepackRequest& repackRequest,
                   log::LogContext& lc);
  void cancelRepack(const cta::common::dataStructures::SecurityIdentity& cliIdentity,
                    const std::string& vid,
                    log::LogContext& lc);
  bool repackExists();
  std::list<cta::common::dataStructures::RepackInfo> getRepacks();
  cta::common::dataStructures::RepackInfo getRepack(const std::string& vid);
  bool isBeingRepacked(const std::string& vid);

  std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob>, std::less<>>
  getPendingArchiveJobs(log::LogContext& lc) const;
  std::list<cta::common::dataStructures::ArchiveJob> getPendingArchiveJobs(const std::string& tapePoolName,
                                                                           log::LogContext& lc) const;
  std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob>, std::less<>>
  getPendingRetrieveJobs(log::LogContext& lc) const;
  std::list<cta::common::dataStructures::RetrieveJob> getPendingRetrieveJobs(std::optional<std::string> vid,
                                                                             log::LogContext& lc) const;

  /*============== Drive state management ====================================*/
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchDrive);

  /**
   * Gets the desired drive state from object store. Used by the tape drive, when scheduling.
   * @param driveName
   * @return The structure representing the desired states
   */
  common::dataStructures::DesiredDriveState getDesiredDriveState(const std::string& driveName,
                                                                 log::LogContext& lc) override;

  /**
   * Sets the desired drive state. This function is used by the front end to pass instructions to the
   * object store for the requested drive status. The status is reset to down by the drives
   * on hardware failures.
   * @param cliIdentity The identity of the user requesting the drive to put up of down.
   * @param driveName The drive name
   * @param desiredState, the structure that contains the desired state informations
   */
  void setDesiredDriveState(const cta::common::dataStructures::SecurityIdentity& cliIdentity,
                            const std::string& driveName,
                            const common::dataStructures::DesiredDriveState& desiredState,
                            log::LogContext& lc) override;

  bool checkDriveCanBeCreated(const cta::common::dataStructures::DriveInfo& driveInfo, log::LogContext& lc) override;

  /**
   * Remove drive from the drive register.
   *
   * @param driveName The drive name
   */
  void removeDrive(const std::string& driveName, log::LogContext& lc);

  /**
   * Reports the state of the drive to the object store. This function fills
   * any necessary fields in the drive's state. The drive entry will be created if necessary.
   * @param defaultState a drive state containing all the default values
   * @param type the type of the session, if any
   * @param status the state of the drive. Reporting the state to down will also
   * mean that  the desired state should be reset to down following an hardware
   * error encountered by the drive.
   */
  void reportDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                         cta::common::dataStructures::MountType type,
                         cta::common::dataStructures::DriveStatus status,
                         log::LogContext& lc) override;

  /**
   * Creates a Table in the Database for a new Tape Drives
   * @param driveInfo default info of the Tape Drive
   * @param desiredState the structure that contains the desired state informations
   * @param type the type of the session, if any
   * @param status the state of the drive. Reporting the state to down will also
   * @param status The identity of the user requesting the drive to put up or down.
   */
  void createTapeDriveStatus(const common::dataStructures::DriveInfo& driveInfo,
                             const common::dataStructures::DesiredDriveState& desiredState,
                             const common::dataStructures::MountType& type,
                             const common::dataStructures::DriveStatus& status,
                             const tape::daemon::DriveConfigEntry& driveConfigEntry,
                             const common::dataStructures::SecurityIdentity& identity,
                             log::LogContext& lc) override;

  /**
   * Reports the configuration of the drive to the objectstore.
   * @param driveName the name of the drive to report the config to the objectstore
   * @param tapedConfig the config of the drive to report to the objectstore.
   */
  void reportDriveConfig(const cta::tape::daemon::DriveConfigEntry& driveConfigEntry,
                         const cta::tape::daemon::common::TapedConfiguration& tapedConfig,
                         log::LogContext& lc) override;

  /**
   * Returns the status of a specific drive
   * @param tapeDriveName
   * @return An optional drive status structures.
   */
  std::optional<cta::common::dataStructures::DriveStatus> getDriveStatus(const std::string& tapeDriveName,
                                                                         log::LogContext* lc) const;

  /*============== Actual mount scheduling and queue status reporting ========*/
private:
  using TapePoolMountPair = std::pair<std::string, common::dataStructures::MountType>;
  using VirtualOrganizationMountPair = std::pair<std::string, common::dataStructures::MountType>;

  struct MountCounts {
    uint32_t totalMounts = 0;

    struct AutoZeroUint32_t {
      uint32_t value = 0;
    };

    std::map<std::string, AutoZeroUint32_t, std::less<>> activityMounts;
  };

  using ExistingMountSummaryPerTapepool = std::map<TapePoolMountPair, MountCounts>;
  using ExistingMountSummaryPerVo = std::map<VirtualOrganizationMountPair, MountCounts>;

  const std::set<std::string, std::less<>> c_mandatoryEnvironmentVariables = {"XrdSecPROTOCOL", "XrdSecSSSKT"};

  /**
   * Common part to getNextMountDryRun() and getNextMount() to populate mount decision info.
   * The structure should be pre-loaded by the calling function.
   */
  void sortAndGetTapesForMountInfo(std::unique_ptr<SchedulerDatabase::TapeMountDecisionInfo>& mountInfo,
                                   const std::string& logicalLibraryName,
                                   const std::string& driveName,
                                   utils::Timer& timer,
                                   ExistingMountSummaryPerTapepool& existingMountsDistinctTypeSummaryPerTapepool,
                                   ExistingMountSummaryPerVo& existingMountBasicTypeSummaryPerVo,
                                   std::set<std::string, std::less<>>& tapesInUse,
                                   std::vector<catalogue::TapeForWriting>& tapesForWriting,
                                   double& getTapeInfoTime,
                                   double& candidateSortingTime,
                                   double& getTapeForWriteTime,
                                   log::LogContext& lc);

  /**
   * Checks wether the tape can be repacked of not.
   * A tape can be repacked if it exists, it is full, not BROKEN and not DISABLED unless there the --disabledtape flag has been provided by the user.
   * @param vid the vid of the tape to check
   * @param repackRequest the associated repackRequest to check the tape can be repacked
   * @throws a UserError exception if the tape cannot be repacked
   */
  void checkTapeCanBeRepacked(const std::string& vid, const SchedulerDatabase::QueueRepackRequest& repackRequest);

  /* common part for getNextMountDryRun() and getNextMount() to check for disabled logical/physical library */
  bool checkLogicalAndPhysicalLibraryValidForMount(const std::string& libraryName,
                                                   double& checkLogicalAndPhysicalLibrariesTime,
                                                   log::LogContext& lc);

  std::optional<common::dataStructures::LogicalLibrary> getLogicalLibrary(const std::string& libraryName,
                                                                          double& getLogicalLibraryTime);

  std::optional<common::dataStructures::PhysicalLibrary> getPhysicalLibrary(const std::string& libraryName,
                                                                            double& getPhysicalLibraryTime);

  void deleteRepackBuffer(std::unique_ptr<cta::disk::Directory> repackBuffer, cta::log::LogContext& lc);

  /**
   * Checks that the environment variables needed by the tapeserver to work
   * are set correctly.
   */
  void checkNeededEnvironmentVariables();

public:
  /**
   * Return the name of the archive mount policy with highest priority from the mountPolicies passed in parameter
   * The aim is to do the same as ArchiveQueue::getJobsSummary() regarding the priority
   * @param mountPolicies the list of mount policies in order to create the best one.
   * @return the archive mount policy with highest priority
   */
  std::string
  getHighestPriorityArchiveMountPolicyName(const std::vector<common::dataStructures::MountPolicy>& mountPolicies) const;

  /**
   * Return the name of the archive mount policy with lowest request age from the mountPolicies passed in parameter
   * The aim is to do the same as ArchiveQueue::getJobsSummary() regarding the request age
   * @param mountPolicies the list of mount policies in order to create the best one.
   * @return the archive mount policy with lowest request age
   */
  std::string getLowestRequestAgeArchiveMountPolicyName(
    const std::vector<common::dataStructures::MountPolicy>& mountPolicies) const;

  /**
   * Return the name of the retrieve mount policy with highest priority from the mountPolicies passed in parameter
   * The aim is to do the same as RetrieveQueue::getJobsSummary() regarding the priority
   * @param mountPolicies the list of mount policies in order to create the best one.
   * @return the retrieve mount policy with highest priority
   */
  std::string getHighestPriorityRetrieveMountPolicyName(
    const std::vector<common::dataStructures::MountPolicy>& mountPolicies) const;

  /**
   * Return the name of the retrieve mount policy with lowest request age from the mountPolicies passed in parameter
   * The aim is to do the same as RetrieveQueue::getJobsSummary() regarding the request age
   * @param mountPolicies the list of mount policies in order to create the best one.
   * @return the retrieve mount policy with lowest request age
   */
  std::string getLowestRequestAgeRetrieveMountPolicyName(
    const std::vector<common::dataStructures::MountPolicy>& mountPolicies) const;

  /**
   * Given a list of mount policies, it compares all of them to extract both the maximum archive priority and the minimum
   * archive age between all entries.
   * @param mountPolicies the list of mount policies in order to create the best one.
   * @return a pair with the max retrieve priority and minimum archive age
   */
  static std::pair<uint64_t, uint64_t>
  getArchiveMountPolicyMaxPriorityMinAge(const std::vector<common::dataStructures::MountPolicy>& mountPolicies);

  /**
   * Given a list of mount policies, it compares all of them to extract both the maximum retrieve priority and the minimum
   * retrieve age between all entries.
   * @param mountPolicies the list of mount policies in order to create the best one.
   * @return a pair with the max retrieve priority and minimum retrieve age
   */
  static std::pair<uint64_t, uint64_t>
  getRetrieveMountPolicyMaxPriorityMinAge(const std::vector<common::dataStructures::MountPolicy>& mountPolicies);

  /**
   * After TapeMountDecisionInfo is received from the Scheduler Database
   * with Potential Mounts we enrich this with information about Existing and Next Mounts
   */
  void getExistingAndNextMounts(SchedulerDatabase::TapeMountDecisionInfo& tmdi, log::LogContext& logContext);
  /**
   * After TapeMountDecisionInfo is received from the Scheduler Database
   * we iterate the Potential Mounts and compare the Mount Polisies with those in the catalogue to
   * select only eligible Mount Policy Names
   */
  void fillMountPolicyNamesForPotentialMounts(SchedulerDatabase::TapeMountDecisionInfo& tmdi,
                                              log::LogContext& logContext);

  /**
   * An internal helper function to build a list of mount policies with the map of the
   * mount policies coming from the queue JobsSummary object (OStoreDB concept)
   * The map contains the name of the mount policies, so it is just a conversion from the name to an entire mount policy object
   * @param mountPoliciesInCatalogue the list of the mountPolicies given by the Catalogue
   * @param queueMountPolicyMap the map of the <mountPolicyName,counter> coming from the queue JobsSummary object
   * @return the list of MountPolicies that are in the map
   */

  std::vector<common::dataStructures::MountPolicy>
  getMountPoliciesInQueue(const std::vector<common::dataStructures::MountPolicy>& mountPoliciesInCatalogue,
                          const std::map<std::string, uint64_t>& queueMountPolicyMap);

  /**
   * Run the mount decision logic lock free, so we have no contention in the
   * most usual case where there is no mount to create.
   * @param logicalLibraryName library for the drive we are scheduling
   * @param driveName name of the drive we are scheduling
   * @param lc log context
   * @return true if a valid mount would have been found.
   */
  bool getNextMountDryRun(const std::string& logicalLibraryName, const std::string& driveName, log::LogContext& lc);
  /**
   * Actually decide which mount to do next for a given drive.
   * Throws a TimeoutException in case the timeout goes out
   * @param logicalLibraryName library for the drive we are scheduling
   * @param driveName name of the drive we are scheduling
   * @param lc log context
   * @param timeout_us get next mount timeout
   * @return unique pointer to the tape mount structure. Next step for the user will be find which type of mount this is.
   */
  std::unique_ptr<TapeMount> getNextMount(const std::string& logicalLibraryName,
                                          const std::string& driveName,
                                          log::LogContext& lc,
                                          uint64_t timeout_us = 0);

  /**
   * A function returning
   * @param lc
   * @return
   */
  std::vector<common::dataStructures::QueueAndMountSummary> getQueuesAndMountSummaries(log::LogContext& lc);

  /**
   * @brief Updates the tape state for a given tape identified by its VID if it is currently in a PENDING state.
   *
   * This method attempts to retrieve the tape information for the specified `queueVid` from the catalogue.
   * If the tape is found and its state is one of the PENDING states (REPACKING_PENDING, BROKEN_PENDING, EXPORTED_PENDING),
   * it updates the tape state to the corresponding final state (REPACKING, BROKEN, EXPORTED), preserving the previous
   * state reason with an additional prepended message.
   *
   * The method also clears the database statistics cache for the tape VID after successful state modification.
   *
   * In case the tape is not found in the catalogue, a warning is logged and the method returns early.
   * If the tape is not in a recognized PENDING state, a warning is logged and no state change occurs.
   *
   * If there is a state mismatch error when modifying the tape state (UserSpecifiedAWrongPrevState exception),
   * the method attempts to detect if the tape state was already updated by another agent and logs accordingly.
   *
   * @param queueVid The unique identifier (VID) of the tape whose state is to be updated.
   * @param logContext Logging context for recording informational and error messages.
   */
  void updateTapeStateFromPending(const std::string& queueVid, cta::log::LogContext& logContext);

  /**
  * Modify the state of the specified tape. Intermediate states may be temporarily applied
  * until the final desired state is achieved.
  * @param admin, the person or the system who modified the state of the tape
  * @param vid the VID of the tape to change the state
  * @param state the desired final state
  * @param stateReason the reason why the state changes, if the state is ACTIVE and the stateReason is std::nullopt, the state will be reset to null
  */
  void triggerTapeStateChange(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& vid,
                              const common::dataStructures::Tape::State& state,
                              const std::optional<std::string>& stateReason,
                              log::LogContext& logContext);

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
                                                                         log::LogContext& logContext);

  void reportArchiveJobsBatch(std::list<std::unique_ptr<ArchiveJob>>& archiveJobsBatch,
                              cta::disk::DiskReporterFactory& reporterFactory,
                              log::TimingList&,
                              utils::Timer&,
                              log::LogContext&);

  /*============== Repack support ===========================================*/
  // Promotion of requests
  void promoteRepackRequestsToToExpand(log::LogContext& lc, size_t repackMaxRequestsToExpand);
  // Expansion support
  std::unique_ptr<RepackRequest> getNextRepackRequestToExpand();
  void expandRepackRequest(const RepackRequest& repackRequest, log::TimingList&, utils::Timer&, log::LogContext&);

  // Scheduler level will not distinguish between report types. It will just do a getnext-report cycle.
  class RepackReportBatch {
    friend Scheduler;
    std::unique_ptr<SchedulerDatabase::RepackReportBatch> m_DbBatch;

  public:
    void report(log::LogContext& lc);

    bool empty() { return nullptr == m_DbBatch; }
  };

  RepackReportBatch getNextRepackReportBatch(log::LogContext& lc);
  std::list<Scheduler::RepackReportBatch> getRepackReportBatches(log::LogContext& lc);

  RepackReportBatch getNextSuccessfulRetrieveRepackReportBatch(log::LogContext& lc);
  RepackReportBatch getNextFailedRetrieveRepackReportBatch(log::LogContext& lc);
  RepackReportBatch getNextSuccessfulArchiveRepackReportBatch(log::LogContext& lc);
  RepackReportBatch getNextFailedArchiveRepackReportBatch(log::LogContext& lc);

  /*======================= Failed archive jobs support ======================*/
  SchedulerDatabase::JobsFailedSummary getArchiveJobsFailedSummary(log::LogContext& lc);

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
                                                                           log::LogContext& logContext);

  void reportRetrieveJobsBatch(std::list<std::unique_ptr<RetrieveJob>>& retrieveJobsBatch,
                               disk::DiskReporterFactory& reporterFactory,
                               log::TimingList&,
                               utils::Timer&,
                               log::LogContext&);

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
  std::list<std::unique_ptr<RetrieveJob>> getNextRetrieveJobsFailedBatch(uint64_t filesRequested,
                                                                         log::LogContext& logContext);

  /*====================== Failed retrieve jobs support ======================*/
  SchedulerDatabase::JobsFailedSummary getRetrieveJobsFailedSummary(log::LogContext& lc);

  /*======================== Administrator management ========================*/
  void authorizeAdmin(const cta::common::dataStructures::SecurityIdentity& cliIdentity, log::LogContext& lc);

  void setRepackRequestExpansionTimeLimit(const double& time);

  double getRepackRequestExpansionTimeLimit() const;

  cta::catalogue::Catalogue& getCatalogue();

private:
  /**
   * The catalogue.
   */
  cta::catalogue::Catalogue& m_catalogue;

  /**
   * The scheduler database.
   */
  SchedulerDatabase& m_db;
  const std::string m_schedulerBackendName;

  const uint64_t m_minFilesToWarrantAMount;
  const uint64_t m_minBytesToWarrantAMount;

  std::unique_ptr<TapeDrivesCatalogueState> m_tapeDrivesState;

};  // class Scheduler

}  // namespace cta
