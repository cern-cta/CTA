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

#include "common/admin/AdminUser.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/MountControl.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/remoteFS/RemotePathAndStatus.hpp"
#include "common/log/LogContext.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "scheduler/TapeMount.hpp"

#include <list>
#include <limits>
#include <map>
#include <stdint.h>
#include <string>
#include <memory>
#include <vector>
#include <stdexcept>
#include <set>

namespace cta {
// Forward declarations for opaque references.
namespace common {
namespace admin {
  class AdminHost;
  class AdminUser;
} // cta::common::admin
namespace archiveRoute {
  class ArchiveRoute;
} // cta::common::archiveRoute
} // cta::common
class ArchiveRequest;
class LogicalLibrary;
class RetrieveRequestDump;
class SchedulerDatabase;
class StorageClass;
class Tape;
class TapeMount;
class TapeSession;
class UserIdentity;
} /// cta

namespace cta {

class ArchiveRequest;

/**
 * Abstract class defining the interface to the database of a tape resource
 * scheduler.
 */
class SchedulerDatabase {
public:

  /**
   * Destructor.
   */
  virtual ~SchedulerDatabase() throw() = 0;
  
  /*============ Basic IO check: validate object store access ===============*/
  /**
   * Validates that the scheduler database is accessible. A trivial operation
   * will be executed to check. The exception is let through in case of problem.
   */
  virtual void ping() = 0;
  
  /*============ Archive management: user side ==============================*/
  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   * @param criteria The criteria retrieved from the CTA catalogue to be used to
   * decide how to queue the request.
   * @param logContext context allowing logging db operation
   */
  virtual void queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request, 
    const cta::common::dataStructures::ArchiveFileQueueCriteria &criteria,
    log::LogContext &logContext) = 0;

  /**
   * Returns all of the queued archive jobs.  The returned jobs are
   * grouped by tape pool and then sorted by creation time, oldest first.
   *
   * @return The queued jobs.
   */
  virtual std::map<std::string, std::list<common::dataStructures::ArchiveJob> >
    getArchiveJobs() const = 0;

  /**
   * Returns the list of queued jobs queued on the specified tape pool.
   * The returned requests are sorted by creation time, oldest first.
   *
   * @param tapePoolName The name of the tape pool.
   * @return The queued requests.
   */
  virtual std::list<cta::common::dataStructures::ArchiveJob> getArchiveJobs(
    const std::string &tapePoolName) const = 0;
  
  /**
   * Deletes the specified archive request.
   *
   * @param archiveFile The ID of the destination file within the
   * archive catalogue.
   */
  virtual void deleteArchiveRequest(
    const std::string &diskInstanceName, 
    uint64_t archiveFileId) = 0;
  
  /*
   * Subclass allowing the tracking and automated cleanup of a 
   * ArchiveToFile requests on the SchdulerDB for deletion.
   * This will mark the request as to be deleted, and then add it to the agent's
   * list. In a second step, the request will be completely deleted when calling
   * the complete() method.
   * In case of failure, the request will be queued to the orphaned requests queue,
   * so that the scheduler picks it up later.
   */ 
  class ArchiveToFileRequestCancelation {
  public:
    virtual void complete() = 0;
    virtual ~ArchiveToFileRequestCancelation() {};
  };

  /*============ Archive management: tape server side =======================*/
  /**
   * The class used by the scheduler database to track the archive mounts
   */
  class ArchiveJob;
  class ArchiveMount {
  public:
    struct MountInfo {
      std::string vid;
      std::string logicalLibrary;
      std::string tapePool;
      std::string drive;
      std::string host;
      uint64_t mountId;
    } mountInfo;
    virtual const MountInfo & getMountInfo() = 0;
    virtual std::list<std::unique_ptr<ArchiveJob>> getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext) = 0;
    virtual void complete(time_t completionTime) = 0;
    virtual void setDriveStatus(common::dataStructures::DriveStatus status, time_t completionTime) = 0;
    virtual void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) = 0;
    virtual ~ArchiveMount() {}
    uint32_t nbFilesCurrentlyOnTape;
  };
  
  /**
   * The class to handle the DB-side of a tape job.
   */
  class ArchiveJob {
  public:
    std::string srcURL;
    std::string archiveReportURL;
    cta::common::dataStructures::ArchiveFile archiveFile;
    cta::common::dataStructures::TapeFile tapeFile;
    /// Indicates a success to the DB. If this is the last job, return true.
    virtual bool succeed() = 0;
    virtual void fail(log::LogContext & lc) = 0;
    virtual void bumpUpTapeFileCount(uint64_t newFileCount) = 0;
    virtual ~ArchiveJob() {}
  };
  
  /*============ Retrieve  management: user side ============================*/

  /**
   * A representation of an existing retrieve queue. This is a (simpler) relative 
   * to the PotentialMount used for mount scheduling. This summary will be used to 
   * decide which retrieve job to use for multiple copy files. 
   * In order to have a stable comparison, we compare on byte number and not file counts.
   */
  struct RetrieveQueueStatistics {
    std::string vid;
    uint64_t bytesQueued;
    uint64_t filesQueued;
    uint64_t currentPriority;
    
    bool operator <(const RetrieveQueueStatistics& right) const {
      return right > * this; // Reuse greater than operator
    }
    
    bool operator >(const RetrieveQueueStatistics& right) const {
      return bytesQueued > right.bytesQueued || currentPriority > right.currentPriority;
    }
    
    static bool leftGreaterThanRight (const RetrieveQueueStatistics& left, const RetrieveQueueStatistics& right) {
      return left > right;
    }

  };
  
  /**
   * Get the retrieve queue statistics for the vids of the tape files from the criteria, that are also
   * listed in the set. 
   * @param criteria the retrieve criteria, containing the list of tape files.
   * @param vidsToConsider list of vids to considers. Other vids should not be considered.
   * @return the list of statistics.
   */
  virtual std::list<RetrieveQueueStatistics> getRetrieveQueueStatistics(
    const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria,
    const std::set<std::string> & vidsToConsider) = 0;
  /**
   * Queues the specified request. As the object store has access to the catalogue,
   * the best queue (most likely to go, and not disabled can be chosen directly there).
   *
   * @param rqst The request.
   * @param criteria The criteria retrieved from the CTA catalogue to be used to
   * decide how to quue the request.
   * @param logContext context allowing logging db operation
   * @return the selected vid (mostly for logging)
   */
  virtual std::string queueRetrieve(const cta::common::dataStructures::RetrieveRequest &rqst,
    const cta::common::dataStructures::RetrieveFileQueueCriteria &criteria, log::LogContext &logContext) = 0;

  /**
   * Returns all of the existing retrieve jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @return All of the existing retrieve jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::map<std::string, std::list<RetrieveRequestDump> > getRetrieveRequests()
    const = 0;

  /**
   * Returns the list of retrieve jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param vid The volume identifier of the tape.
   * @return The list of retrieve jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::list<RetrieveRequestDump> getRetrieveRequestsByVid(
    const std::string &vid) const = 0;
  
  /**
   * Returns the list of retrieve jobs associated with the specified requester
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The requester who created the retrieve request.
   * @return The list of retrieve jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(
    const std::string &requester) const = 0;
  
  /**
   * Deletes the specified retrieve job.
   *
   * @param requester The identity of the requester.
   * @param remoteFile The URL of the destination file.
   */
  virtual void deleteRetrieveRequest(
    const common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &remoteFile) = 0;
  
  /**
   * Returns all of the queued archive jobs.  The returned jobs are
   * grouped by tape pool and then sorted by creation time, oldest first.
   *
   * @return The queued jobs.
   */
  virtual std::map<std::string, std::list<common::dataStructures::RetrieveJob> >
    getRetrieveJobs() const = 0;

  /**
   * Returns the list of queued jobs queued on the specified tape pool.
   * The returned requests are sorted by creation time, oldest first.
   *
   * @param tapePoolName The name of the tape pool.
   * @return The queued requests.
   */
  virtual std::list<cta::common::dataStructures::RetrieveJob> getRetrieveJobs(
    const std::string &tapePoolName) const = 0;
  
  
  /*============ Retrieve management: tape server side ======================*/

  class RetrieveJob;
  class RetrieveMount {
  public:
    struct MountInfo {
      std::string vid;
      std::string logicalLibrary;
      std::string tapePool;
      std::string drive;
      std::string host;
      uint64_t mountId;
    } mountInfo;
    virtual const MountInfo & getMountInfo() = 0;
    virtual std::list<std::unique_ptr<RetrieveJob>> getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext) = 0;
    virtual std::unique_ptr<RetrieveJob> getNextJob(log::LogContext & logContext) = 0;
    virtual void complete(time_t completionTime) = 0;
    virtual void setDriveStatus(common::dataStructures::DriveStatus status, time_t completionTime) = 0;
    virtual void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) = 0;
    virtual ~RetrieveMount() {}
    uint32_t nbFilesCurrentlyOnTape;
  };
  
  class RetrieveJob {
    friend class RetrieveMount;
  public:
    cta::common::dataStructures::RetrieveRequest retrieveRequest;
    cta::common::dataStructures::ArchiveFile archiveFile;
    uint64_t selectedCopyNb;
    virtual void succeed() = 0;
    virtual void fail(log::LogContext &) = 0;
    virtual ~RetrieveJob() {}
  };

  /*============ Label management: user side =================================*/
  // TODO
  
  /*============ Label management: tape server side ==========================*/
  class LabelMount {}; // TODO
  
  /*============ Session management ==========================================*/
  /**
   * A structure describing a potential mount with all the information allowing
   * comparison between mounts.
   */
  struct PotentialMount {
    cta::common::dataStructures::MountType type;    /**< Is this an archive, retireve or label? */
    std::string vid;              /**< The tape VID (for a retieve) */
    std::string tapePool;         /**< The name of the tape pool for both archive 
                                   * and retrieve */
    uint64_t priority;            /**< The priority for the mount, defined as the highest 
                                   * priority of all queued jobs */
    uint64_t maxDrivesAllowed;    /**< The maximum number of drives allowed for this 
                                   * tape pool, defined as the highest value amongst 
                                   * jobs */
    time_t minArchiveRequestAge;  /**< The maximum amount of time to wait before 
                                   * forcing a mount in the absence of enough data. 
                                   * Defined as the smallest value amongst jobs.*/
    uint64_t filesQueued;         /**< The number of files queued for this queue */
    uint64_t bytesQueued;         /**< The amount of data currently queued */
    time_t oldestJobStartTime;    /**< Creation time of oldest request */
    std::string logicalLibrary;   /**< The logical library (for a retrieve) */
    double ratioOfMountQuotaUsed; /**< The [ 0.0, 1.0 ] ratio of existing 
                                   * mounts/quota (for faire share of mounts)*/
    
    bool operator < (const PotentialMount &other) const {
      if (priority < other.priority)
        return true;
      if (priority > other.priority)
        return false;
      if (type == cta::common::dataStructures::MountType::Archive && other.type != cta::common::dataStructures::MountType::Archive)
        return false;
      if (other.type == cta::common::dataStructures::MountType::Archive && type != cta::common::dataStructures::MountType::Archive)
        return true;
      if (ratioOfMountQuotaUsed < other.ratioOfMountQuotaUsed)
        return true;
      return false;
    }
  };
  

  /**
   * Information about the existing mounts.
   */
  struct ExistingMount {
    std::string driveName;
    cta::common::dataStructures::MountType type;
    std::string tapePool;
    std::string vid;
    bool currentMount; ///< True if the mount is current (othermise, it's a next mount).
    uint64_t bytesTransferred;
    uint64_t filesTransferred;
    double latestBandwidth;
  };
  
  /**
   * An entry (to be indexed by drive name (string) in a map) for the dedication
   * lists of each drive.
   */
  struct DedicationEntry {
    // TODO.
  };
  
  /**
   * A class containing all the information needed for mount decision
   * and whose creation implicitly takes a global lock on the drive register
   * so that only one mount scheduling happens at a time. Two member functions
   * then allow the 
   */
  class TapeMountDecisionInfo {
  public:
    std::vector<PotentialMount> potentialMounts; /**< All the potential mounts */
    std::vector<ExistingMount> existingOrNextMounts; /**< Existing mounts */
    std::map<std::string, DedicationEntry> dedicationInfo; /**< Drives dedication info */
    bool queueTrimRequired = false; /**< Indicates an empty queue was encountered */
    /**
     * Create a new archive mount. This implicitly releases the global scheduling
     * lock.
     */
    virtual std::unique_ptr<ArchiveMount> createArchiveMount(
      const catalogue::TapeForWriting & tape, const std::string driveName, 
      const std::string & logicalLibrary, const std::string & hostName, 
      time_t startTime) = 0;
    /**
     * Create a new retrieve mount. This implicitly releases the global scheduling
     * lock.
     */
    virtual std::unique_ptr<RetrieveMount> createRetrieveMount(const std::string & vid,
      const std::string & tapePool, const std::string driveName, 
      const std::string& logicalLibrary, const std::string& hostName,
      time_t startTime) = 0;
    /** Destructor: releases the global lock if not already done */
    virtual ~TapeMountDecisionInfo() {};
  };
  
  /**
   * A function dumping the relevant mount information for deciding which
   * tape to mount next. This also starts the mount decision process (and takes
   * a global lock on for scheduling).
   */
  virtual std::unique_ptr<TapeMountDecisionInfo> getMountInfo() = 0;
  
  /**
   * A function running a queue trim. This should be called if the corresponding
   * bit was set in the TapeMountDecisionInfo returned by getMountInfo().
   */
  virtual void trimEmptyQueues(log::LogContext & lc) = 0;
  
  /**
   * A function dumping the relevant mount information for reporting the system
   * status. It is identical to getMountInfo, yet does not take the global lock.
   */
  virtual std::unique_ptr<TapeMountDecisionInfo> getMountInfoNoLock() = 0;
  
  /* === Drive state handling  ============================================== */
  /**
   * Returns the current list of registered drives.
   *
   * @return The current list of registered drives.
   */
  virtual std::list<cta::common::dataStructures::DriveState> getDriveStates() const = 0;
  
  /**
   * Sets the administrative desired state (up/down/force down) for an existing drive.
   * Will throw an excpeiton is the drive does not exist
   * @param drive
   * @param desiredState
   */
  virtual void setDesiredDriveState(const std::string & drive, const cta::common::dataStructures::DesiredDriveState & state) = 0;
  
  /**
   * Sets the drive status in the object store. The drive status will be recorded in all cases,
   * although some historical information is needed to provide an accurate view of the
   * current session state. This allows the system to gracefully handle drive entry
   * deletion at any time (an operator operation).
   * @param driveInfo Fundamental information about the drive.
   * @param mountType Mount type (required).
   * @param status Current drive status (required).
   * @param reportTime Time of report (required).
   * @param mountSessionId (optional, required by some statuses).
   * @param byteTransfered (optional, required by some statuses).
   * @param filesTransfered (optional, required by some statuses).
   * @param latestBandwidth (optional, required by some statuses).
   * @param vid (optional, required by some statuses).
   * @param tapepool (optional, required by some statuses).
   */
  virtual void reportDriveStatus (const common::dataStructures::DriveInfo & driveInfo,
    cta::common::dataStructures::MountType mountType,
    common::dataStructures::DriveStatus status, 
    time_t reportTime,
    uint64_t mountSessionId = std::numeric_limits<uint64_t>::max(),
    uint64_t byteTransfered = std::numeric_limits<uint64_t>::max(),
    uint64_t filesTransfered = std::numeric_limits<uint64_t>::max(),
    double latestBandwidth = std::numeric_limits<double>::max(),
    const std::string & vid = "",
    const std::string & tapepool = "") = 0;
}; // class SchedulerDatabase

} // namespace cta
