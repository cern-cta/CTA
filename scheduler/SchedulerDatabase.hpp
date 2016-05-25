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

#include <list>
#include <map>
#include <stdint.h>
#include <string>
#include <memory>
#include <vector>
#include <stdexcept>
#include "common/archiveNS/ArchiveFile.hpp"
#include "common/archiveNS/TapeFileLocation.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/remoteFS/RemotePathAndStatus.hpp"
#include "common/MountControl.hpp"
#include "common/DriveState.hpp"
#include "nameserver/NameServerTapeFile.hpp"
#include "scheduler/MountType.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/MountPolicy.hpp"

namespace cta {
// Forward declarations for opaque references.
namespace common {
namespace admin {
  class AdminHost;
  class AdminUser;
} // cta::common::admin
namespace archiveNS {
  class ArchiveFile;
  class ArchiveDirIterator;
  class ArchiveFileStatus;
} // cta::common::archiveNS
namespace archiveRoute {
  class ArchiveRoute;
} // cta::common::archiveRoute
} // cta::common
class ArchiveRequest;
class LogicalLibrary;
class RetrieveRequestDump;
class RetrieveToFileRequest;
class SchedulerDatabase;
class SecurityIdentity;
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
  
  /*============ Archive management: user side ==============================*/
  /*
   * Subclass allowing the tracking and automated cleanup of a 
   * ArchiveToFile requests on the SchdulerDB. Those 2 operations (creation+close
   * or cancel) surround an NS operation. This class can keep references, locks,
   * etc... handy to simplify the implementation of the completion and cancelling
   * (plus the destructor in case the caller fails half way through).
   */ 
  class ArchiveRequestCreation {
  public:
    virtual void complete() = 0;
    virtual void cancel() = 0;
    virtual ~ArchiveRequestCreation() {};
  };
  
  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  virtual std::unique_ptr<ArchiveRequestCreation> queue(const cta::common::dataStructures::ArchiveRequest &request, 
    const cta::common::dataStructures::ArchiveFileQueueCriteria &criteria) = 0;

  /**
   * Returns all of the queued archive requests.  The returned requests are
   * grouped by tape pool and then sorted by creation time, oldest first.
   *
   * @return The queued requests.
   */
  virtual std::map<std::string, std::list<ArchiveToTapeCopyRequest> >
    getArchiveRequests() const = 0;

  /**
   * Returns the list of queued archive requests for the specified tape pool.
   * The returned requests are sorted by creation time, oldest first.
   *
   * @param tapePoolName The name of the tape pool.
   * @return The queued requests.
   */
  virtual std::list<ArchiveToTapeCopyRequest> getArchiveRequests(
    const std::string &tapePoolName) const = 0;

  /**
   * Deletes the specified archive request.
   *
   * @param requester The identity of the requester.
   * @param archiveFile The ID of the destination file within the
   * archive catalogue.
   */
  virtual void deleteArchiveRequest(
    const SecurityIdentity &cliIdentity,
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
  /**
   * Marks the specified archive request for deletion.  The request can only be
   * fully deleted once the corresponding entry has been deleted from the
   * archive namespace.
   *
   * @param requester The identity of the requester.
   * @param fileId Id of the destination file within the archive catalogue.
   */
  virtual std::unique_ptr<ArchiveToFileRequestCancelation> markArchiveRequestForDeletion(
    const SecurityIdentity &cliIdentity,
    uint64_t fileId) = 0;

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
      uint64_t mountId;
    } mountInfo;
    virtual const MountInfo & getMountInfo() = 0;
    virtual std::unique_ptr<ArchiveJob> getNextJob() = 0;
    virtual void complete(time_t completionTime) = 0;
    virtual void setDriveStatus(common::DriveStatus status, time_t completionTime) = 0;
    virtual ~ArchiveMount() {}
    uint32_t nbFilesCurrentlyOnTape;
  };
  
  /**
   * The class to handle the DB-side of a tape job.
   */
  class ArchiveJob {
  public:
    cta::RemotePathAndStatus remoteFile;
    cta::common::archiveNS::ArchiveFile archiveFile;
    cta::NameServerTapeFile nameServerTapeFile;
    virtual void succeed() = 0;
    virtual void fail() = 0;
    virtual void bumpUpTapeFileCount(uint64_t newFileCount) = 0;
    virtual ~ArchiveJob() {}
  };
  
  /*============ Retrieve  management: user side ============================*/

  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  virtual void queue(const RetrieveToFileRequest &rqst_) = 0;

  /**
   * Returns all of the existing retrieve jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @return All of the existing retrieve jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::map<Tape, std::list<RetrieveRequestDump> > getRetrieveRequests()
    const = 0;

  /**
   * Returns the list of retrieve jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param vid The volume identifier of the tape.
   * @return The list of retrieve jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::list<RetrieveRequestDump> getRetrieveRequests(
    const std::string &vid) const = 0;
  
  /**
   * Deletes the specified retrieve job.
   *
   * @param requester The identity of the requester.
   * @param remoteFile The URL of the destination file.
   */
  virtual void deleteRetrieveRequest(
    const SecurityIdentity &cliIdentity,
    const std::string &remoteFile) = 0;
  
  /*============ Retrieve management: tape server side ======================*/

  class RetrieveJob;
  class RetrieveMount {
  public:
    struct MountInfo {
      std::string vid;
      std::string logicalLibrary;
      std::string tapePool;
      std::string drive;
      uint64_t mountId;
    } mountInfo;
    virtual const MountInfo & getMountInfo() = 0;
    virtual std::unique_ptr<RetrieveJob> getNextJob() = 0;
    virtual void complete(time_t completionTime) = 0;
    virtual void setDriveStatus(common::DriveStatus status, time_t completionTime) = 0;
    virtual ~RetrieveMount() {}
    uint32_t nbFilesCurrentlyOnTape;
  };
  
  class RetrieveJob {
    friend class RetrieveMount;
  public:
    std::string remoteFile;
    cta::common::archiveNS::ArchiveFile archiveFile;
    cta::NameServerTapeFile nameServerTapeFile;
    virtual void succeed() = 0;
    virtual void fail() = 0;
    virtual ~RetrieveJob() {}
  };
  
  /*============ Session management ==========================================*/
  /**
   * A structure describing a potential mount with all the information allowing
   * comparison between mounts.
   */
  struct PotentialMount {
    cta::MountType::Enum type; /**< Is this an archive or retireve? */
    std::string vid; /**< The tape VID (for a retieve) */
    std::string tapePool; /**< The name of the tape pool for both archive and retrieve */
    uint64_t priority; /**< The priority for the mount */
    uint64_t filesQueued; /**< The number of files queued for this queue */
    uint64_t bytesQueued; /**< The amount of data currently queued */
    time_t oldestJobStartTime; /**< Creation time of oldest request */
    common::dataStructures::MountPolicy mountPolicy; /**< The mount policy */ 
    std::string logicalLibrary; /**< The logical library (for a retrieve) */
    double ratioOfMountQuotaUsed; /**< The [ 0.0, 1.0 [ ratio of existing mounts/quota (for faire share of mounts)*/
    
    bool operator < (const PotentialMount &other) const {
      if (priority < other.priority)
        return true;
      if (priority > other.priority)
        return false;
      if (type == cta::MountType::Enum::ARCHIVE && other.type != cta::MountType::Enum::ARCHIVE)
        return false;
      if (other.type == cta::MountType::Enum::ARCHIVE && type != cta::MountType::Enum::ARCHIVE)
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
    cta::MountType::Enum type;
    std::string tapePool;
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
    std::vector<ExistingMount> existingMounts; /**< Existing mounts */
    std::map<std::string, DedicationEntry> dedicationInfo; /**< Drives dedication info */
    /**
     * Create a new archive mount. This implicitly releases the global scheduling
     * lock.
     */
    virtual std::unique_ptr<ArchiveMount> createArchiveMount(const std::string & vid,
      const std::string & tapePool, const std::string driveName, 
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
  
  /* === Drive state handling  ============================================== */
  /**
   * Returns the current list of registered drives.
   *
   * @return The current list of registered drives.
   */
  virtual std::list<cta::common::DriveState> getDriveStates() const = 0;

}; // class SchedulerDatabase

} // namespace cta
