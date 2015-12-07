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

namespace cta {

// Forward declarations for opaque references
class AdminHost;
class AdminUser;
class ArchiveRoute;
class ArchiveToDirRequest;
class ArchiveToFileRequest;
class ArchiveToTapeCopyRequest;
class CreationLog;
class DirIterator;
class LogicalLibrary;
class RetrieveRequestDump;
class RetrieveToDirRequest;
class RetrieveToFileRequest;
class SecurityIdentity;
class StorageClass;
class Tape;
class TapePool;
class UserIdentity;

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
  class ArchiveToFileRequestCreation {
  public:
    virtual void complete() = 0;
    virtual void cancel() = 0;
    virtual ~ArchiveToFileRequestCreation() {};
  };
  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  virtual void queue(const ArchiveToDirRequest &rqst) = 0;

  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  virtual std::unique_ptr<ArchiveToFileRequestCreation> queue(const ArchiveToFileRequest &rqst) = 0;

  /**
   * Returns all of the queued archive requests.  The returned requests are
   * grouped by tape pool and then sorted by creation time, oldest first.
   *
   * @return The queued requests.
   */
  virtual std::map<TapePool, std::list<ArchiveToTapeCopyRequest> >
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
   * @param archiveFile The absolute path of the destination file within the
   * archive namespace.
   */
  virtual void deleteArchiveRequest(
    const SecurityIdentity &requester,
    const std::string &archiveFile) = 0;
  
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
   * @param archiveFile The absolute path of the destination file within the
   * archive namespace.
   */
  virtual std::unique_ptr<ArchiveToFileRequestCancelation> markArchiveRequestForDeletion(
    const SecurityIdentity &requester,
    const std::string &archiveFile) = 0;

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
    virtual void setDriveStatus(DriveStatus status, time_t completionTime) = 0;
    virtual ~ArchiveMount() {}
    uint32_t nbFilesCurrentlyOnTape;
  };
  
  /**
   * The class to handle the DB-side of a tape job.
   */
  class ArchiveJob {
  public:
    cta::RemotePathAndStatus remoteFile;
    cta::ArchiveFile archiveFile;
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
  virtual void queue(const RetrieveToDirRequest &rqst) = 0;

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
    const SecurityIdentity &requester,
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
    virtual void setDriveStatus(DriveStatus status, time_t completionTime) = 0;
    virtual ~RetrieveMount() {}
    uint32_t nbFilesCurrentlyOnTape;
  };
  
  class RetrieveJob {
    friend class RetrieveMount;
  public:
    std::string remoteFile;
    cta::ArchiveFile archiveFile;
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
    MountCriteria mountCriteria; /**< The mount criteria collection */ 
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

  
  /*============ Admin user/host management  ================================*/
  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the requester.
   * @param user The identity of the administrator.
   * @param comment The comment describing the sministrator.
   */
  virtual void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment) = 0;

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the requester.
   * @param user The identity of the administrator.
   */
  virtual void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user) = 0;

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @return The current list of administrators in lexicographical order.
   */
  virtual std::list<AdminUser> getAdminUsers() const = 0;

  /**
   * Creates the specified administration host.
   *
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  virtual void createAdminHost(
    const std::string &hostName,
    const CreationLog &creationLog) = 0;

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the requester.
   * @param hostName The network name of the administration host.
   */
  virtual void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName) = 0;

  /**
   * Returns the current list of administration hosts in lexicographical order.
   *
   * @return The current list of administration hosts in lexicographical order.
   */
  virtual std::list<AdminHost> getAdminHosts() const = 0;

  /**
   * Throws an exception if the specified user is not an administrator or if the
   * user is not sending a request from an adminsitration host.
   *
   * @param id The identity of the user.
   */
  virtual void assertIsAdminOnAdminHost(const SecurityIdentity &id) const = 0;

  /*============ StorageClass management ====================================*/
  
  /**
   * Creates the specified storage class.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param creationLog The who, where, when an why of this modification.
   */
  virtual void createStorageClass(
    const std::string &name,
    const uint16_t nbCopies,
    const CreationLog &creationLog) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the requester.
   * @param name The name of the storage class.
   */
  virtual void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<StorageClass> getStorageClasses() const = 0;

  /**
   * Returns the storage class with the specified name.
   *
   * @param name The name of the storage class.
   * @return The storage class with the specified name.
   */
  virtual StorageClass getStorageClass(const std::string &name) const = 0;

  /*============ Tape pools management ======================================*/
  /**
   * Creates a tape pool with the specified name.
   *
   * @param name The name of the tape pool.
   * @param nbPartialTapes The maximum number of tapes that can be partially
   * full at any moment in time.
   * @param creationLog The who, where, when an why of this modification.
   */
  virtual void createTapePool(
    const std::string &name,
    const uint32_t nbPartialTapes,
    const CreationLog& creationLog) = 0;
  
  /**
   * Sets the mount criteria for a tape pool.
   * @param tapePool tape pool name
   * @param mountCriteriaByDirection the set of all the mount criteria.
   */

  virtual void setTapePoolMountCriteria(const std::string & tapePool,
    const MountCriteriaByDirection & mountCriteriaByDirection) = 0;
  
  /**
   * Delete the tape pool with the specified name.
   *
   * @param requester The identity of the requester.
   * @param name The name of the tape pool.
   */
  virtual void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @return The current list of tape pools in lexicographical order.
   */
  virtual std::list<TapePool> getTapePools() const = 0;
  
  /*============ Archive Routes management ==================================*/
  
  /**
   * Creates the specified archive route.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param creationLog The who, where, when an why of this modification.
   */
  virtual void createArchiveRoute(
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const CreationLog &creationLog) = 0;

  /**
   * Deletes the specified archive route.
   *
   * @param requester The identity of the requester.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  virtual void deleteArchiveRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb) = 0;

  /**
   * Returns the current list of archive routes.
   *
   * @return The current list of archive routes.
   */
  virtual std::list<ArchiveRoute> getArchiveRoutes() const = 0;

  /**
   * Returns the list of archive routes for the specified storage class.
   *
   * This method throws an exception if the list of archive routes is
   * incomplete.  For example this method will throw an exception if the number
   * of tape copies in the specified storage class is 2 but only one archive
   * route has been created in the scheduler database for the storage class.
   *
   * @param storageClassName The name of the storage class.
   * @return The list of archive routes for the specified storage class.
   */
  virtual std::list<ArchiveRoute> getArchiveRoutes(
    const std::string &storageClassName) const = 0;
  
  /*============ Logical libraries management ==============================*/
  
  /**
   * Creates a logical library with the specified name.
   *
   * @param requester The identity of the requester.
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  virtual void createLogicalLibrary(
    const std::string &name,
    const cta::CreationLog& creationLog) = 0;

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the requester.
   * @param name The name of the logical library.
   */
  virtual void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @return The current list of libraries in lexicographical order.
   */
  virtual std::list<LogicalLibrary> getLogicalLibraries() const = 0;

   /*============ Tape management ===========================================*/
  
  /**
   * Creates a tape.
   *
   * @param vid The volume identifier of the tape.
   * @param logicalLibraryName The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param creationLog The who, where, when an why of this modification.
   */
  virtual void createTape(
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const CreationLog &creationLog) = 0;

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   */
  virtual void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid) = 0;

  /**
   * Returns the tape with the specified volume identifier.
   *
   * @param vid The volume identifier of the tape.
   * @return The tape with the specified volume identifier.
   */
  virtual Tape getTape(const std::string &vid) const = 0;

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @return The current list of tapes in the lexicographical order of their
   * volume identifiers.
   */
  virtual std::list<Tape> getTapes() const = 0;
  
  /* === Drive state handling  ============================================== */
  /**
   * Returns the current list of registered drives.
   *
   * @return The current list of registered drives.
   */
  virtual std::list<cta::DriveState> getDriveStates() const = 0;

}; // class SchedulerDatabase

} // namespace cta
