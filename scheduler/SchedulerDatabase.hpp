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

namespace cta {

// Forward declarations for opaque references
class AdminHost;
class AdminUser;
class ArchivalJob;
class ArchivalRoute;
class ArchivalFileTransfer;
class ArchiveToDirRequest;
class ArchiveToFileRequest;
class DirIterator;
class LogicalLibrary;
class RetrievalFileTransfer;
class RetrievalJob;
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

  /**
   * Queues the specified request and its associated file transfers.
   *
   * @param rqst The request.
   * @param fileTransfers The file transfers.
   */
  virtual void queue(const ArchiveToDirRequest &rqst,
   const std::list<ArchivalFileTransfer> &fileTransfers) = 0;

  /**
   * Queues the specified request and its associated file transfers.
   *
   * @param rqst The request.
   * @param fileTransfers The file transfers.
   */
  virtual void queue(const ArchiveToFileRequest &rqst,
    const std::list<ArchivalFileTransfer> &fileTransfers) = 0;

  /**
   * Returns all of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   */
  // DO NOT IMPLEMENT YET virtual std::map<TapePool, std::list<ArchivalJob> > getArchivalJobs(
    // DO NOT IMPLEMENT YET const SecurityIdentity &requester) const = 0;

  /**
   * Returns the list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   */
  // DO NOT IMPLEMENT YET virtual std::list<ArchivalJob> getArchivalJobs(
    // DO NOT IMPLEMENT YET const SecurityIdentity &requester,
    // DO NOT IMPLEMENT YET const std::string &tapePoolName) const = 0;

  /**
   * Deletes the specified archival job.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param archiveFile The absolute path of the destination file within the
   * archive namespace.
   */
  virtual void deleteArchivalJob(
    const SecurityIdentity &requester,
    const std::string &archiveFile) = 0;

  /**
   * Queues the specified request and its asscoaited file transfers.
   *
   * @param rqst The request.
   * @param fileTransfers The file transfers.
   */
  virtual void queue(const RetrieveToDirRequest &rqst,
    const std::list<RetrievalFileTransfer> &fileTransfers) = 0;

  /**
   * Queues the specified request and its associated file transfers.
   *
   * @param rqst The request.
   * @param fileTransfers The file transfers.
   */
  virtual void queue(const RetrieveToFileRequest &rqst,
    const std::list<RetrievalFileTransfer> &fileTransfers) = 0;

  /**
   * Returns all of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  // DO NOT IMPLEMENT YET virtual std::map<Tape, std::list<RetrievalJob> > getRetrievalJobs(
    // DO NOT IMPLEMENT YET const SecurityIdentity &requester) const = 0;

  /**
   * Returns the list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param vid The volume identifier of the tape.
   * @return The list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  // DO NOT IMPLEMENT YET virtual std::list<RetrievalJob> getRetrievalJobs(
    // DO NOT IMPLEMENT YET const SecurityIdentity &requester,
    // DO NOT IMPLEMENT YET const std::string &vid) const = 0;
  
  /**
   * Deletes the specified retrieval job.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param dstUrl The URL of the destination file or directory.
   */
  // DO NOT IMPLEMENT YET virtual void deleteRetrievalJob(
    // DO NOT IMPLEMENT YET const SecurityIdentity &requester,
    // DO NOT IMPLEMENT YET const std::string &dstUrl) = 0;

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the user requesting the creation of the
   * administrator.
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
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param user The identity of the administrator.
   */
  virtual void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user) = 0;

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administrators in lexicographical order.
   */
  virtual std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester)
   const = 0;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the user requesting the creation of the
   * administration host.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  virtual void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param hostName The network name of the administration host.
   */
  virtual void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName) = 0;

  /**
   * Returns the current list of administration hosts in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administration hosts in lexicographical order.
   */
  virtual std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester)
   const = 0;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param comment The comment describing the storage class.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies,
    const std::string &comment) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  virtual void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<StorageClass> getStorageClasses(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the creation of the
   * tape pool.
   * @param name The name of the tape pool.
   * @param nbPartialTapes The maximum number of tapes that can be partially
   * full at any moment in time.
   * @param comment The comment describing the tape pool.
   */
  virtual void createTapePool(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint32_t nbPartialTapes,
    const std::string &comment) = 0;

  /**
   * Delete the tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape pool.
   * @param name The name of the tape pool.
   */
  virtual void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tape pools in lexicographical order.
   */
  virtual std::list<TapePool> getTapePools(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates the specified archival route.
   *
   * @param requester The identity of the user requesting the creation of the
   * archival route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param comment The comment describing the archival route.
   */
  virtual void createArchivalRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified archival route.
   *
   * @param requester The identity of the user requesting the deletion of the
   * archival route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  virtual void deleteArchivalRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb) = 0;

  /**
   * Gets the current list of archival routes.
   *
   * @param requester The identity of the user requesting the list.
   */
  virtual std::list<ArchivalRoute> getArchivalRoutes(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a logical library with the specified name.
   *
   * @param requester The identity of the user requesting the creation of the
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  virtual void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment) = 0;

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * logical library.
   * @param name The name of the logical library.
   */
  virtual void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of libraries in lexicographical order.
   */
  virtual std::list<LogicalLibrary> getLogicalLibraries(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a tape.
   *
   * @param requester The identity of the user requesting the creation of the
   * tape.
   * @param vid The volume identifier of the tape.
   * @param logicalLibraryName The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param comment The comment describing the logical library.
   */
  virtual void createTape(
    const SecurityIdentity &requester,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const std::string &comment) = 0;

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param vid The volume identifier of the tape.
   */
  virtual void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid) = 0;

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tapes in the lexicographical order of their
   * volume identifiers.
   */
  virtual std::list<Tape> getTapes(
    const SecurityIdentity &requester) const = 0;

}; // class SchedulerDatabase

} // namespace cta
