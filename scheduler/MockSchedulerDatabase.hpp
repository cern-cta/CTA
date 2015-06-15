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

#include "scheduler/SchedulerDatabase.hpp"

#include <sqlite3.h>

namespace cta {
  
/**
 * A mock implementation of the database of a tape resource scheduler.
 */
class MockSchedulerDatabase: public SchedulerDatabase {
public:

  /**
   * Constructor.
   */
  MockSchedulerDatabase();

  /**
   * Destructor.
   */
  ~MockSchedulerDatabase() throw();

  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  void queue(const ArchiveToDirRequest &rqst);

  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  void queue(const ArchiveToFileRequest &rqst);

  /**
   * Returns all of the queued archive requests.  The returned requests are
   * grouped by tape pool and then sorted by creation time, oldest first.
   *
   * @return The queued requests.
   */
  std::map<TapePool, std::list<ArchiveToTapeCopyRequest> >
    getArchiveRequests() const;

  /**
   * Returns the list of queued archive requests for the specified tape pool.
   * The returned requests are sorted by creation time, oldest first.
   *
   * @param tapePoolName The name of the tape pool.
   * @return The queued requests.
   */
  std::list<ArchiveToTapeCopyRequest> getArchiveRequests(
    const std::string &tapePoolName) const;

  /**
   * Deletes the specified archive request.
   *
   * @param requester The identity of the requester.
   * @param archiveFile The absolute path of the destination file within the
   * archive namespace.
   */
  void deleteArchiveRequest(
    const SecurityIdentity &requester,
    const std::string &archiveFile);

  /**
   * Notifies the scheduler database that the specified file entry has been
   * created in the archive namepace.
   *
   * @param archiveFile The absolute path of the file within the archive
   * namespace.
   */
  void fileEntryCreatedInNS(const std::string &archiveFile);

  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  void queue(const RetrieveToDirRequest &rqst);

  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  void queue(const RetrieveToFileRequest &rqst);

  /**
   * Returns all of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @return All of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  std::map<Tape, std::list<RetrieveFromTapeCopyRequest> > getRetrieveRequests()
    const;

  /**
   * Returns the list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param vid The volume identifier of the tape.
   * @return The list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  std::list<RetrieveFromTapeCopyRequest> getRetrieveRequests(
    const std::string &vid) const;

  /**
   * Deletes the specified retrieval job.
   *
   * @param requester The identity of the requester.
   * @param remoteFile The URL of the destination file.
   */
  void deleteRetrieveRequest(
    const SecurityIdentity &requester,
    const std::string &remoteFile);

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the requester.
   * @param user The identity of the administrator.
   * @param comment The comment describing the sministrator.
   */
  void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment);

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the requester.
   * @param user The identity of the administrator.
   */
  void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user);

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @return The current list of administrators in lexicographical order.
   */
  std::list<AdminUser> getAdminUsers() const;

  /**
   * Throws an exception if the specified user is not an administrator or if the
   * user is not sending a request from an adminsitration host.
   *
   * @param id The identity of the user together with the host they are on.
   */
  void assertIsAdminOnAdminHost(const SecurityIdentity &id) const;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the requester.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment);

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the requester.
   * @param hostName The network name of the administration host.
   */
  void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName);

  /**
   * Returns the current list of administration hosts in lexicographical order.
   *
   * @return The current list of administration hosts in lexicographical order.
   */
  std::list<AdminHost> getAdminHosts() const;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the requester.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param comment The comment describing the storage class.
   */
  void createStorageClass(
    const std::string &name,
    const uint16_t nbCopies,
    const CreationLog &creationLog);

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the requester.
   * @param name The name of the storage class.
   */
  void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Returns the current list of storage classes in lexicographical order.
   *
   * @return The current list of storage classes in lexicographical order.
   */
  std::list<StorageClass> getStorageClasses() const;

  /**
   * Returns the storage class with the specified name.
   *
   * @param name The name of the storage class.
   * @return The storage class with the specified name.
   */
  StorageClass getStorageClass(const std::string &name) const;


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
    const CreationLog& creationLog);

  /**
   * Delete the tape pool with the specified name.
   *
   * @param requester The identity of the requester.
   * @param name The name of the tape pool.
   */
  void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @return The current list of tape pools in lexicographical order.
   */
  std::list<TapePool> getTapePools() const;

  /**
   * Creates the specified archival route.
   *
   * @param requester The identity of the requester.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param comment The comment describing the archival route.
   */
  void createArchivalRoute(
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const CreationLog &creationLog);

  /**
   * Deletes the specified archival route.
   *
   * @param requester The identity of the requester.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void deleteArchivalRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb);

  /**
   * Returns the current list of archival routes.
   *
   * @return The current list of archival routes.
   */
  std::list<ArchivalRoute> getArchivalRoutes() const;

  /**
   * Returns the list of archival routes for the specified storage class.
   *
   * This method throws an exception if the list of archival routes is
   * incomplete.  For example this method will throw an exception if the number
   * of tape copies in the specified storage class is 2 but only one archival
   * route has been created in the scheduler database for the storage class.
   *
   * @param storageClassName The name of the storage class.
   * @return The list of archival routes for the specified storage class.
   */
  std::list<ArchivalRoute> getArchivalRoutes(
    const std::string &storageClassName) const;

  /**
   * Creates a logical library with the specified name.
   *
   * @param requester The identity of the requester.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment);

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the requester.
   * @param name The name of the logical library.
   */
  void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @return The current list of libraries in lexicographical order.
   */
  std::list<LogicalLibrary> getLogicalLibraries() const;

  /**
   * Creates a tape.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   * @param logicalLibraryName The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param comment The comment describing the logical library.
   */
  void createTape(
    const SecurityIdentity &requester,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const std::string &comment);

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   */
  void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid);

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @return The current list of tapes in the lexicographical order of their.
   * volume identifiers.
   */
  std::list<Tape> getTapes() const;

private:

  /**
   * SQLite database handle.
   */
  sqlite3 *m_dbHandle;

  /**
   * Creates the database schema.
   */
  void createSchema();

  /**
   * Throws an exception if the specified user is not an administrator.
   *
   * @param id The identity of the user.
   */
  void assertIsAdmin(const UserIdentity &user) const;

  /**
   * Throws an exception if the specified host is not an administration host.
   *
   * @param host The network name of the host.
   */
  void assertIsAdminHost(const std::string &host) const;

  /**
   * Returns the tape pool with specified name.
   *
   * @param name The name of teh tape pool.
   * @return The tape pool with specified name.
   */
  TapePool getTapePool(const std::string &name) const;

  /**
   * Returns the tape with the specified volume identifier.
   *
   * @param vid The volume identifier of the tape.
   * @return The tape with the specified volume identifier.
   */
  Tape getTape(const std::string &vid) const;

  /**
   * Returns the list of archival routes for the specified storage class.
   *
   * This method does not perfrom any consistency checks.  For example this
   * method will not throw an exception if the list of archival routes is
   * incomplete.
   *
   * @param storageClassName The name of the storage class.
   * @return The list of archival routes for the specified storage class.
   */
  std::list<ArchivalRoute> getArchivalRoutesWithoutChecks(
    const std::string &storageClassName) const;

  /**
   * Throws an exception if the specified tape pool is already a destination in
   * the specified archival routes.
   *
   * @param routes The archival routes.
   * @param tapePoolName The name of the tape pool.
   */
  void assertTapePoolIsNotAlreadyADestination(
    const std::list<ArchivalRoute> &routes, 
    const std::string &tapePoolName) const;

  /**
   * Queues the specified request.
   *
   * @param rqst The request.
   */
  void queue(const ArchiveToTapeCopyRequest &rqst);

}; // class MockSchedulerDatabase

} // namespace cta
