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

#include "catalogue/Catalogue.hpp"
#include "catalogue/SqliteConn.hpp"

#include <atomic>
#include <memory>
#include <mutex>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * Forward declaration.
 */
class TapeFile;

} // namespace dataStructures
} // namespace catalogue
} // namespace cta

namespace cta {
namespace catalogue {

/**
 * Forward declaration.
 */
class ArchiveFileRow;

/**
 * CTA catalogue to facilitate unit testing.
 */
class SqliteCatalogue: public Catalogue {
public:

  /**
   * Constructor.
   */
  SqliteCatalogue();

  /**
   * Destructor.
   */
  virtual ~SqliteCatalogue();
  
  virtual void createBootstrapAdminAndHostNoAuth(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment);

  virtual void createAdminUser(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &comment);
  virtual void deleteAdminUser(const common::dataStructures::UserIdentity &user);
  virtual std::list<common::dataStructures::AdminUser> getAdminUsers() const;
  virtual void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &comment);

  virtual void createAdminHost(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment);
  virtual void deleteAdminHost(const std::string &hostName);
  virtual std::list<common::dataStructures::AdminHost> getAdminHosts() const;
  virtual void modifyAdminHostComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment);

  virtual void createStorageClass(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies, const std::string &comment);
  virtual void deleteStorageClass(const std::string &name);
  virtual std::list<common::dataStructures::StorageClass> getStorageClasses() const;
  virtual void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies);
  virtual void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);

  virtual void createTapePool(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment);
  virtual void deleteTapePool(const std::string &name);
  virtual std::list<common::dataStructures::TapePool> getTapePools() const;
  virtual void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes);
  virtual void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);
  virtual void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue);

  virtual void createArchiveRoute(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment);
  virtual void deleteArchiveRoute(const std::string &storageClassName, const uint64_t copyNb);
  virtual std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const;
  virtual void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName);
  virtual void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment);

  virtual void createLogicalLibrary(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);
  virtual void deleteLogicalLibrary(const std::string &name);
  virtual std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const;
  virtual void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);

  virtual void createTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                          const std::string &encryptionKey, const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment);
  virtual void deleteTape(const std::string &vid);
  virtual std::list<common::dataStructures::Tape> getTapes(const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue, const std::string &lbpValue);
  virtual void reclaimTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid);
  virtual void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName);
  virtual void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName);
  virtual void modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes);
  virtual void modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey);
  virtual void modifyTapeLabelLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp); // internal function (noCLI)
  virtual void modifyTapeLastWrittenLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp); // internal function (noCLI)
  virtual void modifyTapeLastReadLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp); // internal function (noCLI)
  virtual void setTapeBusy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue); // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue);
  virtual void setTapeDisabled(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue);
  virtual void setTapeLbp(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue); // internal function (noCLI)
  virtual void modifyTapeComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment);

  virtual void createRequester(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &mountPolicy, const std::string &comment);
  virtual void deleteRequester(const common::dataStructures::UserIdentity &user);
  virtual std::list<common::dataStructures::Requester> getRequesters() const;
  virtual void modifyRequesterMountPolicy(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &mountPolicy);
  virtual void modifyRequesterComment(const common::dataStructures::SecurityIdentity &cliIdentity, const common::dataStructures::UserIdentity &user, const std::string &comment);

  virtual void createMountPolicy(
    const common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &name,
    const uint64_t archivePriority,
    const uint64_t minArchiveRequestAge,
    const uint64_t retrievePriority,
    const uint64_t minRetrieveRequestAge,
    const uint64_t maxDrivesAllowed,
    const std::string &comment);

  virtual void deleteMountPolicy(const std::string &name);
  virtual std::list<common::dataStructures::MountPolicy> getMountPolicies() const;
  virtual void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority);
  virtual void modifyMountPolicyArchiveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued);
  virtual void modifyMountPolicyArchiveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archiveMinBytesQueued);
  virtual void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge);
  virtual void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority);
  virtual void modifyMountPolicyRetrieveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued);
  virtual void modifyMountPolicyRetrieveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrieveMinBytesQueued);
  virtual void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge);
  virtual void modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed);
  virtual void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);

  virtual void createDedication(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const common::dataStructures::DedicationType dedicationType,
   const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment);
  virtual void deleteDedication(const std::string &drivename);
  virtual std::list<common::dataStructures::Dedication> getDedications() const;
  virtual void modifyDedicationType(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const common::dataStructures::DedicationType dedicationType);
  virtual void modifyDedicationTag(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag);
  virtual void modifyDedicationVid(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid);
  virtual void modifyDedicationFrom(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp);
  virtual void modifyDedicationUntil(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp);
  virtual void modifyDedicationComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment);

  virtual std::list<common::dataStructures::ArchiveFile> getArchiveFiles(const std::string &id, const std::string &eosid,
   const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path);
  virtual common::dataStructures::ArchiveFileSummary getArchiveFileSummary(const std::string &id, const std::string &eosid,
   const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path);

  /**
   * Returns the archive file with the specified unique identifier.
   *
   * This method assumes that the archive file being requested exists and will
   * therefore throw an exception if it does not.
   *
   * @param id The unique identifier of the archive file.
   * @return The archive file.
   */
  virtual common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id);
  
  virtual void setDriveStatus(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force);

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param event The tape file written event.
   */
  virtual void fileWrittenToTape(const TapeFileWritten &event);

  /**
   * Prepares the catalogue for a new archive file and returns the information
   * required to queue the associated archive request.
   *
   * @param storageClass The storage class of the file to be archived.  This
   * will be used by the Catalogue to determine the destinate tape pool for
   * each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The information required to queue the associated archive request.
   */
  virtual common::dataStructures::ArchiveFileQueueCriteria
    prepareForNewFile(const std::string &storageClass, const common::dataStructures::UserIdentity &user);

  virtual common::dataStructures::TapeCopyToPoolMap getTapeCopyToPoolMap(const std::string &storageClass) const;

  /**
   * Returns the mount policy for the specified end user.
   *
   * @param user The name of the end user.
   * @return The archive mount policy.
   */
  virtual common::dataStructures::MountPolicy getMountPolicyForAUser(const common::dataStructures::UserIdentity &user) const;

  virtual bool isAdmin(const common::dataStructures::SecurityIdentity &cliIdentity) const;

protected:

  /**
   * Mutex to be used to a take a global lock on the in-memory database.
   */
  std::mutex m_mutex;

  /**
   * The connection to the underlying relational database.
   */
  mutable SqliteConn m_conn;

  /**
   * The next unique identifier to be used for an archive file.
   */
  std::atomic<uint64_t> m_nextArchiveFileId;

  /**
   * Creates the database schema.
   */
  void createDbSchema();

  /**
   * Returns true if the specified user name is listed in the ADMIN_USER table.
   *
   * @param userName The name of the user to be search for in the ADMIN_USER
   * table.
   * @return true if the specified user name is listed in the ADMIN_USER table.
   */
  bool userIsAdmin(const std::string &userName) const;

  /**
   * Returns true if the specified host name is listed in the ADMIN_HOST table.
   *
   * @param userName The name of the host to be search for in the ADMIN_HOST
   * table.
   * @return true if the specified host name is listed in the ADMIN_HOST table.
   */
  bool hostIsAdmin(const std::string &userName) const;

  /**
   * Returns the unique identifier of the specified archive file.  Note that
   * this method is required by SqliteCatalogue because SQLite does not support
   * the SQL syntax: "INSERT INTO ... VALUES ... RETURNING ... INTO ...".
   *
   * @param diskInstance The name of teh disk storage instance within which the
   * specified disk file identifier is unique.
   * @param diskFileId The disk identifier of the file.
   * @return The unique identifier of the specified archive file.
   */
  uint64_t getArchiveFileId(const std::string &diskInstance,
    const std::string &diskFileId) const;

  /**
   * Returns the expected number of archive routes for the specified storage
   * class as specified by the call to the createStorageClass() method as
   * opposed to the actual number entered so far using the createArchiveRoute()
   * method.
   *
   * @return The expected number of archive routes.
   */
  uint64_t getExpectedNbArchiveRoutes(const std::string &storageClass) const;

  /**
   * Inserts the specified row into the ArchiveFile table.
   *
   * @param row The row to be inserrted.
   */
  void insertArchiveFile(const ArchiveFileRow &row);

  /**
   * Creates the specified tape file.
   *
   * @param tapeFile The tape file.
   * @param archiveFileId The identifier of the archive file of which the tape
   * file is a copy.
   */
  void createTapeFile(const common::dataStructures::TapeFile &tapeFile, const uint64_t archiveFileId);

  /**
   * Returns the list of all the tape files in the catalogue.
   *
   * @return The list of all the tape files in the catalogue.
   */
  std::list<common::dataStructures::TapeFile> getTapeFiles() const;

  /**
   * Sets the last FSeq of the specified tape to the specified value.
   *
   * @param vid The volume identifier of the tape.
   * @param lastFseq The new value of the last FSeq.
   */
  void setTapeLastFSeq(const std::string &vid, const uint64_t lastFSeq);

  /**
   * Returns the last FSeq of the speficied tape.
   *
   * @param vid The volume identifier of the tape.
   * @return The last FSeq.
   */
  uint64_t getTapeLastFSeq(const std::string &vid) const;

  /**
   * Returns the specified archive file or a NULL pointer if it does not exist.
   *
   * @param archiveFileId The identifier of the archive file.
   * @return The archive file or NULL.
   * an empty list.
   */
  std::unique_ptr<common::dataStructures::ArchiveFile> getArchiveFile(const uint64_t archiveFileId) const;

  /**
   * Throws an exception if the there is a mismatch between the expected and
   * actual common event data.
   *
   * @param expected The expected event data.
   * @param actual The actual event data.
   */
  void throwIfCommonEventDataMismatch(const common::dataStructures::ArchiveFile &expected,
    const TapeFileWritten &actual) const;

  /**
   * Returns the list of tapes that can be written to be a tape drive in the
   * specified logical library.
   *
   * @param logicalLibraryName The name of the logical library.
   */
  virtual std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const;
}; // class SqliteCatalogue

} // namespace catalogue
} // namespace cta
