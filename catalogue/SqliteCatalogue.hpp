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

namespace cta {
namespace catalogue {

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
  
  virtual void createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment);

  virtual void createAdminUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment);
  virtual void deleteAdminUser(const cta::common::dataStructures::UserIdentity &user);
  virtual std::list<cta::common::dataStructures::AdminUser> getAdminUsers() const;
  virtual void modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment);

  virtual void createAdminHost(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment);
  virtual void deleteAdminHost(const std::string &hostName);
  virtual std::list<cta::common::dataStructures::AdminHost> getAdminHosts() const;
  virtual void modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment);

  virtual void createStorageClass(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies, const std::string &comment);
  virtual void deleteStorageClass(const std::string &name);
  virtual std::list<cta::common::dataStructures::StorageClass> getStorageClasses() const;
  virtual void modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies);
  virtual void modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);

  virtual void createTapePool(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment);
  virtual void deleteTapePool(const std::string &name);
  virtual std::list<cta::common::dataStructures::TapePool> getTapePools() const;
  virtual void modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes);
  virtual void modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);
  virtual void setTapePoolEncryption(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue);

  virtual void createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment);
  virtual void deleteArchiveRoute(const std::string &storageClassName, const uint64_t copyNb);
  virtual std::list<cta::common::dataStructures::ArchiveRoute> getArchiveRoutes() const;
  virtual void modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName);
  virtual void modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment);

  virtual void createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);
  virtual void deleteLogicalLibrary(const std::string &name);
  virtual std::list<cta::common::dataStructures::LogicalLibrary> getLogicalLibraries() const;
  virtual void modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);

  virtual void createTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                          const std::string &encryptionKey, const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment);
  virtual void deleteTape(const std::string &vid);
  virtual std::list<cta::common::dataStructures::Tape> getTapes(const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue, const std::string &lbpValue);
  virtual void reclaimTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid);
  virtual void modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName);
  virtual void modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName);
  virtual void modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes);
  virtual void modifyTapeEncryptionKey(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey);
  virtual void modifyTapeLabelLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp); // internal function (noCLI)
  virtual void modifyTapeLastWrittenLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp); // internal function (noCLI)
  virtual void modifyTapeLastReadLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp); // internal function (noCLI)
  virtual void setTapeBusy(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue); // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue);
  virtual void setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue);
  virtual void setTapeLbp(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue); // internal function (noCLI)
  virtual void modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment);

  virtual void createUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup, const std::string &comment);
  virtual void deleteUser(const std::string &name, const std::string &group);
  virtual std::list<cta::common::dataStructures::User> getUsers() const;
  virtual void modifyUserMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup);
  virtual void modifyUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &comment);

  virtual void createMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment);
  virtual void deleteMountGroup(const std::string &name);
  virtual std::list<cta::common::dataStructures::MountGroup> getMountGroups() const;
  virtual void modifyMountGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority);
  virtual void modifyMountGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued);
  virtual void modifyMountGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveBytesQueued);
  virtual void modifyMountGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge);
  virtual void modifyMountGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority);
  virtual void modifyMountGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued);
  virtual void modifyMountGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveBytesQueued);
  virtual void modifyMountGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge);
  virtual void modifyMountGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed);
  virtual void modifyMountGroupComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment);

  virtual void createDedication(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType, const std::string &mountGroup,
   const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment);
  virtual void deleteDedication(const std::string &drivename);
  virtual std::list<cta::common::dataStructures::Dedication> getDedications() const;
  virtual void modifyDedicationType(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType);
  virtual void modifyDedicationMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &mountGroup);
  virtual void modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag);
  virtual void modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid);
  virtual void modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp);
  virtual void modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp);
  virtual void modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment);

  /**
   * Creates the specified archive file without any tape copies and returns its
   * unique identifier.
   *
   * @param archiveFile The archive file to be created.  Note that the
   * archiveFileID atrribute of the file shall be ignored.
   * @return The unique identifier of the newly created archive file.  TBD -
   * Eric suggests that this method should return the idenitifer of the archive
   * file and the destination tape pools, in other words the Catalogue should
   * resolve the archive routes of the archive file given the value of its
   * storageClass attribute.
   */
  virtual uint64_t createArchiveFile(const common::dataStructures::ArchiveFile &archiveFile);
  virtual std::list<cta::common::dataStructures::ArchiveFile> getArchiveFiles(const uint64_t id, const std::string &eosid,
   const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path);
  virtual cta::common::dataStructures::ArchiveFileSummary getArchiveFileSummary(const uint64_t id, const std::string &eosid,
   const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path);
  virtual cta::common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id);
  
  virtual void setDriveStatus(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force);

  /**
   * Returns the next identifier to be used for a new archive file.
   *
   * @return The next identifier to be used for a new archive file.
   */
  virtual uint64_t getNextArchiveFileId();

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param archiveRequest The identifier of the archive file.
   *
   */
  virtual void fileWrittenToTape(
    const cta::common::dataStructures::ArchiveRequest &archiveRequest,
    const cta::common::dataStructures::TapeFileLocation &tapeFileLocation);

  virtual std::tuple<uint64_t, cta::common::dataStructures::TapeCopyRoutes,
    cta::common::dataStructures::MountGroup> prepareForNewFile(
    const std::string &storageClass, const std::string &user);
  
  virtual std::map<uint64_t,std::string> getCopyNbToTapePoolMap(const std::string &storageClass) const;
  virtual cta::common::dataStructures::MountPolicy getArchiveMountPolicy(const cta::common::dataStructures::UserIdentity &requester) const;
  virtual cta::common::dataStructures::MountPolicy getRetrieveMountPolicy(const cta::common::dataStructures::UserIdentity &requester) const;
  virtual bool isAdmin(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const;

private:

  /**
   * The connection to the underlying relational database.
   */
  mutable SqliteConn m_conn;

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

}; // class SqliteCatalogue

} // namespace catalogue
} // namespace cta
