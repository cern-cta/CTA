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

namespace cta {
namespace catalogue {

/**
 * Dummy CTA catalogue to facilitate unit testing.
 */
class DummyCatalogue: public Catalogue {
public:

  /**
   * Destructor.
   */
  virtual ~DummyCatalogue();

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
  virtual cta::common::dataStructures::ArchiveFileQueueCriteria
    prepareForNewFile(const std::string &storageClass, const std::string &user);

  std::map<uint64_t,std::string> getCopyNbToTapePoolMap(const std::string &storageClass) const;
  
  virtual cta::common::dataStructures::MountPolicy getArchiveMountPolicy(const cta::common::dataStructures::UserIdentity &requester) const;
  virtual cta::common::dataStructures::MountPolicy getRetrieveMountPolicy(const cta::common::dataStructures::UserIdentity &requester) const;
  virtual bool isAdmin(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const;

}; // class DummyCatalogue

} // namespace catalogue
} // namespace cta
