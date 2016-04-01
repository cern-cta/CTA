/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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
#include <stdint.h>
#include <string>
#include <map>
#include <tuple>

#include "common/dataStructures/AdminHost.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/Dedication.hpp"
#include "common/dataStructures/DedicationType.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RepackType.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/TapeCopyToPoolMap.hpp"
#include "common/dataStructures/TapeFileLocation.hpp"
#include "common/dataStructures/TapePool.hpp"
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/MountGroup.hpp"
#include "common/dataStructures/User.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/VerifyInfo.hpp"
#include "common/dataStructures/WriteTestResult.hpp"

namespace cta {

namespace catalogue {

/**
 * Abstract class defining the interface to the CTA catalogue responsible for
 * storing crticial information about archive files, tapes and tape files.
 */
class Catalogue {
public:

  /**
 * Destructor.
 */
  virtual ~Catalogue() = 0;
  
  virtual void createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment) = 0;

  virtual void createAdminUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) = 0;
  virtual void deleteAdminUser(const cta::common::dataStructures::UserIdentity &user) = 0;
  virtual std::list<cta::common::dataStructures::AdminUser> getAdminUsers() const = 0;
  virtual void modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) = 0;

  virtual void createAdminHost(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) = 0;
  virtual void deleteAdminHost(const std::string &hostName) = 0;
  virtual std::list<cta::common::dataStructures::AdminHost> getAdminHosts() const = 0;
  virtual void modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) = 0;

  virtual void createStorageClass(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies, const std::string &comment) = 0;
  virtual void deleteStorageClass(const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::StorageClass> getStorageClasses() const = 0;
  virtual void modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies) = 0;
  virtual void modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;

  virtual void createTapePool(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment) = 0;
  virtual void deleteTapePool(const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::TapePool> getTapePools() const = 0;
  virtual void modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes) = 0;
  virtual void modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;
  virtual void setTapePoolEncryption(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue) = 0;

  virtual void createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment) = 0;
  virtual void deleteArchiveRoute(const std::string &storageClassName, const uint64_t copyNb) = 0;
  virtual std::list<cta::common::dataStructures::ArchiveRoute> getArchiveRoutes() const = 0;
  virtual void modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) = 0;
  virtual void modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archiveMinBytesQueued, const uint64_t retrieveMinBytesQueued, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::LogicalLibrary> getLogicalLibraries() const = 0;
  virtual void modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;
  virtual void modifyLogicalLibraryMinArchiveBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, uint64_t const archiveMinBytesQueued) = 0;
  virtual void modifyLogicalLibraryMinRetrieveBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, uint64_t const retrieveMinBytesQueued) = 0;

  virtual void createTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                          const std::string &encryptionKey, const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment) = 0;
  virtual void deleteTape(const std::string &vid) = 0;
  virtual std::list<cta::common::dataStructures::Tape> getTapes(const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue, const std::string &lbpValue) = 0;
  virtual void reclaimTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) = 0;
  virtual void modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes) = 0;
  virtual void modifyTapeEncryptionKey(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey) = 0;
  virtual void modifyTapeLabelLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) = 0; // internal function (noCLI)
  virtual void modifyTapeLastWrittenLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) = 0; // internal function (noCLI)
  virtual void modifyTapeLastReadLog(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) = 0; // internal function (noCLI)
  virtual void setTapeBusy(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue) = 0; // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue) = 0;
  virtual void setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue) = 0;
  virtual void setTapeLbp(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue) = 0; // internal function (noCLI)
  virtual void modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment) = 0;

  virtual void createUser(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup, const std::string &comment) = 0;
  virtual void deleteUser(const std::string &name, const std::string &group) = 0;
  virtual std::list<cta::common::dataStructures::User> getUsers() const = 0;
  virtual void modifyUserMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &mountGroup) = 0;
  virtual void modifyUserComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &group, const std::string &comment) = 0;

   virtual void createMountGroup(
    const cta::common::dataStructures::SecurityIdentity &cliIdentity, 
    const std::string &name, 
    const uint64_t archivePriority, 
    const uint64_t minArchiveRequestAge, 
    const uint64_t retrievePriority, 
    const uint64_t minRetrieveRequestAge, 
    const uint64_t maxDrivesAllowed, 
    const std::string &comment) = 0;

  virtual void deleteMountGroup(const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::MountGroup> getMountGroups() const = 0;
  virtual void modifyMountGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority) = 0;
  virtual void modifyMountGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued) = 0;
  virtual void modifyMountGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archiveMinBytesQueued) = 0;
  virtual void modifyMountGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge) = 0;
  virtual void modifyMountGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority) = 0;
  virtual void modifyMountGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued) = 0;
  virtual void modifyMountGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrieveMinBytesQueued) = 0;
  virtual void modifyMountGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge) = 0;
  virtual void modifyMountGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed) = 0;
  virtual void modifyMountGroupComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;

  virtual void createDedication(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType, const std::string &mountGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) = 0;
  virtual void deleteDedication(const std::string &drivename) = 0;
  virtual std::list<cta::common::dataStructures::Dedication> getDedications() const = 0;
  virtual void modifyDedicationType(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const cta::common::dataStructures::DedicationType dedicationType) = 0;
  virtual void modifyDedicationMountGroup(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &mountGroup) = 0;
  virtual void modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag) = 0;
  virtual void modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid) = 0;
  virtual void modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp) = 0;
  virtual void modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp) = 0;
  virtual void modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment) = 0;

  virtual std::list<cta::common::dataStructures::ArchiveFile> getArchiveFiles(const uint64_t id, const std::string &eosid,
   const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) = 0;
  virtual cta::common::dataStructures::ArchiveFileSummary getArchiveFileSummary(const uint64_t id, const std::string &eosid,
   const std::string &copynb, const std::string &tapepool, const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) = 0;  
  virtual cta::common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) = 0;
  
  virtual void setDriveStatus(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) = 0;

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
    prepareForNewFile(const std::string &storageClass, const std::string &user)
    = 0;

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param archiveRequest The identifier of the archive file.
   *
   */
  virtual void fileWrittenToTape(
    const cta::common::dataStructures::ArchiveRequest &archiveRequest,
    const cta::common::dataStructures::TapeFileLocation &tapeFileLocation) = 0;
  
  virtual cta::common::dataStructures::TapeCopyToPoolMap getTapeCopyToPoolMap(const std::string &storageClass) const = 0;

  /**
   * Returns the archive mount policy for the specified end user.
   *
   * @param user The name of the end user.
   * @return The archive mount policy.
   */
  virtual cta::common::dataStructures::MountPolicy getArchiveMountPolicy(
    const std::string &user) const = 0;

  /**
   * Returns the retrieve mount policy for the specified end user.
   *
   * @param user The name of the end user.
   * @return The retrieve mount policy.
   */
  virtual cta::common::dataStructures::MountPolicy getRetrieveMountPolicy(
    const std::string &user) const = 0;

  virtual bool isAdmin(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const = 0;

}; // class Catalogue

} // namespace catalogue
} // namespace cta
