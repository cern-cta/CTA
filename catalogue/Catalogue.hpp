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

#include "common/dataStructures/AdminHost.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveMount.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/Dedication.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RepackType.hpp"
#include "common/dataStructures/Requester.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveMount.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/TapeFileLocation.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/TapeMount.hpp"
#include "common/dataStructures/TapePool.hpp"
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/UserGroup.hpp"
#include "common/dataStructures/User.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/VerifyInfo.hpp"
#include "common/dataStructures/VerifyType.hpp"
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
  
  virtual void createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment) = 0;

  virtual void createAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) = 0;
  virtual void deleteAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user) = 0;
  virtual std::list<cta::common::dataStructures::AdminUser> getAdminUsers(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment) = 0;

  virtual void createAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) = 0;
  virtual void deleteAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName) = 0;
  virtual std::list<cta::common::dataStructures::AdminHost> getAdminHosts(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) = 0;

  virtual void createStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies, const std::string &comment) = 0;
  virtual void deleteStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::StorageClass> getStorageClasses(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbCopies) = 0;
  virtual void modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes, const std::string &comment) = 0;
  virtual void deleteTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::TapePool> getTapePools(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t nbPartialTapes) = 0;
  virtual void modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment) = 0;
  virtual void deleteArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb) = 0;
  virtual std::list<cta::common::dataStructures::ArchiveRoute> getArchiveRoutes(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) = 0;
  virtual void modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::LogicalLibrary> getLogicalLibraries(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, 
                          const bool disabledValue, const bool fullValue, const std::string &comment) = 0;
  virtual void deleteTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) = 0;
  virtual std::list<cta::common::dataStructures::Tape> getTapes(const cta::common::dataStructures::SecurityIdentity &requester,
        const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue) = 0;
  virtual void labelTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag) = 0;
  virtual void reclaimTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) = 0;
  virtual void modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) = 0;
  virtual void setTapeBusy(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool busyValue) = 0; // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool fullValue) = 0;
  virtual void setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool disabledValue) = 0;
  virtual void modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &comment) = 0;

  virtual void createUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup, const std::string &comment) = 0;
  virtual void deleteUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group) = 0;
  virtual std::list<cta::common::dataStructures::User> getUsers(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyUserUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup) = 0;
  virtual void modifyUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment) = 0;

  virtual void createUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string &comment) = 0;
  virtual void deleteUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::common::dataStructures::UserGroup> getUserGroups(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyUserGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t archivePriority) = 0;
  virtual void modifyUserGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued) = 0;
  virtual void modifyUserGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued) = 0;
  virtual void modifyUserGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge) = 0;
  virtual void modifyUserGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t retrievePriority) = 0;
  virtual void modifyUserGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued) = 0;
  virtual void modifyUserGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued) = 0;
  virtual void modifyUserGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge) = 0;
  virtual void modifyUserGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t maxDrivesAllowed) = 0;
  virtual void modifyUserGroupComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) = 0;
  virtual void deleteDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename) = 0;
  virtual std::list<cta::common::dataStructures::Dedication> getDedications(const cta::common::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyDedicationReadonly(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly) = 0;
  virtual void modifyDedicationWriteonly(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool writeonly) = 0;
  virtual void modifyDedicationUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup) = 0;
  virtual void modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &tag) = 0;
  virtual void modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &vid) = 0;
  virtual void modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) = 0;
  virtual void modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) = 0;
  virtual void modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &comment) = 0;

  virtual std::list<cta::common::dataStructures::ArchiveFile> getArchiveFiles(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &copynb, const std::string &tapepool, 
   const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) = 0;
  virtual cta::common::dataStructures::ArchiveFileSummary getArchiveFileSummary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &copynb, const std::string &tapepool, 
   const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path) = 0;
  
  virtual void setDriveStatus(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force) = 0;

  /**
   * Returns the next identifier to be used for a new archive file.
   *
   * @return The next identifier to be used for a new archive file.
   */
  virtual uint64_t getNextArchiveFileId() = 0;

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param archiveRequest The identifier of the archive file.
   *
   */
  virtual void fileWrittenToTape(
 const cta::common::dataStructures::ArchiveRequest &archiveRequest,
 const cta::common::dataStructures::TapeFileLocation tapeFileLocation) = 0;

}; // class Catalogue

} // namespace catalogue
} // namespace cta
