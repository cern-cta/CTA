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
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveMount.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/Dedication.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DRData.hpp"
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
  
  virtual void createBootstrapAdminAndHostNoAuth(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment) = 0;

  virtual void createAdminUser(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user, const std::string &comment) = 0;
  virtual void deleteAdminUser(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user) = 0;
  virtual std::list<cta::dataStructures::AdminUser> getAdminUsers(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyAdminUserComment(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user, const std::string &comment) = 0;

  virtual void createAdminHost(const cta::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) = 0;
  virtual void deleteAdminHost(const cta::dataStructures::SecurityIdentity &requester, const std::string &hostName) = 0;
  virtual std::list<cta::dataStructures::AdminHost> getAdminHosts(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyAdminHostComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment) = 0;

  virtual void createStorageClass(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment) = 0;
  virtual void deleteStorageClass(const cta::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::dataStructures::StorageClass> getStorageClasses(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyStorageClassNbCopies(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies) = 0;
  virtual void modifyStorageClassComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createTapePool(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const std::string &comment) = 0;
  virtual void deleteTapePool(const cta::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::dataStructures::TapePool> getTapePools(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyTapePoolNbPartialTapes(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes) = 0;
  virtual void modifyTapePoolComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createArchiveRoute(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName, const std::string &comment) = 0;
  virtual void deleteArchiveRoute(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb) = 0;
  virtual std::list<cta::dataStructures::ArchiveRoute> getArchiveRoutes(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyArchiveRouteTapePoolName(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName) = 0;
  virtual void modifyArchiveRouteComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const cta::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::dataStructures::LogicalLibrary> getLogicalLibraries(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyLogicalLibraryComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, 
                          const bool disabledValue, const bool fullValue, const std::string &comment) = 0;
  virtual void deleteTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid) = 0;
  virtual std::list<cta::dataStructures::Tape> getTapes(const cta::dataStructures::SecurityIdentity &requester, const std::map<std::string, std::string> &where) = 0; // "where" is a map resembling an "and-ed" where clause in a SQL query
  virtual void labelTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag) = 0;
  virtual void reclaimTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid) = 0;
  virtual void modifyTapeLogicalLibraryName(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeCapacityInBytes(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) = 0;
  virtual void setTapeBusy(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool busyValue) = 0; // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool fullValue) = 0;
  virtual void setTapeDisabled(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool disabledValue) = 0;
  virtual void modifyTapeComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &comment) = 0;

  virtual void createUser(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup, const std::string &comment) = 0;
  virtual void deleteUser(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group) = 0;
  virtual std::list<cta::dataStructures::User> getUsers(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyUserUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup) = 0;
  virtual void modifyUserComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment) = 0;

  virtual void createUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint32_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment) = 0;
  virtual void deleteUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<cta::dataStructures::UserGroup> getUserGroups(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyUserGroupArchivePriority(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority) = 0;
  virtual void modifyUserGroupArchiveMinFilesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued) = 0;
  virtual void modifyUserGroupArchiveMinBytesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued) = 0;
  virtual void modifyUserGroupArchiveMinRequestAge(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge) = 0;
  virtual void modifyUserGroupRetrievePriority(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t retrievePriority) = 0;
  virtual void modifyUserGroupRetrieveMinFilesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued) = 0;
  virtual void modifyUserGroupRetrieveMinBytesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued) = 0;
  virtual void modifyUserGroupRetrieveMinRequestAge(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge) = 0;
  virtual void modifyUserGroupMaxDrivesAllowed(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed) = 0;
  virtual void modifyUserGroupComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createDedication(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) = 0;
  virtual void deleteDedication(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename) = 0;
  virtual std::list<cta::dataStructures::Dedication> getDedications(const cta::dataStructures::SecurityIdentity &requester) const = 0;
  virtual void modifyDedicationReadonly(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly) = 0;
  virtual void modifyDedicationWriteonly(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool writeonly) = 0;
  virtual void modifyDedicationUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup) = 0;
  virtual void modifyDedicationTag(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &tag) = 0;
  virtual void modifyDedicationVid(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &vid) = 0;
  virtual void modifyDedicationFrom(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) = 0;
  virtual void modifyDedicationUntil(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) = 0;
  virtual void modifyDedicationComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &comment) = 0;

  virtual std::list<cta::dataStructures::ArchiveFile> getArchiveFiles(const cta::dataStructures::SecurityIdentity &requester, const std::map<std::string, std::string> &where) = 0; // "where" is a map resembling an "and-ed" where clause in a SQL query

  virtual void setDriveStatus(const cta::dataStructures::SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force) = 0;

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
 const cta::dataStructures::ArchiveRequest &archiveRequest,
 const cta::dataStructures::TapeFileLocation tapeFileLocation) = 0;

}; // class Catalogue

} // namespace catalogue
} // namespace cta
