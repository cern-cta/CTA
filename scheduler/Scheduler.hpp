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

#include "catalogue/Catalogue.hpp"

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

#include "common/exception/Exception.hpp"
#include "scheduler/TapeMount.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include "common/forwardDeclarations.hpp"
#include "catalogue/MockCatalogue.hpp"

#include <list>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class implementing a tape resource scheduler.
 */
class Scheduler {
  
public:
  
  /**
   * Deprecated Constructor.
   */
  Scheduler(NameServer &ns, SchedulerDatabase &db, RemoteNS &remoteNS);
  
  /**
   * Constructor.
   */
  Scheduler();

  /**
   * Destructor.
   */
  virtual ~Scheduler() throw();

  virtual void queueArchiveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::ArchiveRequest &request);
  virtual void queueRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::RetrieveRequest &request);
  virtual void deleteArchiveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::DeleteArchiveRequest &request);
  virtual void cancelRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::CancelRetrieveRequest &request);
  virtual void updateFileInfoRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::UpdateFileInfoRequest &request);
  virtual void listStorageClassRequest(const cta::common::dataStructures::SecurityIdentity &requestPusher, const cta::common::dataStructures::ListStorageClassRequest &request);

  virtual void createBootstrapAdminAndHostNoAuth(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment);

  virtual void createAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment);
  virtual void deleteAdminUser(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user);
  virtual std::list<cta::common::dataStructures::AdminUser> getAdminUsers(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyAdminUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const cta::common::dataStructures::UserIdentity &user, const std::string &comment);

  virtual void createAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment);
  virtual void deleteAdminHost(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName);
  virtual std::list<cta::common::dataStructures::AdminHost> getAdminHosts(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyAdminHostComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment);

  virtual void createStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment);
  virtual void deleteStorageClass(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::common::dataStructures::StorageClass> getStorageClasses(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyStorageClassNbCopies(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies);
  virtual void modifyStorageClassComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const std::string &comment);
  virtual void deleteTapePool(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::common::dataStructures::TapePool> getTapePools(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyTapePoolNbPartialTapes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes);
  virtual void modifyTapePoolComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName,
   const std::string &comment);
  virtual void deleteArchiveRoute(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb);
  virtual std::list<cta::common::dataStructures::ArchiveRoute> getArchiveRoutes(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyArchiveRouteTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName);
  virtual void modifyArchiveRouteComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment);

  virtual void createLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);
  virtual void deleteLogicalLibrary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::common::dataStructures::LogicalLibrary> getLogicalLibraries(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyLogicalLibraryComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
   const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment);
  virtual void deleteTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid);
  virtual std::list<cta::common::dataStructures::Tape> getTapes(const cta::common::dataStructures::SecurityIdentity &requester,
        const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue);
  virtual void labelTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag);
  virtual void reclaimTape(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid);
  virtual void modifyTapeLogicalLibraryName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName);
  virtual void modifyTapeTapePoolName(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName);
  virtual void modifyTapeCapacityInBytes(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes);
  virtual void setTapeBusy(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool busyValue); // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool fullValue);
  virtual void setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool disabledValue);
  virtual void modifyTapeComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &comment);

  virtual void createUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup,
   const std::string &comment);
  virtual void deleteUser(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group);
  virtual std::list<cta::common::dataStructures::User> getUsers(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyUserUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup);
  virtual void modifyUserComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment);

  virtual void createUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority, const uint64_t minArchiveFilesQueued, 
   const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint32_t retrievePriority, const uint64_t minRetrieveFilesQueued,
   const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment);
  virtual void deleteUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::common::dataStructures::UserGroup> getUserGroups(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyUserGroupArchivePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority);
  virtual void modifyUserGroupArchiveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued);
  virtual void modifyUserGroupArchiveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued);
  virtual void modifyUserGroupArchiveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge);
  virtual void modifyUserGroupRetrievePriority(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t retrievePriority);
  virtual void modifyUserGroupRetrieveMinFilesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued);
  virtual void modifyUserGroupRetrieveMinBytesQueued(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued);
  virtual void modifyUserGroupRetrieveMinRequestAge(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge);
  virtual void modifyUserGroupMaxDrivesAllowed(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed);
  virtual void modifyUserGroupComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
   const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment);
  virtual void deleteDedication(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename);
  virtual std::list<cta::common::dataStructures::Dedication> getDedications(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyDedicationReadonly(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly);
  virtual void modifyDedicationWriteonly(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool writeonly);
  virtual void modifyDedicationUserGroup(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup);
  virtual void modifyDedicationTag(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &tag);
  virtual void modifyDedicationVid(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &vid);
  virtual void modifyDedicationFrom(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp);
  virtual void modifyDedicationUntil(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp);
  virtual void modifyDedicationComment(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &comment);

  virtual void repack(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tag, const cta::common::dataStructures::RepackType);
  virtual void cancelRepack(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid);
  virtual std::list<cta::common::dataStructures::RepackInfo> getRepacks(const cta::common::dataStructures::SecurityIdentity &requester);
  virtual cta::common::dataStructures::RepackInfo getRepack(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid);

  virtual void shrink(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &tapepool); // removes extra tape copies from a specific pool(usually an "_2" pool)

  virtual void verify(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tag, const cta::common::dataStructures::VerifyType);
  virtual void cancelVerify(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid);
  virtual std::list<cta::common::dataStructures::VerifyInfo> getVerifys(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual cta::common::dataStructures::VerifyInfo getVerify(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) const;

  virtual std::list<cta::common::dataStructures::ArchiveFile> getArchiveFiles(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &copynb, const std::string &tapepool, 
   const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path);
  virtual cta::common::dataStructures::ArchiveFileSummary getArchiveFileSummary(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &id, const std::string &copynb, const std::string &tapepool, 
   const std::string &vid, const std::string &owner, const std::string &group, const std::string &storageclass, const std::string &path);

  virtual cta::common::dataStructures::ReadTestResult readTest(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const std::string &vid, const uint64_t firstFSeq, const uint64_t lastFSeq, 
   const bool checkChecksum, const uint32_t retriesPerFile, const std::string &outputDir, const bool redirectToDevNull, const std::string &tag) const;
  virtual cta::common::dataStructures::WriteTestResult writeTest(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const std::string &vid, const uint64_t numberOfFiles, const uint64_t fileSize, 
   const bool randomSize, const bool devZero, const bool devURandom, const std::list<std::string> &inputFiles, const std::string &tag) const;

  virtual void setDriveStatus(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force);

  virtual std::list<cta::common::dataStructures::ArchiveFile> reconcile(const cta::common::dataStructures::SecurityIdentity &requester); // returns the list of files unknown to EOS, to be deleted manually by the admin after proper checks

  virtual std::map<cta::common::dataStructures::TapePool, std::list<cta::common::dataStructures::ArchiveJob> > getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual std::list<cta::common::dataStructures::ArchiveJob> getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &tapePoolName) const;
  virtual std::map<cta::common::dataStructures::Tape, std::list<cta::common::dataStructures::RetrieveJob> > getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &requester) const;
  virtual std::list<cta::common::dataStructures::RetrieveJob> getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &requester, const std::string &vid) const;

  virtual std::list<cta::common::dataStructures::DriveState> getDriveStates(const cta::common::dataStructures::SecurityIdentity &requester) const;

  virtual std::unique_ptr<cta::common::dataStructures::TapeMount> getNextMount(const std::string &logicalLibraryName, const std::string &driveName);
  
  virtual std::unique_ptr<TapeMount> _old_getNextMount(const std::string &logicalLibraryName, const std::string & driveName);

private:

}; // class Scheduler

} // namespace cta
