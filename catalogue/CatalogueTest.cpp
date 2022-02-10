/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/CatalogueTest.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/Constants.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/exception/UserErrorWithCacheInfo.hpp"
#include "common/make_unique.hpp"
#include "common/SourcedParameter.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"
#include "InsertFileRecycleLog.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"
#include "RdbmsCatalogue.hpp"

#include <algorithm>
#include <gtest/gtest.h>
#include <iomanip>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>

namespace unitTests {

const uint32_t PUBLIC_DISK_USER = 9751;
const uint32_t PUBLIC_DISK_GROUP = 9752;
const uint32_t DISK_FILE_OWNER_UID = 9753;
const uint32_t DISK_FILE_GID = 9754;
const uint32_t NON_EXISTENT_DISK_FILE_OWNER_UID = 9755;
const uint32_t NON_EXISTENT_DISK_FILE_GID = 9756;

namespace {
  cta::common::dataStructures::SecurityIdentity getLocalAdmin() {
    using namespace cta;

    common::dataStructures::SecurityIdentity localAdmin;
    localAdmin.username = "local_admin_user";
    localAdmin.host = "local_admin_host";

    return localAdmin;
  }

  cta::common::dataStructures::SecurityIdentity getAdmin() {
    using namespace cta;

    common::dataStructures::SecurityIdentity admin;
    admin.username = "admin_user_name";
    admin.host = "admin_host";

    return admin;
  }

  cta::common::dataStructures::VirtualOrganization getVo() {
    using namespace cta;

    common::dataStructures::VirtualOrganization vo;
    vo.name = "vo";
    vo.comment = "Creation of virtual organization vo";
    vo.readMaxDrives = 1;
    vo.writeMaxDrives = 1;
    vo.maxFileSize = 0;
    return vo;
  }

  cta::common::dataStructures::VirtualOrganization getAnotherVo() {
    using namespace cta;

    common::dataStructures::VirtualOrganization vo;
    vo.name = "anotherVo";
    vo.comment = "Creation of another virtual organization vo";
    vo.readMaxDrives = 1;
    vo.writeMaxDrives = 1;
    vo.maxFileSize = 0;
    return vo;
  }

  cta::common::dataStructures::StorageClass getStorageClass() {
    using namespace cta;

    common::dataStructures::StorageClass storageClass;
    storageClass.name = "storage_class_single_copy";
    storageClass.nbCopies = 1;
    storageClass.vo.name = getVo().name;
    storageClass.comment = "Creation of storage class with 1 copy on tape";
    return storageClass;
  }

  cta::common::dataStructures::StorageClass getAnotherStorageClass() {
    using namespace cta;

    common::dataStructures::StorageClass storageClass;
    storageClass.name = "another_storage_class";
    storageClass.nbCopies = 1;
    storageClass.vo.name = getVo().name;
    storageClass.comment = "Creation of another storage class";
    return storageClass;
  }

  cta::common::dataStructures::StorageClass getStorageClassDualCopy() {
    using namespace cta;

    common::dataStructures::StorageClass storageClass;
    storageClass.name = "storage_class_dual_copy";
    storageClass.nbCopies = 2;
    storageClass.vo.name = getVo().name;
    storageClass.comment = "Creation of storage class with 2 copies on tape";
    return storageClass;
  }

  cta::common::dataStructures::StorageClass getStorageClassTripleCopy() {
    using namespace cta;

    common::dataStructures::StorageClass storageClass;
    storageClass.name = "storage_class_triple_copy";
    storageClass.nbCopies = 3;
    storageClass.vo.name = getVo().name;
    storageClass.comment = "Creation of storage class with 3 copies on tape";
    return storageClass;
  }

  cta::catalogue::MediaType getMediaType() {
    using namespace cta;

    catalogue::MediaType mediaType;
    mediaType.name = "media_type";
    mediaType.capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
    mediaType.cartridge = "cartridge";
    mediaType.comment = "comment";
    mediaType.maxLPos = 100;
    mediaType.minLPos = 1;
    mediaType.nbWraps = 500;
    mediaType.primaryDensityCode = 50;
    mediaType.secondaryDensityCode = 50;

    return mediaType;
  }

  cta::catalogue::CreateTapeAttributes getTape1() {
    using namespace cta;

    catalogue::CreateTapeAttributes tape;
    tape.vid = "VIDONE";
    tape.mediaType = getMediaType().name;
    tape.vendor = "vendor";
    tape.logicalLibraryName = "logical_library";
    tape.tapePoolName = "tape_pool";
    tape.full = false;
    tape.state = common::dataStructures::Tape::ACTIVE;
    tape.comment = "Creation of tape one";

    return tape;
  }

  cta::catalogue::CreateTapeAttributes getTape2() {
    // Tape 2 is an exact copy of tape 1 except for its VID and comment
    auto tape = getTape1();
    tape.vid = "VIDTWO";
    tape.comment = "Creation of tape two";
    return tape;
  }

  cta::catalogue::CreateTapeAttributes getTape3() {
    // Tape 3 is an exact copy of tape 1 except for its VID and comment
    auto tape = getTape1();
    tape.vid = "VIDTHREE";
    tape.comment = "Creation of tape three";
    return tape;
  }

  cta::catalogue::CreateMountPolicyAttributes getMountPolicy1() {
    using namespace cta;

    catalogue::CreateMountPolicyAttributes mountPolicy;
    mountPolicy.name = "mount_policy";
    mountPolicy.archivePriority = 1;
    mountPolicy.minArchiveRequestAge = 2;
    mountPolicy.retrievePriority = 3;
    mountPolicy.minRetrieveRequestAge = 4;
    mountPolicy.comment = "Create mount policy";
    return mountPolicy;
  }

  cta::catalogue::CreateMountPolicyAttributes getMountPolicy2() {
    // Higher priority mount policy
    using namespace cta;

    catalogue::CreateMountPolicyAttributes mountPolicy;
    mountPolicy.name = "mount_policy_2";
    mountPolicy.archivePriority = 2;
    mountPolicy.minArchiveRequestAge = 1;
    mountPolicy.retrievePriority = 4;
    mountPolicy.minRetrieveRequestAge = 3;
    mountPolicy.comment = "Create mount policy";
    return mountPolicy;
  }


  cta::common::dataStructures::TapeDrive getTapeDriveWithMandatoryElements(const std::string &driveName) {
    cta::common::dataStructures::TapeDrive tapeDrive;
    tapeDrive.driveName = driveName;
    tapeDrive.host = "admin_host";
    tapeDrive.logicalLibrary = "VLSTK10";
    tapeDrive.mountType = cta::common::dataStructures::MountType::NoMount;
    tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
    tapeDrive.desiredUp = false;
    tapeDrive.desiredForceDown = false;
    tapeDrive.diskSystemName = "dummyDiskSystemName";
    tapeDrive.reservedBytes = 694498291384;
    return tapeDrive;
  }

  cta::common::dataStructures::TapeDrive getTapeDriveWithAllElements(const std::string &driveName) {
    cta::common::dataStructures::TapeDrive tapeDrive;
    tapeDrive.driveName = driveName;
    tapeDrive.host = "admin_host";
    tapeDrive.logicalLibrary = "VLSTK10";
    tapeDrive.mountType = cta::common::dataStructures::MountType::NoMount;
    tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
    tapeDrive.desiredUp = false;
    tapeDrive.desiredForceDown = false;
    tapeDrive.diskSystemName = "dummyDiskSystemName";
    tapeDrive.reservedBytes = 694498291384;

    tapeDrive.sessionStartTime = 1001;
    tapeDrive.mountStartTime = 1002;
    tapeDrive.transferStartTime = 1003;
    tapeDrive.unloadStartTime = 1004;
    tapeDrive.unmountStartTime = 1005;
    tapeDrive.drainingStartTime = 1006;
    tapeDrive.downOrUpStartTime = 1007;
    tapeDrive.probeStartTime = 1008;
    tapeDrive.cleanupStartTime = 1009;
    tapeDrive.startStartTime = 1010;
    tapeDrive.shutdownTime = 1011;

    tapeDrive.reasonUpDown = "Random Reason";

    tapeDrive.currentVid = "VIDONE";
    tapeDrive.ctaVersion = "v1.0.0";
    tapeDrive.currentPriority = 3;
    tapeDrive.currentActivity = "Activity1";
    tapeDrive.currentTapePool = "tape_pool_0";
    tapeDrive.nextMountType = cta::common::dataStructures::MountType::Retrieve;
    tapeDrive.nextVid = "VIDTWO";
    tapeDrive.nextTapePool = "tape_pool_1";
    tapeDrive.nextPriority = 1;
    tapeDrive.nextActivity = "Activity2";

    tapeDrive.devFileName = "fileName";
    tapeDrive.rawLibrarySlot = "librarySlot1";

    tapeDrive.currentVo = "VO_ONE";
    tapeDrive.nextVo = "VO_TWO";

    tapeDrive.userComment = "Random comment";
    tapeDrive.creationLog = cta::common::dataStructures::EntryLog("user_name_1", "host_1", 100002);
    tapeDrive.lastModificationLog = cta::common::dataStructures::EntryLog("user_name_2", "host_2", 10032131);

    return tapeDrive;
  }
}  // namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta_catalogue_CatalogueTest::cta_catalogue_CatalogueTest():
        m_dummyLog("dummy", "dummy"),
        m_localAdmin(getLocalAdmin()),
        m_admin(getAdmin()),
        m_vo(getVo()),
        m_anotherVo(getAnotherVo()),
        m_storageClassSingleCopy(getStorageClass()),
        m_anotherStorageClass(getAnotherStorageClass()),
        m_storageClassDualCopy(getStorageClassDualCopy()),
        m_storageClassTripleCopy(getStorageClassTripleCopy()),
        m_mediaType(getMediaType()),
        m_tape1(getTape1()),
        m_tape2(getTape2()),
        m_tape3(getTape3()) {
}

//------------------------------------------------------------------------------
// Setup
//------------------------------------------------------------------------------
void cta_catalogue_CatalogueTest::SetUp() {
  using namespace cta;
  using namespace cta::catalogue;
  log::LogContext dummyLc(m_dummyLog);
  try {
    CatalogueFactory *const *const catalogueFactoryPtrPtr = GetParam();

    if(nullptr == catalogueFactoryPtrPtr) {
      throw exception::Exception("Global pointer to the catalogue factory pointer for unit-tests in null");
    }

    if(nullptr == (*catalogueFactoryPtrPtr)) {
      throw exception::Exception("Global pointer to the catalogue factoryfor unit-tests in null");
    }

    m_catalogue = (*catalogueFactoryPtrPtr)->create();

    {
      const std::list<common::dataStructures::AdminUser> adminUsers = m_catalogue->getAdminUsers();
      for(auto &adminUser: adminUsers) {
        m_catalogue->deleteAdminUser(adminUser.name);
      }
    }
    {
      const std::list<common::dataStructures::ArchiveRoute> archiveRoutes = m_catalogue->getArchiveRoutes();
      for(auto &archiveRoute: archiveRoutes) {
        m_catalogue->deleteArchiveRoute(archiveRoute.storageClassName,
          archiveRoute.copyNb);
      }
    }
    {
      const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
      for(auto &rule: rules) {
        m_catalogue->deleteRequesterActivityMountRule(rule.diskInstance, rule.name, rule.activityRegex);
      }
    }
    {
      const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
      for(auto &rule: rules) {
        m_catalogue->deleteRequesterMountRule(rule.diskInstance, rule.name);
      }
    }
    {
      const std::list<common::dataStructures::RequesterGroupMountRule> rules =
        m_catalogue->getRequesterGroupMountRules();
      for(auto &rule: rules) {
        m_catalogue->deleteRequesterGroupMountRule(rule.diskInstance, rule.name);
      }
    }
    {
      // The iterator returned from m_catalogue->getArchiveFilesItor() will lock
      // an SQLite file database, so copy all of its results into a list in
      // order to release the lock before moving on to deleting database rows
      auto itor = m_catalogue->getArchiveFilesItor();
      std::list<common::dataStructures::ArchiveFile> archiveFiles;
      while(itor.hasMore()) {
        archiveFiles.push_back(itor.next());
      }

      for(const auto &archiveFile: archiveFiles) {
        m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(archiveFile.diskInstance, archiveFile.archiveFileID, dummyLc);
      }
    }

    {
      //Delete all the entries from the recycle log table
      auto itor = m_catalogue->getFileRecycleLogItor();
      std::list<common::dataStructures::FileRecycleLog> filesRecycleLog;
      while(itor.hasMore()){
        m_catalogue->deleteFilesFromRecycleLog(itor.next().vid,dummyLc);
      }
    }
    {
      const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
      for(auto &tape: tapes) {
        m_catalogue->deleteTape(tape.vid);
      }
    }
    {
      const auto mediaTypes = m_catalogue->getMediaTypes();
      for(auto &mediaType: mediaTypes) {
        m_catalogue->deleteMediaType(mediaType.name);
      }
    }
    {
      const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();
      for(auto &storageClass: storageClasses) {
        m_catalogue->deleteStorageClass(storageClass.name);
      }
    }
    {
      const std::list<TapePool> tapePools = m_catalogue->getTapePools();
      for(auto &tapePool: tapePools) {
        m_catalogue->deleteTapePool(tapePool.name);
      }
    }
    {
      const std::list<common::dataStructures::LogicalLibrary> logicalLibraries = m_catalogue->getLogicalLibraries();
      for(auto &logicalLibrary: logicalLibraries) {
        m_catalogue->deleteLogicalLibrary(logicalLibrary.name);
      }
    }
    {
      const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
      for(auto &mountPolicy: mountPolicies) {
        m_catalogue->deleteMountPolicy(mountPolicy.name);
      }
    }
    {
      const auto diskSystems = m_catalogue->getAllDiskSystems();
      for(auto &ds: diskSystems) {
        m_catalogue->deleteDiskSystem(ds.name);
      }
    }
    {
      const auto diskInstanceSpaces = m_catalogue->getAllDiskInstanceSpaces();
      for(auto &dis: diskInstanceSpaces) {
        m_catalogue->deleteDiskInstanceSpace(dis.name, dis.diskInstance);
      }
    }
    {
      const auto diskInstances = m_catalogue->getAllDiskInstances();
      for(auto &di: diskInstances) {
        m_catalogue->deleteDiskInstance(di.name);
      }
    }
    {
      const auto virtualOrganizations = m_catalogue->getVirtualOrganizations();
      for(auto &vo: virtualOrganizations) {
        m_catalogue->deleteVirtualOrganization(vo.name);
      }
    }
    {
      const auto tapeDriveNames = m_catalogue->getTapeDriveNames();
      for (const auto& name : tapeDriveNames) {
        m_catalogue->deleteTapeDrive(name);
      }
    }
    {
      const auto configurationTapeNamesAndKeys = m_catalogue->getTapeDriveConfigNamesAndKeys();
      for (const auto& nameAndKey : configurationTapeNamesAndKeys) {
        m_catalogue->deleteTapeDriveConfig(nameAndKey.first, nameAndKey.second);
      }
    }

    if(!m_catalogue->getAdminUsers().empty()) {
      throw exception::Exception("Found one of more admin users after emptying the database");
    }

    if(m_catalogue->getArchiveFilesItor().hasMore()) {
      throw exception::Exception("Found one of more archive files after emptying the database");
    }

    if(!m_catalogue->getArchiveRoutes().empty()) {
      throw exception::Exception("Found one of more archive routes after emptying the database");
    }

    if(m_catalogue->getFileRecycleLogItor().hasMore()){
      throw exception::Exception("Found one or more files in the file recycle log after emptying the database");
    }

    if(!m_catalogue->getAllDiskSystems().empty()) {
      throw exception::Exception("Found one of more disk systems after emptying the database");
    }

    if (!m_catalogue->getAllDiskInstances().empty()) {
      throw exception::Exception("Found one of more disk instances after emptying the database");
    }

    if(!m_catalogue->getLogicalLibraries().empty()) {
      throw exception::Exception("Found one of more logical libraries after emptying the database");
    }

    if(!m_catalogue->getMediaTypes().empty()) {
      throw exception::Exception("Found one of more media types after emptying the database");
    }

    if(!m_catalogue->getMountPolicies().empty()) {
      throw exception::Exception("Found one of more mount policies after emptying the database");
    }

    if(!m_catalogue->getRequesterGroupMountRules().empty()) {
      throw exception::Exception("Found one of more requester group mount rules after emptying the database");
    }

    if(!m_catalogue->getRequesterMountRules().empty()) {
      throw exception::Exception("Found one of more requester mount rules after emptying the database");
    }

    if(!m_catalogue->getRequesterActivityMountRules().empty()) {
      throw exception::Exception("Found one of more requester activity mount rules after emptying the database");
    }

    if(!m_catalogue->getStorageClasses().empty()) {
      throw exception::Exception("Found one of more storage classes after emptying the database");
    }

    if(!m_catalogue->getTapes().empty()) {
      throw exception::Exception("Found one of more tapes after emptying the database");
    }

    if(!m_catalogue->getTapePools().empty()) {
      throw exception::Exception("Found one of more tape pools after emptying the database");
    }

    if(!m_catalogue->getVirtualOrganizations().empty()) {
      throw exception::Exception("Found one of more virtual organizations after emptying the database");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// TearDown
//------------------------------------------------------------------------------
void cta_catalogue_CatalogueTest::TearDown() {
  m_catalogue.reset();
}

//------------------------------------------------------------------------------
// LogicaLibraryListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::common::dataStructures::LogicalLibrary> cta_catalogue_CatalogueTest::logicalLibraryListToMap(
  const std::list<cta::common::dataStructures::LogicalLibrary> &listOfLibs) {
  using namespace cta;

  try {
    std::map<std::string, cta::common::dataStructures::LogicalLibrary> nameToLib;

    for (auto &lib: listOfLibs) {
      if(nameToLib.end() != nameToLib.find(lib.name)) {
        throw exception::Exception(std::string("Duplicate logical library: value=") + lib.name);
      }
      nameToLib[lib.name] = lib;
    }

    return nameToLib;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// tapeListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::common::dataStructures::Tape> cta_catalogue_CatalogueTest::tapeListToMap(
  const std::list<cta::common::dataStructures::Tape> &listOfTapes) {
  using namespace cta;

  try {
    std::map<std::string, cta::common::dataStructures::Tape> vidToTape;

    for (auto &tape: listOfTapes) {
      if(vidToTape.end() != vidToTape.find(tape.vid)) {
        throw exception::Exception(std::string("Duplicate VID: value=") + tape.vid);
      }
      vidToTape[tape.vid] = tape;
    }

    return vidToTape;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// archiveFileItorToMap
//------------------------------------------------------------------------------
std::map<uint64_t, cta::common::dataStructures::ArchiveFile> cta_catalogue_CatalogueTest::archiveFileItorToMap(
  cta::catalogue::Catalogue::ArchiveFileItor &itor) {
  using namespace cta;

  try {
    std::map<uint64_t, common::dataStructures::ArchiveFile> m;
    while(itor.hasMore()) {
      const auto archiveFile = itor.next();
      if(m.end() != m.find(archiveFile.archiveFileID)) {
        exception::Exception ex;
        ex.getMessage() << "Archive file with ID " << archiveFile.archiveFileID << " is a duplicate";
        throw ex;
      }
      m[archiveFile.archiveFileID] = archiveFile;
    }
    return m;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// archiveFileListToMap
//------------------------------------------------------------------------------
std::map<uint64_t, cta::common::dataStructures::ArchiveFile> cta_catalogue_CatalogueTest::archiveFileListToMap(
  const std::list<cta::common::dataStructures::ArchiveFile> &listOfArchiveFiles) {
  using namespace cta;

  try {
    std::map<uint64_t, common::dataStructures::ArchiveFile> archiveIdToArchiveFile;

    for (auto &archiveFile: listOfArchiveFiles) {
      if(archiveIdToArchiveFile.end() != archiveIdToArchiveFile.find(archiveFile.archiveFileID)) {
        throw exception::Exception(std::string("Duplicate archive file ID: value=") + std::to_string(archiveFile.archiveFileID));
      }
      archiveIdToArchiveFile[archiveFile.archiveFileID] = archiveFile;
    }

    return archiveIdToArchiveFile;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// adminUserListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::common::dataStructures::AdminUser> cta_catalogue_CatalogueTest::adminUserListToMap(
  const std::list<cta::common::dataStructures::AdminUser> &listOfAdminUsers) {
  using namespace cta;

  try {
    std::map<std::string, common::dataStructures::AdminUser> m;

    for(auto &adminUser: listOfAdminUsers) {
      if(m.end() != m.find(adminUser.name)) {
        exception::Exception ex;
        ex.getMessage() << "Admin user " << adminUser.name << " is a duplicate";
        throw ex;
      }
      m[adminUser.name] = adminUser;
    }
    return m;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// mediaTypeWithLogsListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::catalogue::MediaTypeWithLogs> cta_catalogue_CatalogueTest::mediaTypeWithLogsListToMap(
  const std::list<cta::catalogue::MediaTypeWithLogs> &listOfMediaTypes) {
  using namespace cta;

  try {
    std::map<std::string, cta::catalogue::MediaTypeWithLogs> m;

    for(auto &mediaType: listOfMediaTypes) {
      if(m.end() != m.find(mediaType.name)) {
        exception::Exception ex;
        ex.getMessage() << "Media type " << mediaType.name << " is a duplicate";
        throw ex;
      }
      m[mediaType.name] = mediaType;
    }
    return m;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// tapePoolListToMap
//------------------------------------------------------------------------------
std::map<std::string, cta::catalogue::TapePool> cta_catalogue_CatalogueTest::tapePoolListToMap(
  const std::list<cta::catalogue::TapePool> &listOfTapePools) {
  using namespace cta;

  try {
    std::map<std::string, cta::catalogue::TapePool> m;

    for(auto &tapePool: listOfTapePools) {
      if(m.end() != m.find(tapePool.name)) {
        exception::Exception ex;
        ex.getMessage() << "Tape pool " << tapePool.name << " is a duplicate";
        throw ex;
      }
      m[tapePool.name] = tapePool;
    }

    return m;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

TEST_P(cta_catalogue_CatalogueTest, createAdminUser) {
  using namespace cta;

  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createAdminUser_same_twice) {
  using namespace cta;

  m_catalogue->createAdminUser(m_localAdmin, m_admin.username, "comment 1");

  ASSERT_THROW(m_catalogue->createAdminUser(m_localAdmin, m_admin.username, "comment 2"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminUser) {
  using namespace cta;

  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  m_catalogue->deleteAdminUser(m_admin.username);

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());
}

TEST_P(cta_catalogue_CatalogueTest, createAdminUser_emptyStringUsername) {
  using namespace cta;

  const std::string adminUsername = "";
  const std::string createAdminUserComment = "Create admin user";
  ASSERT_THROW(m_catalogue->createAdminUser(m_localAdmin, adminUsername, createAdminUserComment),
    catalogue::UserSpecifiedAnEmptyStringUsername);
}

TEST_P(cta_catalogue_CatalogueTest, createAdminUser_emptyStringComment) {
  using namespace cta;

  const std::string createAdminUserComment = "";
  ASSERT_THROW(m_catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminUser_non_existent) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->deleteAdminUser("non_existent_admin_user"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment) {
  using namespace cta;

  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyAdminUserComment(m_localAdmin, m_admin.username, modifiedComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(modifiedComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment_emptyStringUsername) {
  using namespace cta;

  const std::string adminUsername = "";
  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->modifyAdminUserComment(m_localAdmin, adminUsername, modifiedComment),
    catalogue::UserSpecifiedAnEmptyStringUsername);
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment_emptyStringComment) {
  using namespace cta;

  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  const std::string modifiedComment = "";
  ASSERT_THROW(m_catalogue->modifyAdminUserComment(m_localAdmin, m_admin.username, modifiedComment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment_nonExtistentAdminUser) {
  using namespace cta;

  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->modifyAdminUserComment(m_localAdmin, m_admin.username, modifiedComment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, isAdmin_false) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->isAdmin(m_admin));
}

TEST_P(cta_catalogue_CatalogueTest, isAdmin_true) {
  using namespace cta;

  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = m_catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  ASSERT_TRUE(m_catalogue->isAdmin(m_admin));
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
  ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);
  ASSERT_EQ(m_storageClassSingleCopy.vo.name, storageClasses.front().vo.name);

  const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_same_twice) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_emptyStringStorageClassName) {
  using namespace cta;

  auto storageClass = m_storageClassSingleCopy;
  storageClass.name = "";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass), catalogue::UserSpecifiedAnEmptyStringStorageClassName);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_emptyStringComment) {
  using namespace cta;

  auto storageClass = m_storageClassSingleCopy;
  storageClass.comment = "";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass), catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_emptyStringVo) {
  using namespace cta;

  auto storageClass = m_storageClassSingleCopy;
  storageClass.vo.name = "";
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass), catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_nonExistingVo) {
  using namespace cta;

  auto storageClass = m_storageClassSingleCopy;
  storageClass.vo.name = "NonExistingVO";
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteStorageClass) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
  ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

  const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name);
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteStorageClass_non_existent) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->deleteStorageClass("non_existent_storage_class"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassNbCopies) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedNbCopies = 5;
  m_catalogue->modifyStorageClassNbCopies(m_admin, m_storageClassSingleCopy.name, modifiedNbCopies);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(modifiedNbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassNbCopies_nonExistentStorageClass) {
  using namespace cta;

  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 5;
  ASSERT_THROW(m_catalogue->modifyStorageClassNbCopies(m_admin, storageClassName, nbCopies), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassComment) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyStorageClassComment(m_admin, m_storageClassSingleCopy.name, modifiedComment);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(modifiedComment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassComment_nonExistentStorageClass) {
  using namespace cta;

  const std::string storageClassName = "storage_class";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyStorageClassComment(m_admin, storageClassName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassName) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newStorageClassName = "new_storage_class_name";
  m_catalogue->modifyStorageClassName(m_admin, m_storageClassSingleCopy.name, newStorageClassName);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(newStorageClassName, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassName_nonExistentStorageClass) {
  using namespace cta;

  const std::string currentStorageClassName = "storage_class";
  const std::string newStorageClassName = "new_storage_class";
  ASSERT_THROW(m_catalogue->modifyStorageClassName(m_admin, currentStorageClassName, newStorageClassName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassName_newNameAlreadyExists) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto storageClass2 = m_storageClassSingleCopy;
  storageClass2.name = "storage_class2";

  m_catalogue->createStorageClass(m_admin, storageClass2);

  //Try to rename the first storage class with the name of the second one
  ASSERT_THROW(m_catalogue->modifyStorageClassName(m_admin, m_storageClassSingleCopy.name, storageClass2.name), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassVo) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto newVo = m_vo;
  newVo.name = "newVo";
  m_catalogue->createVirtualOrganization(m_admin, newVo);

  m_catalogue->modifyStorageClassVo(m_admin, m_storageClassSingleCopy.name, newVo.name);

  auto storageClasses = m_catalogue->getStorageClasses();
  ASSERT_EQ(newVo.name, storageClasses.front().vo.name);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassEmptyStringVo) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  ASSERT_THROW(m_catalogue->modifyStorageClassVo(m_admin, m_storageClassSingleCopy.name, ""), catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassVoDoesNotExist) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  ASSERT_THROW(m_catalogue->modifyStorageClassVo(m_admin, m_storageClassSingleCopy.name, "DOES_NOT_EXISTS"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createMediaType) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  const auto mediaTypes = m_catalogue->getMediaTypes();

  ASSERT_EQ(1, mediaTypes.size());

  ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
  ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
  ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
  ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
  ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
  ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
  ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
  ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
  ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

  const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createMediaType_same_twice) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);
  ASSERT_THROW(m_catalogue->createMediaType(m_admin, m_mediaType), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createMediaType_emptyStringMediaTypeName) {
  using namespace cta;

  auto mediaType = m_mediaType;
  mediaType.name = "";
  ASSERT_THROW(m_catalogue->createMediaType(m_admin, mediaType), catalogue::UserSpecifiedAnEmptyStringMediaTypeName);
}

TEST_P(cta_catalogue_CatalogueTest, createMediaType_emptyStringComment) {
  using namespace cta;

  auto mediaType = m_mediaType;
  mediaType.comment = "";
  ASSERT_THROW(m_catalogue->createMediaType(m_admin, mediaType), catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, createMediaType_emptyStringCartridge) {
  using namespace cta;

  auto mediaType = m_mediaType;
  mediaType.cartridge = "";
  ASSERT_THROW(m_catalogue->createMediaType(m_admin, mediaType), catalogue::UserSpecifiedAnEmptyStringCartridge);
}

TEST_P(cta_catalogue_CatalogueTest, createMediaType_zeroCapacity) {
  using namespace cta;

  auto mediaType = m_mediaType;
  mediaType.capacityInBytes = 0;
  ASSERT_THROW(m_catalogue->createMediaType(m_admin, mediaType), catalogue::UserSpecifiedAZeroCapacity);
}

TEST_P(cta_catalogue_CatalogueTest, deleteMediaType) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  const auto mediaTypes = m_catalogue->getMediaTypes();

  ASSERT_EQ(1, mediaTypes.size());

  ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
  ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
  ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
  ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
  ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
  ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
  ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
  ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
  ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

  const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteMediaType(m_mediaType.name);

  ASSERT_TRUE(m_catalogue->getMediaTypes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteMediaType_nonExistentMediaType) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->deleteMediaType("media_type"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteMediaType_usedByTapes) {
  using namespace cta;

  log::LogContext dummyLc(m_dummyLog);
  const bool logicalLibraryIsDisabled = false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);

  //Media type is used by at least one tape, deleting it should throw an exception
  ASSERT_THROW(m_catalogue->deleteMediaType(m_tape1.mediaType), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getTapes_non_existent_tape_pool) {
  using namespace cta;

  log::LogContext dummyLc(m_dummyLog);
  const bool logicalLibraryIsDisabled = false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);

  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.tapePool = "non_existent";
    ASSERT_THROW(m_catalogue->getTapes(criteria), catalogue::UserSpecifiedANonExistentTapePool);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_deleteStorageClass) {
  // TO BE DONE
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeName) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newMediaTypeName = "new_media_type";
  m_catalogue->modifyMediaTypeName(m_admin, m_mediaType.name, newMediaTypeName);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(newMediaTypeName, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeName_nonExistentMediaType) {
  using namespace cta;

  const std::string currentName = "media_type";
  const std::string newName = "new_media_type";
  ASSERT_THROW(m_catalogue->modifyMediaTypeName(m_admin, currentName, newName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeName_newNameAlreadyExists) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  auto mediaType2 = m_mediaType;
  mediaType2.name = "media_type_2";

  m_catalogue->createMediaType(m_admin, mediaType2);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(2, mediaTypes.size());

    const auto mediaTypeMap = mediaTypeWithLogsListToMap(mediaTypes);

    ASSERT_EQ(2, mediaTypeMap.size());

    auto mediaType1Itor = mediaTypeMap.find(m_mediaType.name);
    ASSERT_TRUE(mediaType1Itor != mediaTypeMap.end());

    ASSERT_EQ(m_mediaType.name, mediaType1Itor->second.name);
    ASSERT_EQ(m_mediaType.cartridge, mediaType1Itor->second.cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaType1Itor->second.capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaType1Itor->second.primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaType1Itor->second.secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaType1Itor->second.nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaType1Itor->second.minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaType1Itor->second.maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaType1Itor->second.comment);

    const common::dataStructures::EntryLog creationLog1 = mediaType1Itor->second.creationLog;
    ASSERT_EQ(m_admin.username, creationLog1.username);
    ASSERT_EQ(m_admin.host, creationLog1.host);

    const common::dataStructures::EntryLog lastModificationLog1 = mediaType1Itor->second.lastModificationLog;
    ASSERT_EQ(creationLog1, lastModificationLog1);

    auto mediaType2Itor = mediaTypeMap.find(mediaType2.name);
    ASSERT_TRUE(mediaType2Itor != mediaTypeMap.end());

    ASSERT_EQ(mediaType2.name, mediaType2Itor->second.name);
    ASSERT_EQ(mediaType2.cartridge, mediaType2Itor->second.cartridge);
    ASSERT_EQ(mediaType2.capacityInBytes, mediaType2Itor->second.capacityInBytes);
    ASSERT_EQ(mediaType2.primaryDensityCode, mediaType2Itor->second.primaryDensityCode);
    ASSERT_EQ(mediaType2.secondaryDensityCode, mediaType2Itor->second.secondaryDensityCode);
    ASSERT_EQ(mediaType2.nbWraps, mediaType2Itor->second.nbWraps);
    ASSERT_EQ(mediaType2.minLPos, mediaType2Itor->second.minLPos);
    ASSERT_EQ(mediaType2.maxLPos, mediaType2Itor->second.maxLPos);
    ASSERT_EQ(mediaType2.comment, mediaType2Itor->second.comment);

    const common::dataStructures::EntryLog creationLog2 = mediaType2Itor->second.creationLog;
    ASSERT_EQ(m_admin.username, creationLog2.username);
    ASSERT_EQ(m_admin.host, creationLog2.host);

    const common::dataStructures::EntryLog lastModificationLog2 = mediaType2Itor->second.lastModificationLog;
    ASSERT_EQ(creationLog2, lastModificationLog2);
  }

  // Try to rename the first media type with the name of the second one
  ASSERT_THROW(m_catalogue->modifyMediaTypeName(m_admin, m_mediaType.name, mediaType2.name),exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeCartridge) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedCartridge = "new_cartridge";
  m_catalogue->modifyMediaTypeCartridge(m_admin, m_mediaType.name, modifiedCartridge);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(modifiedCartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeCartridge_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const std::string cartridge = "cartride";
  ASSERT_THROW(m_catalogue->modifyMediaTypeCartridge(m_admin, name, cartridge), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeCapacityInBytes) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedCapacityInBytes = m_mediaType.capacityInBytes + 7;
  m_catalogue->modifyMediaTypeCapacityInBytes(m_admin, m_mediaType.name, modifiedCapacityInBytes);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(modifiedCapacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeCapacityInBytes_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const uint64_t capacityInBytes = 1;
  ASSERT_THROW(m_catalogue->modifyMediaTypeCapacityInBytes(m_admin, name, capacityInBytes), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypePrimaryDensityCode) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint8_t modifiedPrimaryDensityCode = 7;
  m_catalogue->modifyMediaTypePrimaryDensityCode(m_admin, m_mediaType.name, modifiedPrimaryDensityCode);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(modifiedPrimaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypePrimaryDensityCode_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const uint8_t primaryDensityCode = 1;
  ASSERT_THROW(m_catalogue->modifyMediaTypePrimaryDensityCode(m_admin, name, primaryDensityCode), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeSecondaryDensityCode) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint8_t modifiedSecondaryDensityCode = 7;
  m_catalogue->modifyMediaTypeSecondaryDensityCode(m_admin, m_mediaType.name, modifiedSecondaryDensityCode);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(modifiedSecondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeSecondaryDensityCode_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const uint8_t secondaryDensityCode = 1;
  ASSERT_THROW(m_catalogue->modifyMediaTypeSecondaryDensityCode(m_admin, name, secondaryDensityCode), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeNbWraps) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint32_t modifiedNbWraps = 7;
  m_catalogue->modifyMediaTypeNbWraps(m_admin, m_mediaType.name, modifiedNbWraps);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(modifiedNbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeNbWraps_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const uint32_t nbWraps = 1;
  ASSERT_THROW(m_catalogue->modifyMediaTypeNbWraps(m_admin, name, nbWraps), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeMinLPos) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMinLPos = 7;
  m_catalogue->modifyMediaTypeMinLPos(m_admin, m_mediaType.name, modifiedMinLPos);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(modifiedMinLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeMinLPos_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const uint64_t minLPos = 1;
  ASSERT_THROW(m_catalogue->modifyMediaTypeMinLPos(m_admin, name, minLPos), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeMaxLPos) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMaxLPos = 7;
  m_catalogue->modifyMediaTypeMaxLPos(m_admin, m_mediaType.name, modifiedMaxLPos);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(modifiedMaxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeMaxLPos_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const uint64_t maxLPos = 1;
  ASSERT_THROW(m_catalogue->modifyMediaTypeMaxLPos(m_admin, name, maxLPos), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeComment) {
  using namespace cta;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(m_mediaType.comment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mediaTypes.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyMediaTypeComment(m_admin, m_mediaType.name, modifiedComment);

  {
    const auto mediaTypes = m_catalogue->getMediaTypes();

    ASSERT_EQ(1, mediaTypes.size());

    ASSERT_EQ(m_mediaType.name, mediaTypes.front().name);
    ASSERT_EQ(m_mediaType.cartridge, mediaTypes.front().cartridge);
    ASSERT_EQ(m_mediaType.capacityInBytes, mediaTypes.front().capacityInBytes);
    ASSERT_EQ(m_mediaType.primaryDensityCode, mediaTypes.front().primaryDensityCode);
    ASSERT_EQ(m_mediaType.secondaryDensityCode, mediaTypes.front().secondaryDensityCode);
    ASSERT_EQ(m_mediaType.nbWraps, mediaTypes.front().nbWraps);
    ASSERT_EQ(m_mediaType.minLPos, mediaTypes.front().minLPos);
    ASSERT_EQ(m_mediaType.maxLPos, mediaTypes.front().maxLPos);
    ASSERT_EQ(modifiedComment, mediaTypes.front().comment);

    const common::dataStructures::EntryLog creationLog = mediaTypes.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMediaTypeComment_nonExistentMediaType) {
  using namespace cta;

  const std::string name = "media_type";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyMediaTypeComment(m_admin, name, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getMediaTypeByVid_nonExistentTape) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->getMediaTypeByVid("DOES_NOT_EXIST"),exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, getMediaTypeByVid) {
  using namespace cta;

  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  auto tapeMediaType = m_catalogue->getMediaTypeByVid(m_tape1.vid);
  ASSERT_EQ(m_mediaType.name, tapeMediaType.name);
  ASSERT_EQ(m_mediaType.capacityInBytes, tapeMediaType.capacityInBytes);
  ASSERT_EQ(m_mediaType.cartridge, tapeMediaType.cartridge);
  ASSERT_EQ(m_mediaType.comment, tapeMediaType.comment);
  ASSERT_EQ(m_mediaType.maxLPos, tapeMediaType.maxLPos);
  ASSERT_EQ(m_mediaType.minLPos, tapeMediaType.minLPos);
  ASSERT_EQ(m_mediaType.nbWraps, tapeMediaType.nbWraps);
  ASSERT_EQ(m_mediaType.primaryDensityCode, tapeMediaType.primaryDensityCode);
  ASSERT_EQ(m_mediaType.secondaryDensityCode, tapeMediaType.secondaryDensityCode);
}

TEST_P(cta_catalogue_CatalogueTest, getTapePool_non_existent) {
  using namespace cta;

  const std::string tapePoolName = "non_existent_tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const auto pool = m_catalogue->getTapePool(tapePoolName);

  ASSERT_FALSE((bool)pool);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  ASSERT_TRUE(m_catalogue->tapePoolExists(tapePoolName));

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(supply.value(), pool.supply.value());
    ASSERT_EQ(supply, pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  {
    const auto pool = m_catalogue->getTapePool(tapePoolName);

    ASSERT_TRUE((bool)pool);

    ASSERT_EQ(tapePoolName, pool->name);
    ASSERT_EQ(m_vo.name, pool->vo.name);
    ASSERT_EQ(nbPartialTapes, pool->nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool->encryption);
    ASSERT_TRUE((bool)pool->supply);
    ASSERT_EQ(supply.value(), pool->supply.value());
    ASSERT_EQ(supply, pool->supply);
    ASSERT_EQ(0, pool->nbTapes);
    ASSERT_EQ(0, pool->capacityBytes);
    ASSERT_EQ(0, pool->dataBytes);
    ASSERT_EQ(0, pool->nbPhysicalFiles);
    ASSERT_EQ(comment, pool->comment);

    const common::dataStructures::EntryLog creationLog = pool->creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool->lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_null_supply) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply;
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  ASSERT_TRUE(m_catalogue->tapePoolExists(tapePoolName));

  const auto pools = m_catalogue->getTapePools();

  ASSERT_EQ(1, pools.size());

  const auto &pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(m_vo.name, pool.vo.name);
  ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
  ASSERT_EQ(isEncrypted, pool.encryption);
  ASSERT_FALSE((bool)pool.supply);
  ASSERT_EQ(0, pool.nbTapes);
  ASSERT_EQ(0, pool.capacityBytes);
  ASSERT_EQ(0, pool.dataBytes);
  ASSERT_EQ(0, pool.nbPhysicalFiles);
  ASSERT_EQ(comment, pool.comment);

  const common::dataStructures::EntryLog creationLog = pool.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_same_twice) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_vo_does_not_exist) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_tapes_of_mixed_state) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  cta::catalogue::TapeSearchCriteria criteria;
  criteria.vid = m_tape1.vid;
  ASSERT_EQ(0,m_catalogue->getTapes(criteria).size());

  m_catalogue->createTape(m_admin, m_tape1);

  auto tape_disabled_01 = m_tape1;
  tape_disabled_01.vid = "D000001";
  tape_disabled_01.state = common::dataStructures::Tape::DISABLED;
  tape_disabled_01.stateReason = "unit Test";
  m_catalogue->createTape(m_admin, tape_disabled_01);

  auto tape_disabled_02 = m_tape1;
  tape_disabled_02.vid = "D000002";
  tape_disabled_02.state = common::dataStructures::Tape::DISABLED;
  tape_disabled_02.stateReason = "unit Test";
  m_catalogue->createTape(m_admin, tape_disabled_02);

  auto tape_broken_01 = m_tape1;
  tape_broken_01.vid = "B000002";
  tape_broken_01.state = common::dataStructures::Tape::BROKEN;
  tape_broken_01.stateReason = "unit Test";
  m_catalogue->createTape(m_admin, tape_broken_01);

  auto tape_full_01 = m_tape1;
  tape_full_01.vid = "F000001";
  tape_full_01.full = true;
  m_catalogue->createTape(m_admin, tape_full_01);

  auto tape_full_02 = m_tape1;
  tape_full_02.vid = "F000002";
  tape_full_02.full = true;
  m_catalogue->createTape(m_admin, tape_full_02);

  auto tape_full_03 = m_tape1;
  tape_full_03.vid = "F000003";
  tape_full_03.full = true;
  m_catalogue->createTape(m_admin, tape_full_03);

  auto tape_broken_full_01 = m_tape1;
  tape_broken_full_01.vid = "BFO001";
  tape_broken_full_01.state = common::dataStructures::Tape::BROKEN;
  tape_broken_full_01.stateReason = "unit Test";
  tape_broken_full_01.full = true;
  m_catalogue->createTape(m_admin, tape_broken_full_01);

  auto tape_disabled_full_01 = m_tape1;
  tape_disabled_full_01.vid = "DFO001";
  tape_disabled_full_01.state = common::dataStructures::Tape::DISABLED;
  tape_disabled_full_01.stateReason = "unit Test";
  tape_disabled_full_01.full = true;
  m_catalogue->createTape(m_admin, tape_disabled_full_01);

  auto tape_disabled_full_02 = m_tape1;
  tape_disabled_full_02.vid = "DFO002";
  tape_disabled_full_02.full = true;
  tape_disabled_full_02.state = common::dataStructures::Tape::DISABLED;
  tape_disabled_full_02.stateReason = "unit Test";
  m_catalogue->createTape(m_admin, tape_disabled_full_02);

  const auto tapes = m_catalogue->getTapes();

  ASSERT_EQ(10, tapes.size());

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(10, pool.nbTapes);
    ASSERT_EQ(10, pool.nbEmptyTapes);
    ASSERT_EQ(4, pool.nbDisabledTapes);
    ASSERT_EQ(6, pool.nbFullTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(10 * m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    const auto pool = m_catalogue->getTapePool(m_tape1.tapePoolName);
    ASSERT_TRUE((bool)pool);

    ASSERT_EQ(m_tape1.tapePoolName, pool->name);
    ASSERT_EQ(m_vo.name, pool->vo.name);
    ASSERT_EQ(10, pool->nbTapes);
    ASSERT_EQ(10, pool->nbEmptyTapes);
    ASSERT_EQ(4, pool->nbDisabledTapes);
    ASSERT_EQ(6, pool->nbFullTapes);
    ASSERT_EQ(1, pool->nbWritableTapes);
    ASSERT_EQ(10 * m_mediaType.capacityInBytes, pool->capacityBytes);
    ASSERT_EQ(0, pool->dataBytes);
    ASSERT_EQ(0, pool->nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool) {
  using namespace cta;

  const uint64_t tapePoolNbPartialTapes = 2;
  const bool tapePoolIsEncrypted = true;
  const std::string tapePoolComment = "Create tape pool";
  {
    const cta::optional<std::string> supply("value for the supply pool mechanism");
    m_catalogue->createVirtualOrganization(m_admin, m_vo);
    m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, tapePoolNbPartialTapes, tapePoolIsEncrypted,
      supply, tapePoolComment);
  }

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(tapePoolNbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(tapePoolIsEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(tapePoolComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  // Create a separate archive route with another tape pool that has nothing to
  // do with the tape pool being tested in order to test
  // RdbmsCatalogue::tapePoolUsedInAnArchiveRoute()
  const std::string anotherTapePoolName = "another_tape_pool";
  const uint64_t anotherNbPartialTapes = 4;
  const std::string anotherTapePoolComment = "Create another tape pool";
  const bool anotherTapePoolIsEncrypted = false;
  {
    m_catalogue->createStorageClass(m_admin, m_anotherStorageClass);
    const cta::optional<std::string> supply("value for the supply pool mechanism");
    m_catalogue->createVirtualOrganization(m_admin, m_anotherVo);
    m_catalogue->createTapePool(m_admin, anotherTapePoolName, m_anotherVo.name, anotherNbPartialTapes,
      anotherTapePoolIsEncrypted, supply, anotherTapePoolComment);
    const uint32_t copyNb = 1;
    const std::string comment = "Create a separate archive route";
    m_catalogue->createArchiveRoute(m_admin, m_anotherStorageClass.name, copyNb, anotherTapePoolName, comment);
  }

  {
    const auto pools = tapePoolListToMap(m_catalogue->getTapePools());

    ASSERT_EQ(2, pools.size());

    {
      const auto poolMaplet = pools.find(m_tape1.tapePoolName);
      ASSERT_NE(pools.end(), poolMaplet);

      const auto &pool = poolMaplet->second;
      ASSERT_EQ(m_tape1.tapePoolName, pool.name);
      ASSERT_EQ(m_vo.name, pool.vo.name);
      ASSERT_EQ(tapePoolNbPartialTapes, pool.nbPartialTapes);
      ASSERT_EQ(tapePoolIsEncrypted, pool.encryption);
      ASSERT_EQ(0, pool.nbTapes);
      ASSERT_EQ(0, pool.capacityBytes);
      ASSERT_EQ(0, pool.dataBytes);
      ASSERT_EQ(0, pool.nbPhysicalFiles);
      ASSERT_EQ(tapePoolComment, pool.comment);

      const common::dataStructures::EntryLog creationLog = pool.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }

    {
      const auto poolMaplet = pools.find(anotherTapePoolName);
      ASSERT_NE(pools.end(), poolMaplet);

      const auto &pool = poolMaplet->second;
      ASSERT_EQ(anotherTapePoolName, pool.name);
      ASSERT_EQ(m_anotherVo.name, pool.vo.name);
      ASSERT_EQ(anotherNbPartialTapes, pool.nbPartialTapes);
      ASSERT_EQ(anotherTapePoolIsEncrypted, pool.encryption);
      ASSERT_EQ(0, pool.nbTapes);
      ASSERT_EQ(0, pool.capacityBytes);
      ASSERT_EQ(0, pool.dataBytes);
      ASSERT_EQ(0, pool.nbPhysicalFiles);
      ASSERT_EQ(anotherTapePoolComment, pool.comment);

      const common::dataStructures::EntryLog creationLog = pool.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  m_catalogue->deleteTapePool(m_tape1.tapePoolName);

  ASSERT_EQ(1, m_catalogue->getTapePools().size());
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool_notEmpty) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, m_tape1);

  ASSERT_TRUE(m_catalogue->tapeExists(m_tape1.vid));

  const auto tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.state,tape.state);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const auto creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->deleteTapePool(m_tape1.tapePoolName), catalogue::UserSpecifiedAnEmptyTapePool);
  ASSERT_THROW(m_catalogue->deleteTapePool(m_tape1.tapePoolName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_emptyStringTapePoolName) {
  using namespace cta;

  const std::string tapePoolName = "";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_emptyStringVO) {
  using namespace cta;

  const std::string vo = "";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, "", nbPartialTapes, isEncrypted, supply, comment), catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_emptyStringComment) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool_non_existent) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->deleteTapePool("non_existent_tape_pool"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool_used_in_an_archive_route) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes(m_storageClassSingleCopy.name, tapePoolName);

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->deleteTapePool(tapePoolName), catalogue::UserSpecifiedTapePoolUsedInAnArchiveRoute);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolVo) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  auto modifiedVo = m_vo;
  modifiedVo.name = "modified_vo";
  m_catalogue->createVirtualOrganization(m_admin, modifiedVo);
  m_catalogue->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo.name);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(modifiedVo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolVo_emptyStringTapePool) {
  using namespace cta;

  const std::string tapePoolName = "";
  const std::string modifiedVo = "modified_vo";
  ASSERT_THROW(m_catalogue->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolVo_emptyStringVo) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedVo = "";
  ASSERT_THROW(m_catalogue->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo),
    catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolVo_VoDoesNotExist) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedVo = "DoesNotExists";
  ASSERT_THROW(m_catalogue->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolNbPartialTapes) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedNbPartialTapes = 5;
  m_catalogue->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, modifiedNbPartialTapes);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(modifiedNbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolNbPartialTapes_emptyStringTapePoolName) {
  using namespace cta;

  const std::string tapePoolName = "";
  const uint64_t modifiedNbPartialTapes = 5;
  ASSERT_THROW(m_catalogue->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, modifiedNbPartialTapes), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolNbPartialTapes_nonExistentTapePool) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 5;
  ASSERT_THROW(m_catalogue->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, nbPartialTapes), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyTapePoolComment(m_admin, tapePoolName, modifiedComment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(modifiedComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment_emptyStringTapePoolName) {
  using namespace cta;

  const std::string tapePoolName = "";
  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->modifyTapePoolComment(m_admin, tapePoolName, modifiedComment), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment_emptyStringComment) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "";
  ASSERT_THROW(m_catalogue->modifyTapePoolComment(m_admin, tapePoolName, modifiedComment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment_nonExistentTapePool) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyTapePoolComment(m_admin, tapePoolName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapePoolEncryption) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const bool modifiedIsEncrypted = !isEncrypted;
  m_catalogue->setTapePoolEncryption(m_admin, tapePoolName, modifiedIsEncrypted);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(modifiedIsEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapePoolEncryption_nonExistentTapePool) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const bool isEncrypted = false;
  ASSERT_THROW(m_catalogue->setTapePoolEncryption(m_admin, tapePoolName, isEncrypted), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolSupply) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)supply);
    ASSERT_EQ(supply.value(), pool.supply.value());
    ASSERT_EQ(supply, pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedSupply("Modified supply");
  m_catalogue->modifyTapePoolSupply(m_admin, tapePoolName, modifiedSupply);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)supply);
    ASSERT_EQ(modifiedSupply, pool.supply.value());
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolSupply_emptyStringTapePoolName) {
  using namespace cta;

  const std::string tapePoolName = "";
  const std::string modifiedSupply = "Modified supply";
  ASSERT_THROW(m_catalogue->modifyTapePoolSupply(m_admin, tapePoolName, modifiedSupply), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolSupply_emptyStringSupply) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)supply);
    ASSERT_EQ(supply.value(), pool.supply.value());
    ASSERT_EQ(supply, pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedSupply;
  m_catalogue->modifyTapePoolSupply(m_admin, tapePoolName, modifiedSupply);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_FALSE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolSupply_nonExistentTapePool) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const std::string supply = "value for the supply pool mechanism";
  ASSERT_THROW(m_catalogue->modifyTapePoolSupply(m_admin, tapePoolName, supply), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getTapePools_filterName) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const std::string secondTapePoolName = "tape_pool_2";

  const uint64_t nbFirstPoolPartialTapes = 2;
  const uint64_t nbSecondPoolPartialTapes = 3;

  const bool firstPoolIsEncrypted = true;
  const bool secondPoolIsEncrypted = false;

  const cta::optional<std::string> firstPoolSupply("value for the supply first pool mechanism");
  const cta::optional<std::string> secondPoolSupply("value for the supply second pool mechanism");

  const std::string firstPoolComment = "Create first tape pool";
  const std::string secondPoolComment = "Create second tape pool";

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createVirtualOrganization(m_admin, m_anotherVo);

  m_catalogue->createTapePool(m_admin, tapePoolName, m_vo.name, nbFirstPoolPartialTapes, firstPoolIsEncrypted, firstPoolSupply, firstPoolComment);
  m_catalogue->createTapePool(m_admin, secondTapePoolName, m_anotherVo.name, nbSecondPoolPartialTapes, secondPoolIsEncrypted, secondPoolSupply, secondPoolComment);


  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = tapePoolName;
    const auto pools = m_catalogue->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbFirstPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(firstPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(firstPoolComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = secondTapePoolName;
    const auto pools = m_catalogue->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(secondTapePoolName, pool.name);
    ASSERT_EQ(m_anotherVo.name, pool.vo.name);
    ASSERT_EQ(nbSecondPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(secondPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(secondPoolComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = "no pool with such name";

    ASSERT_THROW(m_catalogue->getTapePools(criteria), catalogue::UserSpecifiedANonExistentTapePool);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = "";

    ASSERT_THROW(m_catalogue->getTapePools(criteria), exception::UserError);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getTapePools_filterVO) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const std::string secondTapePoolName = "tape_pool_2";

  const uint64_t nbFirstPoolPartialTapes = 2;
  const uint64_t nbSecondPoolPartialTapes = 3;

  const bool firstPoolIsEncrypted = true;
  const bool secondPoolIsEncrypted = false;

  const cta::optional<std::string> firstPoolSupply("value for the supply first pool mechanism");
  const cta::optional<std::string> secondPoolSupply("value for the supply second pool mechanism");

  const std::string firstPoolComment = "Create first tape pool";
  const std::string secondPoolComment = "Create second tape pool";

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createVirtualOrganization(m_admin, m_anotherVo);

  m_catalogue->createTapePool(m_admin, tapePoolName, m_vo.name, nbFirstPoolPartialTapes, firstPoolIsEncrypted, firstPoolSupply, firstPoolComment);
  m_catalogue->createTapePool(m_admin, secondTapePoolName, m_anotherVo.name, nbSecondPoolPartialTapes, secondPoolIsEncrypted, secondPoolSupply, secondPoolComment);


  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = m_vo.name;
    const auto pools = m_catalogue->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbFirstPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(firstPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(firstPoolComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = m_anotherVo.name;
    const auto pools = m_catalogue->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(secondTapePoolName, pool.name);
    ASSERT_EQ(m_anotherVo.name, pool.vo.name);
    ASSERT_EQ(nbSecondPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(secondPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(secondPoolComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = "no vo with such name";

    ASSERT_THROW(m_catalogue->getTapePools(criteria), catalogue::UserSpecifiedANonExistentVirtualOrganization);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = "";

    ASSERT_THROW(m_catalogue->getTapePools(criteria), exception::UserError);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getTapePools_filterEncrypted) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const std::string secondTapePoolName = "tape_pool_2";

  const uint64_t nbFirstPoolPartialTapes = 2;
  const uint64_t nbSecondPoolPartialTapes = 3;

  const bool firstPoolIsEncrypted = true;
  const bool secondPoolIsEncrypted = false;

  const cta::optional<std::string> firstPoolSupply("value for the supply first pool mechanism");
  const cta::optional<std::string> secondPoolSupply("value for the supply second pool mechanism");

  const std::string firstPoolComment = "Create first tape pool";
  const std::string secondPoolComment = "Create second tape pool";

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createVirtualOrganization(m_admin, m_anotherVo);

  m_catalogue->createTapePool(m_admin, tapePoolName, m_vo.name, nbFirstPoolPartialTapes, firstPoolIsEncrypted, firstPoolSupply, firstPoolComment);
  m_catalogue->createTapePool(m_admin, secondTapePoolName, m_anotherVo.name, nbSecondPoolPartialTapes, secondPoolIsEncrypted, secondPoolSupply, secondPoolComment);


  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.encrypted = true;
    const auto pools = m_catalogue->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbFirstPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(firstPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(firstPoolComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.encrypted = false;
    const auto pools = m_catalogue->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(secondTapePoolName, pool.name);
    ASSERT_EQ(m_anotherVo.name, pool.vo.name);
    ASSERT_EQ(nbSecondPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(secondPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(secondPoolComment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes(m_storageClassSingleCopy.name, tapePoolName);

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolName) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newTapePoolName = "new_tape_pool";
  m_catalogue->modifyTapePoolName(m_admin, tapePoolName, newTapePoolName);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(newTapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolName_emptyStringCurrentTapePoolName) {
  using namespace cta;

  const std::string tapePoolName = "";
  const std::string newTapePoolName = "new_tape_pool";
  ASSERT_THROW(m_catalogue->modifyTapePoolName(m_admin, tapePoolName, newTapePoolName),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolName_emptyStringNewTapePoolName) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newTapePoolName = "";
  ASSERT_THROW(m_catalogue->modifyTapePoolName(m_admin, tapePoolName, newTapePoolName),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_emptyStringStorageClassName) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);

  common::dataStructures::StorageClass storageClass;

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string storageClassName = "";
  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClassName, copyNb,
   tapePoolName, comment), catalogue::UserSpecifiedAnEmptyStringStorageClassName);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_zeroCopyNb) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 0;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment), catalogue::UserSpecifiedAZeroCopyNb);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_emptyStringTapePoolName) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "";
  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, comment), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_emptyStringComment) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment), catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_non_existent_storage_class) {
  using namespace cta;

  const std::string storageClassName = "storage_class";

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClassName, copyNb, m_tape1.tapePoolName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_non_existent_tape_pool) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "non_existent_tape_pool";

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";

  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_same_twice) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment);
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_two_routes_same_pool) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb1 = 1;
  const std::string comment1 = "Create archive route for copy 1";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb1, m_tape1.tapePoolName, comment1);

  const uint32_t copyNb2 = 2;
  const std::string comment2 = "Create archive route for copy 2";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb2, m_tape1.tapePoolName, comment2), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveRoute) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteArchiveRoute(m_storageClassSingleCopy.name, copyNb);

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveRoute_non_existent) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->deleteArchiveRoute("non_existent_storage_class", 1234), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_deleteStorageClass) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name), catalogue::UserSpecifiedStorageClassUsedByArchiveRoutes);
  ASSERT_THROW(m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteTapePoolName) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string anotherTapePoolName = "another_tape_pool";
  m_catalogue->createTapePool(m_admin, anotherTapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create another tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyArchiveRouteTapePoolName(m_admin, m_storageClassSingleCopy.name, copyNb, anotherTapePoolName);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(anotherTapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteTapePoolName_nonExistentTapePool) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string anotherTapePoolName = "another_tape_pool";
  m_catalogue->createTapePool(m_admin, anotherTapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create another tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->modifyArchiveRouteTapePoolName(m_admin, m_storageClassSingleCopy.name, copyNb, "non_existent_tape_pool"), catalogue::UserSpecifiedANonExistentTapePool);
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteTapePoolName_nonExistentArchiveRoute) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  ASSERT_THROW(m_catalogue->modifyArchiveRouteTapePoolName(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName), catalogue::UserSpecifiedANonExistentArchiveRoute);
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteComment) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, m_tape1.tapePoolName, comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyArchiveRouteComment(m_admin, m_storageClassSingleCopy.name, copyNb, modifiedComment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(modifiedComment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteComment_nonExistentArchiveRoute) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyArchiveRouteComment(m_admin, m_storageClassSingleCopy.name, copyNb, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment);

  const std::list<common::dataStructures::LogicalLibrary> libs = m_catalogue->getLogicalLibraries();

  ASSERT_EQ(1, libs.size());

  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_FALSE(lib.isDisabled);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary_disabled_true) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled(true);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment);

  const std::list<common::dataStructures::LogicalLibrary> libs = m_catalogue->getLogicalLibraries();

  ASSERT_EQ(1, libs.size());

  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_TRUE(lib.isDisabled);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary_disabled_false) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled(false);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment);

  const std::list<common::dataStructures::LogicalLibrary> libs = m_catalogue->getLogicalLibraries();

  ASSERT_EQ(1, libs.size());

  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_FALSE(lib.isDisabled);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary_same_twice) {
  using namespace cta;

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment);
  ASSERT_THROW(m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setLogicalLibraryDisabled_true) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs =
      m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_FALSE(lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const bool modifiedLogicalLibraryIsDisabled= true;
  m_catalogue->setLogicalLibraryDisabled(m_admin, logicalLibraryName, modifiedLogicalLibraryIsDisabled);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs =
      m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(modifiedLogicalLibraryIsDisabled, lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setLogicalLibraryDisabled_false) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs =
      m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_FALSE(lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const bool modifiedLogicalLibraryIsDisabled= false;
  m_catalogue->setLogicalLibraryDisabled(m_admin, logicalLibraryName, modifiedLogicalLibraryIsDisabled);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs =
      m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(modifiedLogicalLibraryIsDisabled, lib.isDisabled);
    ASSERT_EQ(comment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, deleteLogicalLibrary) {
  using namespace cta;

  const bool libNotToDeleteIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string libNotToDeleteComment = "Create logical library to NOT be deleted";

  // Create a tape and a logical library that are not the ones to be deleted
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, libNotToDeleteIsDisabled, libNotToDeleteComment);
  {
    const auto libs = m_catalogue->getLogicalLibraries();
    ASSERT_EQ(1, libs.size());
    const auto lib = libs.front();
    ASSERT_EQ(m_tape1.logicalLibraryName, lib.name);
    ASSERT_EQ(libNotToDeleteIsDisabled, lib.isDisabled);
    ASSERT_EQ(libNotToDeleteComment, lib.comment);
    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
    const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
  m_catalogue->createTape(m_admin, m_tape1);
  ASSERT_TRUE(m_catalogue->tapeExists(m_tape1.vid));
  {
    const auto tapes = m_catalogue->getTapes();
    ASSERT_EQ(1, tapes.size());

    const auto tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.state,tape.state);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const auto creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  // Create the logical library to be deleted
  const std::string libToDeleteName = "lib_to_delete";
  const bool libToDeleteIsDisabled = false;
  const std::string libToDeleteComment = "Create logical library to be deleted";
  m_catalogue->createLogicalLibrary(m_admin, libToDeleteName, libToDeleteIsDisabled, libToDeleteComment);
  {
    const auto libs = m_catalogue->getLogicalLibraries();
    ASSERT_EQ(2, libs.size());
    const auto nameToLib = logicalLibraryListToMap(libs);
    ASSERT_EQ(2, nameToLib.size());

    {
      const auto nameToLibItor = nameToLib.find(m_tape1.logicalLibraryName);
      ASSERT_NE(nameToLib.end(), nameToLibItor);
      const auto &lib = nameToLibItor->second;
      ASSERT_EQ(m_tape1.logicalLibraryName, lib.name);
      ASSERT_EQ(libNotToDeleteIsDisabled, lib.isDisabled);
      ASSERT_EQ(libNotToDeleteComment, lib.comment);
      const common::dataStructures::EntryLog creationLog = lib.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);
      const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }

    {
      const auto nameToLibItor = nameToLib.find(libToDeleteName);
      ASSERT_NE(nameToLib.end(), nameToLibItor);
      const auto &lib = nameToLibItor->second;
      ASSERT_EQ(libToDeleteName, lib.name);
      ASSERT_EQ(libToDeleteIsDisabled, lib.isDisabled);
      ASSERT_EQ(libToDeleteComment, lib.comment);
      const common::dataStructures::EntryLog creationLog = lib.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);
      const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  m_catalogue->deleteLogicalLibrary(libToDeleteName);
  {
    const auto libs = m_catalogue->getLogicalLibraries();
    ASSERT_EQ(1, libs.size());
    const auto lib = libs.front();
    ASSERT_EQ(m_tape1.logicalLibraryName, lib.name);
    ASSERT_EQ(libNotToDeleteIsDisabled, lib.isDisabled);
    ASSERT_EQ(libNotToDeleteComment, lib.comment);
    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
    const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_CatalogueTest, deleteLogicalLibrary_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
  ASSERT_THROW(m_catalogue->deleteLogicalLibrary("non_existent_logical_library"),
    catalogue::UserSpecifiedANonExistentLogicalLibrary);
}

TEST_P(cta_catalogue_CatalogueTest, deleteLogicalLibrary_non_empty) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);
  ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
  ASSERT_EQ(m_tape1.vendor, tape.vendor);
  ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
  ASSERT_EQ(m_vo.name, tape.vo);
  ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(m_tape1.state,tape.state);
  ASSERT_EQ(m_tape1.full, tape.full);

  ASSERT_FALSE(tape.isFromCastor);
  ASSERT_EQ(m_tape1.comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->deleteLogicalLibrary(m_tape1.logicalLibraryName), catalogue::UserSpecifiedANonEmptyLogicalLibrary);
}

TEST_P(cta_catalogue_CatalogueTest, modifyLogicalLibraryName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string libraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool libraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, libraryName, libraryIsDisabled, comment);

  {
    const auto libraries = m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libraries.size());

    const auto &library = libraries.front();
    ASSERT_EQ(libraryName, library.name);
    ASSERT_FALSE(library.isDisabled);
    ASSERT_EQ(comment, library.comment);

    const common::dataStructures::EntryLog creationLog = library.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = library.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newLibraryName = "new_logical_library";
  m_catalogue->modifyLogicalLibraryName(m_admin, libraryName, newLibraryName);

  {
    const auto libraries = m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libraries.size());

    const auto &library = libraries.front();
    ASSERT_EQ(newLibraryName, library.name);
    ASSERT_FALSE(library.isDisabled);
    ASSERT_EQ(comment, library.comment);

    const common::dataStructures::EntryLog creationLog = library.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyLogicalLibraryName_emptyStringCurrentLogicalLibraryName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string libraryName = "logical_library";
  const bool libraryIsDisabled = false;
  const std::string comment = "Create logical library";
  m_catalogue->createLogicalLibrary(m_admin, libraryName, libraryIsDisabled, comment);

  const std::string newLibraryName = "new_logical_library";
  ASSERT_THROW(m_catalogue->modifyLogicalLibraryName(m_admin, "", newLibraryName),
    catalogue::UserSpecifiedAnEmptyStringLogicalLibraryName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyLogicalLibraryName_emptyStringNewLogicalLibraryName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string libraryName = "logical_library";
  const bool libraryIsDisabled = false;
  const std::string comment = "Create logical library";
  m_catalogue->createLogicalLibrary(m_admin, libraryName, libraryIsDisabled, comment);

  {
    const auto libraries = m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libraries.size());

    const auto library = libraries.front();
    ASSERT_EQ(libraryName, library.name);
    ASSERT_FALSE(library.isDisabled);
    ASSERT_EQ(comment, library.comment);

    const common::dataStructures::EntryLog creationLog = library.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = library.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newLibraryName = "";
  ASSERT_THROW(m_catalogue->modifyLogicalLibraryName(m_admin, libraryName, newLibraryName),
    catalogue::UserSpecifiedAnEmptyStringLogicalLibraryName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyLogicalLibraryComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, comment);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs = m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(comment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyLogicalLibraryComment(m_admin, logicalLibraryName, modifiedComment);

  {
    const std::list<common::dataStructures::LogicalLibrary> libs = m_catalogue->getLogicalLibraries();

    ASSERT_EQ(1, libs.size());

    const common::dataStructures::LogicalLibrary lib = libs.front();
    ASSERT_EQ(logicalLibraryName, lib.name);
    ASSERT_EQ(modifiedComment, lib.comment);

    const common::dataStructures::EntryLog creationLog = lib.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = lib.lastModificationLog;
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyLogicalLibraryComment_nonExisentLogicalLibrary) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());

  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  ASSERT_THROW(m_catalogue->modifyLogicalLibraryComment(m_admin, logicalLibraryName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeExists_emptyString) {
  using namespace cta;

  const std::string vid = "";
  ASSERT_THROW(m_catalogue->tapeExists(vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, createTape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, m_tape1);

  ASSERT_TRUE(m_catalogue->tapeExists(m_tape1.vid));

  const auto tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.state,tape.state);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const auto creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringVid) {
  using namespace cta;

  const std::string vid = "";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    auto tape = m_tape1;
    tape.vid = "";
    ASSERT_THROW(m_catalogue->createTape(m_admin, tape), catalogue::UserSpecifiedAnEmptyStringVid);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringMediaType) {
  using namespace cta;

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape = m_tape1;
  tape.mediaType = "";
  ASSERT_THROW(m_catalogue->createTape(m_admin, tape), catalogue::UserSpecifiedAnEmptyStringMediaType);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringVendor) {
  using namespace cta;

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

   auto tape = m_tape1;
   tape.vendor = "";
   ASSERT_THROW(m_catalogue->createTape(m_admin, tape), catalogue::UserSpecifiedAnEmptyStringVendor);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringLogicalLibraryName) {
  using namespace cta;

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape = m_tape1;
  tape.logicalLibraryName = "";
  ASSERT_THROW(m_catalogue->createTape(m_admin, tape), catalogue::UserSpecifiedAnEmptyStringLogicalLibraryName);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringTapePoolName) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  auto tape = m_tape1;
  tape.tapePoolName = "";
  ASSERT_THROW(m_catalogue->createTape(m_admin, tape), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_non_existent_logical_library) {
  using namespace cta;

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  ASSERT_THROW(m_catalogue->createTape(m_admin, m_tape1), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_non_existent_tape_pool) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  ASSERT_THROW(m_catalogue->createTape(m_admin, m_tape1), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_9_exabytes_capacity) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  // The maximum size of an SQLite integer is a signed 64-bit integer
  m_catalogue->createTape(m_admin, m_tape1);

  const auto tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto &tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.state,tape.state);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const auto creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_same_twice) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, m_tape1), exception::UserError);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_StateDoesNotExist) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  auto tape = m_tape1;
  tape.state = (cta::common::dataStructures::Tape::State)42;
  ASSERT_THROW(m_catalogue->createTape(m_admin, tape),cta::catalogue::UserSpecifiedANonExistentTapeState);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_StateNotActiveWithoutReasonShouldThrow) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  auto tape = m_tape1;
  tape.state = cta::common::dataStructures::Tape::DISABLED;
  ASSERT_THROW(m_catalogue->createTape(m_admin, tape),cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

  tape.state = cta::common::dataStructures::Tape::BROKEN;
  ASSERT_THROW(m_catalogue->createTape(m_admin, tape),cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

  tape.stateReason = "Tape broken";
  ASSERT_NO_THROW(m_catalogue->createTape(m_admin, tape));
}

TEST_P(cta_catalogue_CatalogueTest, createTape_many_tapes) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  const uint64_t nbTapes = 10;

  // Effectively clone the tapes from m_tape1 but give each one its own VID
  for(uint64_t i = 1; i <= nbTapes; i++) {
    std::ostringstream vid;
    vid << "vid" << i;

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->createTape(m_admin, tape);

    {
      const auto pools = m_catalogue->getTapePools();
      ASSERT_EQ(1, pools.size());

      const auto &pool = pools.front();
      ASSERT_EQ(m_tape1.tapePoolName, pool.name);
      ASSERT_EQ(m_vo.name, pool.vo.name);
      ASSERT_EQ(i, pool.nbTapes);
      ASSERT_EQ(i * m_mediaType.capacityInBytes, pool.capacityBytes);
      ASSERT_EQ(0, pool.dataBytes);
      ASSERT_EQ(0, pool.nbPhysicalFiles);
    }
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());

    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "vid" << i;

      auto vidAndTapeItor = vidToTape.find(vid.str());
      ASSERT_NE(vidToTape.end(), vidAndTapeItor);

      const common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.state,tape.state);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "";
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.mediaType = "";
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vendor = "";
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.logicalLibrary = "";
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.tapePool = "";
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vo = "";
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.diskFileIds = std::vector<std::string>();
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.state = (cta::common::dataStructures::Tape::State)42;
    ASSERT_THROW(m_catalogue->getTapes(searchCriteria), exception::UserError);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "vid1";
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ("vid1", vidToTape.begin()->first);
    ASSERT_EQ("vid1", vidToTape.begin()->second.vid);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.mediaType = m_tape1.mediaType;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.mediaType, vidToTape.begin()->second.mediaType);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vendor = m_tape1.vendor;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.vendor, vidToTape.begin()->second.vendor);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.logicalLibrary = m_tape1.logicalLibraryName;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.logicalLibraryName, vidToTape.begin()->second.logicalLibraryName);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.tapePool = m_tape1.tapePoolName;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.tapePoolName, vidToTape.begin()->second.tapePoolName);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vo = m_vo.name;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_vo.name, vidToTape.begin()->second.vo);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.capacityInBytes = m_mediaType.capacityInBytes;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_mediaType.capacityInBytes, vidToTape.begin()->second.capacityInBytes);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.state = m_tape1.state;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.state, vidToTape.begin()->second.state);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.full = m_tape1.full;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.full, vidToTape.begin()->second.full);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "non_existent_vid";
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_TRUE(tapes.empty());
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("non_existent_fid");
    searchCriteria.diskFileIds = diskFileIds;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_TRUE(tapes.empty());
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "vid1";
    searchCriteria.logicalLibrary = m_tape1.logicalLibraryName;
    searchCriteria.tapePool = m_tape1.tapePoolName;
    searchCriteria.capacityInBytes = m_mediaType.capacityInBytes;
    searchCriteria.state = m_tape1.state;
    searchCriteria.full = m_tape1.full;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ("vid1", vidToTape.begin()->first);
    ASSERT_EQ("vid1", vidToTape.begin()->second.vid);
    ASSERT_EQ(m_tape1.logicalLibraryName, vidToTape.begin()->second.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, vidToTape.begin()->second.tapePoolName);
    ASSERT_EQ(m_mediaType.capacityInBytes, vidToTape.begin()->second.capacityInBytes);
    ASSERT_EQ(m_tape1.state, vidToTape.begin()->second.state);
    ASSERT_EQ(m_tape1.full, vidToTape.begin()->second.full);
  }

  {
    std::set<std::string> vids;
    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "vid" << i;
      vids.insert(vid.str());
    }

    const common::dataStructures::VidToTapeMap vidToTape = m_catalogue->getTapesByVid(vids);
    ASSERT_EQ(nbTapes, vidToTape.size());

    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "vid" << i;

      auto vidAndTapeItor = vidToTape.find(vid.str());
      ASSERT_NE(vidToTape.end(), vidAndTapeItor);

      const common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_1_tape_with_write_log_1_tape_without) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string diskInstance = "disk_instance";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const auto tapes = cta_catalogue_CatalogueTest::tapeListToMap(m_catalogue->getTapes());
    ASSERT_EQ(1, tapes.size());

    const auto tapeItor = tapes.find(m_tape1.vid);
    ASSERT_NE(tapes.end(), tapeItor);

    const common::dataStructures::Tape tape = tapeItor->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  const uint64_t fileSize = 1234 * 1000000000UL;
  const uint64_t archiveFileId = 1234;
  const std::string diskFileId = "5678";
  {
    auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = archiveFileId;
    file1Written.diskInstance         = diskInstance;
    file1Written.diskFileId           = diskFileId;
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(checksum::ADLER32, 0x1000); // tests checksum with embedded zeros
    file1Written.storageClassName     = m_storageClassSingleCopy.name;
    file1Written.vid                  = m_tape1.vid;
    file1Written.fSeq                 = 1;
    file1Written.blockId              = 4321;
    file1Written.copyNb               = 1;
    file1Written.tapeDrive            = "tape_drive";
    m_catalogue->filesWrittenToTape(file1WrittenSet);
  }

  {
    // Check that a lookup of diskFileId 5678 returns 1 tape
    catalogue::TapeSearchCriteria searchCriteria;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("5678");
    searchCriteria.diskFileIds = diskFileIds;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ(m_tape1.vid, vidToTape.begin()->first);
    ASSERT_EQ(m_tape1.vid, vidToTape.begin()->second.vid);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(fileSize, pool.dataBytes);
    ASSERT_EQ(1, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, m_tape2);

  {
    const auto tapes = cta_catalogue_CatalogueTest::tapeListToMap(m_catalogue->getTapes());
    ASSERT_EQ(2, tapes.size());

    const auto tapeItor = tapes.find(m_tape2.vid);
    ASSERT_NE(tapes.end(), tapeItor);

    const common::dataStructures::Tape tape = tapeItor->second;
    ASSERT_EQ(m_tape2.vid, tape.vid);
    ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape2.vendor, tape.vendor);
    ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape2.state, tape.state);
    ASSERT_EQ(m_tape2.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape2.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(2, pool.nbTapes);
    ASSERT_EQ(2*m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(fileSize, pool.dataBytes);
    ASSERT_EQ(1, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, deleteTape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);
  ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
  ASSERT_EQ(m_tape1.vendor, tape.vendor);
  ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
  ASSERT_EQ(m_vo.name, tape.vo);
  ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(m_tape1.full, tape.full);

  ASSERT_FALSE(tape.isFromCastor);
  ASSERT_EQ(m_tape1.comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteTape(tape.vid);
  ASSERT_TRUE(m_catalogue->getTapes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteNonEmptyTape) {
  using namespace cta;

  log::LogContext dummyLc(m_dummyLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t fileSize = 1234 * 1000000000UL;
  const uint64_t archiveFileId = 1234;
  const std::string diskFileId = "5678";
  {
    auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = archiveFileId;
    file1Written.diskInstance         = diskInstance;
    file1Written.diskFileId           = diskFileId;
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
    file1Written.storageClassName     = m_storageClassSingleCopy.name;
    file1Written.vid                  = m_tape1.vid;
    file1Written.fSeq                 = 1;
    file1Written.blockId              = 4321;
    file1Written.copyNb               = 1;
    file1Written.tapeDrive            = "tape_drive";
    m_catalogue->filesWrittenToTape(file1WrittenSet);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(fileSize, tape.dataOnTapeInBytes);
    ASSERT_EQ(fileSize, tape.masterDataInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->deleteTape(m_tape1.vid), catalogue::UserSpecifiedANonEmptyTape);
  ASSERT_FALSE(m_catalogue->getTapes().empty());

  //Put the files on the tape on the recycle log
  cta::common::dataStructures::DeleteArchiveRequest deletedArchiveReq;
  deletedArchiveReq.archiveFile = m_catalogue->getArchiveFileById(archiveFileId);
  deletedArchiveReq.diskInstance = diskInstance;
  deletedArchiveReq.archiveFileID = archiveFileId;
  deletedArchiveReq.diskFileId = diskFileId;
  deletedArchiveReq.recycleTime = time(nullptr);
  deletedArchiveReq.requester = cta::common::dataStructures::RequesterIdentity(m_admin.username,"group");
  deletedArchiveReq.diskFilePath = "/path/";
  m_catalogue->moveArchiveFileToRecycleLog(deletedArchiveReq,dummyLc);

  //The ArchiveFilesItor should not have any file in it
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  //The tape should not be deleted
  ASSERT_THROW(m_catalogue->deleteTape(m_tape1.vid), catalogue::UserSpecifiedANonEmptyTape);
  ASSERT_FALSE(m_catalogue->getTapes().empty());
  m_catalogue->setTapeFull(m_admin,m_tape1.vid,true);
  //Reclaim it to delete the files from the recycle log
  m_catalogue->reclaimTape(m_admin,m_tape1.vid,dummyLc);
  //Deletion should be successful
  ASSERT_NO_THROW(m_catalogue->deleteTape(m_tape1.vid));
  ASSERT_TRUE(m_catalogue->getTapes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteTape_non_existent) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->deleteTape("non_existent_tape"), catalogue::UserSpecifiedANonExistentTape);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeMediaType) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);

  auto anotherMediaType = m_mediaType;
  anotherMediaType.name = "another_media_type";

  m_catalogue->createMediaType(m_admin, anotherMediaType);

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeMediaType(m_admin, m_tape1.vid, anotherMediaType.name);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(anotherMediaType.name, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  ASSERT_THROW(m_catalogue->modifyTapeMediaType(m_admin, m_tape1.vid, "DOES NOT EXIST"),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeVendor) {
  using namespace cta;

  const std::string anotherVendor = "another_vendor";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeVendor(m_admin, m_tape1.vid, anotherVendor);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(anotherVendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeLogicalLibraryName) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string anotherLogicalLibraryName = "another_logical_library_name";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createLogicalLibrary(m_admin, anotherLogicalLibraryName, logicalLibraryIsDisabled,
    "Create another logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeLogicalLibraryName(m_admin, m_tape1.vid, anotherLogicalLibraryName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(anotherLogicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeLogicalLibraryName_nonExistentTape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  ASSERT_THROW(m_catalogue->modifyTapeLogicalLibraryName(m_admin, m_tape1.vid, m_tape1.logicalLibraryName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeTapePoolName) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string anotherTapePoolName = "another_tape_pool_name";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, anotherTapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create another tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeTapePoolName(m_admin, m_tape1.vid, anotherTapePoolName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(anotherTapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeTapePoolName_nonExistentTape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  ASSERT_THROW(m_catalogue->modifyTapeTapePoolName(m_admin, m_tape1.vid, m_tape1.tapePoolName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeEncryptionKeyName) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedEncryptionKeyName = "modified_encryption_key_name";
  m_catalogue->modifyTapeEncryptionKeyName(m_admin, m_tape1.vid, modifiedEncryptionKeyName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(modifiedEncryptionKeyName, tape.encryptionKeyName);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeEncryptionKeyName_emptyStringEncryptionKey) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedEncryptionKeyName;
  m_catalogue->modifyTapeEncryptionKeyName(m_admin, m_tape1.vid, modifiedEncryptionKeyName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_FALSE((bool)tape.encryptionKeyName);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeEncryptionKeyName_nonExistentTape) {
  using namespace cta;

  const std::string encryptionKeyName = "encryption_key_name";

  ASSERT_THROW(m_catalogue->modifyTapeEncryptionKeyName(m_admin, m_tape1.vid, encryptionKeyName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeState_nonExistentTape) {
  using namespace cta;

  common::dataStructures::Tape::State state = common::dataStructures::Tape::State::ACTIVE;
  ASSERT_THROW(m_catalogue->modifyTapeState(m_admin,"DOES_NOT_EXIST",state,cta::nullopt),cta::catalogue::UserSpecifiedANonExistentTape);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDisabled_nonExistentTape) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->setTapeDisabled(m_admin, m_tape1.vid, "Test"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeState_nonExistentState) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  common::dataStructures::Tape::State state = (common::dataStructures::Tape::State)42;
  ASSERT_THROW(m_catalogue->modifyTapeState(m_admin, m_tape1.vid,state,cta::nullopt),cta::catalogue::UserSpecifiedANonExistentTapeState);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeState_noReasonWhenNotActive) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  std::string reason = "";
  ASSERT_THROW(m_catalogue->modifyTapeState(m_admin,m_tape1.vid,common::dataStructures::Tape::State::BROKEN,reason),cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

  ASSERT_THROW(m_catalogue->modifyTapeState(m_admin,m_tape1.vid,common::dataStructures::Tape::State::DISABLED,cta::nullopt),cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeState) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  std::string reason = "tape broken";

  std::string vid = m_tape1.vid;
  ASSERT_NO_THROW(m_catalogue->modifyTapeState(m_admin,vid,common::dataStructures::Tape::State::BROKEN,reason));

  {
    //catalogue getTapesByVid test
    auto vidToTapeMap = m_catalogue->getTapesByVid({vid});
    auto tape = vidToTapeMap.at(vid);
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(common::dataStructures::Tape::BROKEN,tape.state);
    ASSERT_EQ(reason,tape.stateReason);
    ASSERT_EQ(catalogue::RdbmsCatalogue::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }

  {
    //Get tape by search criteria test
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.vid = vid;
    auto tapes = m_catalogue->getTapes(criteria);
    auto tape = tapes.front();
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(common::dataStructures::Tape::BROKEN,tape.state);
    ASSERT_EQ(reason,tape.stateReason);
    ASSERT_EQ(catalogue::RdbmsCatalogue::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }

  {
    auto vidToTapeMap = m_catalogue->getTapesByVid({vid});
    auto tape = vidToTapeMap.at(vid);
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(common::dataStructures::Tape::BROKEN,tape.state);
    ASSERT_EQ(reason,tape.stateReason);
    ASSERT_EQ(catalogue::RdbmsCatalogue::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeStateResetReasonWhenBackToActiveState) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  std::string vid = m_tape1.vid;

  std::string reason = "Broken tape";
  ASSERT_NO_THROW(m_catalogue->modifyTapeState(m_admin,vid,common::dataStructures::Tape::State::BROKEN,reason));

  ASSERT_NO_THROW(m_catalogue->modifyTapeState(m_admin,vid,common::dataStructures::Tape::State::ACTIVE,cta::nullopt));

  {
    auto vidToTapeMap = m_catalogue->getTapesByVid({vid});
    auto tape = vidToTapeMap.at(vid);
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(common::dataStructures::Tape::ACTIVE,tape.state);
    ASSERT_FALSE(tape.stateReason);
    ASSERT_EQ(catalogue::RdbmsCatalogue::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getTapesSearchCriteriaByState) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);

  std::string vidTape1 = m_tape1.vid;
  std::string vidTape2 = m_tape2.vid;

  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.state = common::dataStructures::Tape::ACTIVE;
    auto tapes = m_catalogue->getTapes(criteria);
    ASSERT_EQ(2,tapes.size());
    auto tape = tapes.front();
    ASSERT_EQ(vidTape1,tape.vid);
    ASSERT_EQ(common::dataStructures::Tape::ACTIVE,tape.state);
    ASSERT_FALSE(tape.stateReason);
    ASSERT_EQ(m_admin.username + "@" + m_admin.host,tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }

  std::string reason = "Broken tape";
  ASSERT_NO_THROW(m_catalogue->modifyTapeState(m_admin,vidTape1,common::dataStructures::Tape::State::BROKEN,reason));

  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.state = common::dataStructures::Tape::ACTIVE;
    auto tapes = m_catalogue->getTapes(criteria);
    ASSERT_EQ(1,tapes.size());
    auto tape = tapes.front();
    //The tape 2 is ACTIVE so this is the one we expect
    ASSERT_EQ(vidTape2,tape.vid);
  }
  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.state = common::dataStructures::Tape::BROKEN;
    auto tapes = m_catalogue->getTapes(criteria);
    ASSERT_EQ(1,tapes.size());
    auto tape = tapes.front();
    //The tape 2 is ACTIVE so this is the one we expect
    ASSERT_EQ(vidTape1,tape.vid);
    ASSERT_EQ(common::dataStructures::Tape::BROKEN,tape.state);
  }
}

TEST_P(cta_catalogue_CatalogueTest, tapeLabelled) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string labelDrive = "labelling_drive";
  m_catalogue->tapeLabelled(m_tape1.vid, labelDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_TRUE((bool)tape.labelLog);
    ASSERT_EQ(labelDrive, tape.labelLog.value().drive);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, tapeLabelled_nonExistentTape) {
  using namespace cta;

  const std::string labelDrive = "drive";

  ASSERT_THROW(m_catalogue->tapeLabelled(m_tape1.vid, labelDrive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForArchive) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedDrive = "modified_drive";
  m_catalogue->tapeMountedForArchive(m_tape1.vid, modifiedDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(1, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(modifiedDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  for(int i=1; i<1024; i++) {
    m_catalogue->tapeMountedForArchive(m_tape1.vid, modifiedDrive);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(1024, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(modifiedDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForArchive_nonExistentTape) {
  using namespace cta;

  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->tapeMountedForArchive(m_tape1.vid, drive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForRetrieve) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedDrive = "modified_drive";
  m_catalogue->tapeMountedForRetrieve(m_tape1.vid, modifiedDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(1, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_TRUE((bool)tape.lastReadLog);
    ASSERT_EQ(modifiedDrive, tape.lastReadLog.value().drive);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  for(int i=1; i<1024; i++) {
    m_catalogue->tapeMountedForRetrieve(m_tape1.vid, modifiedDrive);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(1024, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_TRUE((bool)tape.lastReadLog);
    ASSERT_EQ(modifiedDrive, tape.lastReadLog.value().drive);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForRetrieve_nonExistentTape) {
  using namespace cta;

  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->tapeMountedForRetrieve(m_tape1.vid, drive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeFull) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeFull(m_admin, m_tape1.vid, true);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeFull_nonExistentTape) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->setTapeFull(m_admin, m_tape1.vid, true), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDirty) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_TRUE(tape.dirty);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeDirty(m_admin, m_tape1.vid, false);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_FALSE(tape.dirty);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDirty_nonExistentTape) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->setTapeDirty(m_admin, m_tape1.vid, true), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, noSpaceLeftOnTape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->noSpaceLeftOnTape(m_tape1.vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, noSpaceLeftOnTape_nonExistentTape) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->noSpaceLeftOnTape(m_tape1.vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeIsFromCastorInUnitTests) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeIsFromCastorInUnitTests(m_tape1.vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_TRUE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  // do it twice
  m_catalogue->setTapeIsFromCastorInUnitTests(m_tape1.vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_TRUE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeIsFromCastor_nonExistentTape) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->setTapeIsFromCastorInUnitTests(m_tape1.vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  m_catalogue->tapeLabelled(m_tape1.vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_EQ(1, tapes.size());

  const catalogue::TapeForWriting tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);
  ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
  ASSERT_EQ(m_tape1.vendor, tape.vendor);
  ASSERT_EQ(m_tape1.tapePoolName, tape.tapePool);
  ASSERT_EQ(m_vo.name, tape.vo);
  ASSERT_EQ(0, tape.lastFSeq);
  ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(0, tape.dataOnTapeInBytes);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWritingOrderedByDataInBytesDesc) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);


  m_catalogue->tapeLabelled(m_tape1.vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_EQ(1, tapes.size());

  const catalogue::TapeForWriting tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);
  ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
  ASSERT_EQ(m_tape1.vendor, tape.vendor);
  ASSERT_EQ(m_tape1.tapePoolName, tape.tapePool);
  ASSERT_EQ(m_vo.name, tape.vo);
  ASSERT_EQ(0, tape.lastFSeq);
  ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(0, tape.dataOnTapeInBytes);

  //Create a tape and insert a file in it
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);
  m_catalogue->createTape(m_admin, m_tape2);
  m_catalogue->tapeLabelled(m_tape2.vid, "tape_drive");

  const uint64_t fileSize = 1234 * 1000000000UL;
  {
    auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = 1234;
    file1Written.diskInstance         = "diskInstance";
    file1Written.diskFileId           = "5678";
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(checksum::ADLER32, 0x1000); // tests checksum with embedded zeros
    file1Written.storageClassName     = m_storageClassSingleCopy.name;
    file1Written.vid                  = m_tape2.vid;
    file1Written.fSeq                 = 1;
    file1Written.blockId              = 4321;
    file1Written.copyNb               = 1;
    file1Written.tapeDrive            = "tape_drive";
    m_catalogue->filesWrittenToTape(file1WrittenSet);
  }

  //The tape m_tape2 should be returned by the Catalogue::getTapesForWriting() method
  ASSERT_EQ(m_tape2.vid,m_catalogue->getTapesForWriting(m_tape2.logicalLibraryName).front().vid);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting_disabled_tape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled = false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  auto tape = m_tape1;
  tape.state = common::dataStructures::Tape::DISABLED;
  tape.stateReason = "test";
  m_catalogue->createTape(m_admin, tape);

  m_catalogue->tapeLabelled(m_tape1.vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_EQ(0, tapes.size());
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting_full_tape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  auto tape1 = m_tape1;
  tape1.full = true;

  m_catalogue->createMediaType(m_admin, m_mediaType);

  m_catalogue->createLogicalLibrary(m_admin, tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, tape1);

  m_catalogue->tapeLabelled(tape1.vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(tape1.logicalLibraryName);

  ASSERT_EQ(0, tapes.size());
}

TEST_P(cta_catalogue_CatalogueTest, DISABLED_getTapesForWriting_no_labelled_tapes) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_TRUE(tapes.empty());
}

TEST_P(cta_catalogue_CatalogueTest, createMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  catalogue::CreateMountPolicyAttributes mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin, mountPolicyToAdd);

  const std::list<common::dataStructures::MountPolicy> mountPolicies =
    m_catalogue->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

  ASSERT_EQ(mountPolicyName, mountPolicy.name);

  ASSERT_EQ(mountPolicyToAdd.archivePriority, mountPolicy.archivePriority);
  ASSERT_EQ(mountPolicyToAdd.minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

  ASSERT_EQ(mountPolicyToAdd.retrievePriority, mountPolicy.retrievePriority);
  ASSERT_EQ(mountPolicyToAdd.minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

  ASSERT_EQ(mountPolicyToAdd.comment, mountPolicy.comment);

  const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    mountPolicy.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createMountPolicy_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  auto mountPolicy = getMountPolicy1();

  m_catalogue->createMountPolicy(m_admin,mountPolicy);

  ASSERT_THROW(m_catalogue->createMountPolicy(m_admin, mountPolicy),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  m_catalogue->deleteMountPolicy(mountPolicyName);

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteMountPolicy_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());
  ASSERT_THROW(m_catalogue->deleteMountPolicy("non_existent_mount_policy"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchivePriority) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedArchivePriority = mountPolicyToAdd.archivePriority + 10;
  m_catalogue->modifyMountPolicyArchivePriority(m_admin, mountPolicyToAdd.name, modifiedArchivePriority);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedArchivePriority, mountPolicy.archivePriority);

    const common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchivePriority_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;

  ASSERT_THROW(m_catalogue->modifyMountPolicyArchivePriority(m_admin, name, archivePriority), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchiveMinRequestAge) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  auto mountPolicyToAdd = getMountPolicy1();

  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedMinArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge + 10;
  m_catalogue->modifyMountPolicyArchiveMinRequestAge(m_admin, mountPolicyToAdd.name, modifiedMinArchiveRequestAge);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedMinArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    const common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchiveMinRequestAge_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minArchiveRequestAge = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyArchiveMinRequestAge(m_admin, name, minArchiveRequestAge), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrievePriority) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedRetrievePriority = mountPolicyToAdd.retrievePriority + 10;
  m_catalogue->modifyMountPolicyRetrievePriority(m_admin, mountPolicyToAdd.name, modifiedRetrievePriority);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedRetrievePriority, mountPolicy.retrievePriority);

    const common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrievePriority_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t retrievePriority = 1;

  ASSERT_THROW(m_catalogue->modifyMountPolicyRetrievePriority(m_admin, name, retrievePriority), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrieveMinRequestAge) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedMinRetrieveRequestAge = mountPolicyToAdd.minRetrieveRequestAge + 10;
  m_catalogue->modifyMountPolicyRetrieveMinRequestAge(m_admin, mountPolicyToAdd.name, modifiedMinRetrieveRequestAge);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedMinRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    const common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrieveMinRequestAge_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minRetrieveRequestAge = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyRetrieveMinRequestAge(m_admin, name, minRetrieveRequestAge), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyMountPolicyComment(m_admin, mountPolicyToAdd.name, modifiedComment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedComment, mountPolicy.comment);

    const common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyComment_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const std::string comment = "Comment";

  ASSERT_THROW(m_catalogue->modifyMountPolicyComment(m_admin, name, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterActivityMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, activityRegex, comment);

  const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterActivityMountRule rule = rules.front();

  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(activityRegex, rule.activityRegex);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterActivityMountRule_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, activityRegex, comment);

  ASSERT_THROW(m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, activityRegex, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterActivityMountRule_non_existent_mount_policy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());

  const std::string comment = "Create mount rule for requester";
  const std::string mountPolicyName = "non_existent_mount_policy";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";

  ASSERT_THROW( m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    activityRegex, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterActivityMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, activityRegex, comment);

  const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
  ASSERT_EQ(1, rules.size());

  m_catalogue->deleteRequesterActivityMountRule(diskInstanceName, requesterName, activityRegex);
  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterActivityMountRule_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());
  ASSERT_THROW(m_catalogue->deleteRequesterActivityMountRule("non_existent_disk_instance", "non_existent_requester", "non_existrnt_activity"),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterActivityMountRulePolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string anotherMountPolicyName = "another_mount_policy";

  auto anotherMountPolicy = getMountPolicy1();
  anotherMountPolicy.name = anotherMountPolicyName;
  m_catalogue->createMountPolicy(m_admin,anotherMountPolicy);


  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity";
  m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, activityRegex, comment);

  {
    const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  m_catalogue->modifyRequesterActivityMountRulePolicy(m_admin, diskInstanceName, requesterName, activityRegex, anotherMountPolicyName);

  {
    const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(anotherMountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterActivityMountRulePolicy_nonExistentRequesterActivity) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity";

  ASSERT_THROW(m_catalogue->modifyRequesterActivityMountRulePolicy(m_admin, diskInstanceName, requesterName, activityRegex, mountPolicyName),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterActivityMountRuleComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity";
  m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, activityRegex, comment);

  {
    const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyRequesterActivityMountRuleComment(m_admin, diskInstanceName, requesterName, activityRegex, modifiedComment);

  {
    const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(modifiedComment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterMountRuleComment_nonExistentRequesterActivity) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterActivityMountRules().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string comment = "Comment";
  const std::string activityRegex = "activity";
  ASSERT_THROW(m_catalogue->modifyRequesterActivityMountRuleComment(m_admin, diskInstanceName, requesterName, activityRegex, comment),
    exception::UserError);
}


TEST_P(cta_catalogue_CatalogueTest, createRequesterMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterMountRule_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);
  ASSERT_THROW(m_catalogue->createRequesterMountRule(m_admin, mountPolicyToAdd.name, diskInstanceName, requesterName,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterMountRule_non_existent_mount_policy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string comment = "Create mount rule for requester";
  const std::string mountPolicyName = "non_existent_mount_policy";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  ASSERT_THROW(m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  m_catalogue->deleteRequesterMountRule(diskInstanceName, requesterName);
  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterMountRule_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());
  ASSERT_THROW(m_catalogue->deleteRequesterMountRule("non_existent_disk_instance", "non_existent_requester"),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterMountRulePolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string anotherMountPolicyName = "another_mount_policy";

  auto anotherMountPolicy = getMountPolicy1();
  anotherMountPolicy.name = anotherMountPolicyName;
  m_catalogue->createMountPolicy(m_admin,anotherMountPolicy);


  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  m_catalogue->modifyRequesterMountRulePolicy(m_admin, diskInstanceName, requesterName, anotherMountPolicyName);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(anotherMountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterMountRulePolicy_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";

  ASSERT_THROW(m_catalogue->modifyRequesterMountRulePolicy(m_admin, diskInstanceName, requesterName, mountPolicyName),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesteMountRuleComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyRequesteMountRuleComment(m_admin, diskInstanceName, requesterName, modifiedComment);

  {
    const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(modifiedComment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesteMountRuleComment_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string comment = "Comment";

  ASSERT_THROW(m_catalogue->modifyRequesteMountRuleComment(m_admin, diskInstanceName, requesterName, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRulePolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  std::string mountPolicyName = mountPolicyToAdd.name;

  const std::string anotherMountPolicyName = "another_mount_policy";

  auto anotherMountPolicy = getMountPolicy1();
  anotherMountPolicy.name = anotherMountPolicyName;
  m_catalogue->createMountPolicy(m_admin,anotherMountPolicy);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  m_catalogue->modifyRequesterGroupMountRulePolicy(m_admin, diskInstanceName, requesterGroupName, anotherMountPolicyName);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(anotherMountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRulePolicy_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";

  ASSERT_THROW(m_catalogue->modifyRequesterGroupMountRulePolicy(m_admin, diskInstanceName, requesterGroupName,
    mountPolicyName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRuleComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  }

  const std::string modifiedComment = "ModifiedComment";
  m_catalogue->modifyRequesterGroupMountRuleComment(m_admin, diskInstanceName, requesterGroupName, modifiedComment);

  {
    const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(modifiedComment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRuleComment_nonExistentRequester) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";
  const std::string comment  = "Comment";

  ASSERT_THROW(m_catalogue->modifyRequesterGroupMountRuleComment(m_admin, diskInstanceName, requesterGroupName,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterGroupMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules =
    m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterGroupMountRule_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);
  ASSERT_THROW(m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterGroupName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterGroupMountRule_non_existent_mount_policy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string comment = "Create mount rule for requester group";
  const std::string mountPolicyName = "non_existent_mount_policy";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  ASSERT_THROW(m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterGroupName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterGroupMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);
  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteRequesterGroupMountRule_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());
  ASSERT_THROW(m_catalogue->deleteRequesterGroupMountRule("non_existent_disk_isntance", "non_existent_requester_group"),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_no_archive_routes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);

  const std::string diskInstance = "disk_instance";
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->checkAndGetNextArchiveFileId(diskInstance, m_storageClassSingleCopy.name, requesterIdentity), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_no_mount_rules) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string diskInstance = "disk_instance";

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  const std::string requesterName = "requester_name";
  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->checkAndGetNextArchiveFileId(diskInstance, m_storageClassSingleCopy.name, requesterIdentity), exception::UserErrorWithCacheInfo);
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_after_cached_and_then_deleted_requester_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  // Get an archive ID which should pouplate for the first time the user mount
  // rule cache
  m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);

  // Delete the user mount rule which should immediately invalidate the user
  // mount rule cache
  m_catalogue->deleteRequesterMountRule(diskInstanceName, requesterName);

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  // Try to get an archive ID which should now fail because there is no user
  // mount rule and the invalidated user mount rule cache should not hide this
  // fact
  ASSERT_THROW(m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity), exception::UserErrorWithCacheInfo);
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_requester_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  std::set<uint64_t> archiveFileIds;
  for(uint64_t i = 0; i<10; i++) {
    const uint64_t archiveFileId =
      m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_requester_group_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = "username";
  requesterIdentity.group = requesterGroupName;

  std::set<uint64_t> archiveFileIds;
  for(uint64_t i = 0; i<10; i++) {
    const uint64_t archiveFileId = m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_after_cached_and_then_deleted_requester_group_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = "username";
  requesterIdentity.group = requesterGroupName;

  // Get an archive ID which should populate for the first time the group mount
  // rule cache
  m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);

  // Delete the group mount rule which should immediately invalidate the group
  // mount rule cache
  m_catalogue->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  // Try to get an archive ID which should now fail because there is no group
  // mount rule and the invalidated group mount rule cache should not hide this
  // fact
  ASSERT_THROW(m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity), exception::UserErrorWithCacheInfo);
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_requester_mount_rule_overide) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string requesterRuleComment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    requesterRuleComment);

  const std::list<common::dataStructures::RequesterMountRule> requesterRules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, requesterRules.size());

  const common::dataStructures::RequesterMountRule requesterRule = requesterRules.front();

  ASSERT_EQ(requesterName, requesterRule.name);
  ASSERT_EQ(mountPolicyName, requesterRule.mountPolicy);
  ASSERT_EQ(requesterRuleComment, requesterRule.comment);
  ASSERT_EQ(m_admin.username, requesterRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterRule.creationLog.host);
  ASSERT_EQ(requesterRule.creationLog, requesterRule.lastModificationLog);

  const std::string requesterGroupRuleComment = "Create mount rule for requester group";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    requesterGroupRuleComment);

  const std::list<common::dataStructures::RequesterGroupMountRule> requesterGroupRules =
    m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, requesterGroupRules.size());

  const common::dataStructures::RequesterGroupMountRule requesterGroupRule = requesterGroupRules.front();

  ASSERT_EQ(requesterName, requesterGroupRule.name);
  ASSERT_EQ(mountPolicyName, requesterGroupRule.mountPolicy);
  ASSERT_EQ(requesterGroupRuleComment, requesterGroupRule.comment);
  ASSERT_EQ(m_admin.username, requesterGroupRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterGroupRule.creationLog.host);
  ASSERT_EQ(requesterGroupRule.creationLog, requesterGroupRule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  std::set<uint64_t> archiveFileIds;
  for(uint64_t i = 0; i<10; i++) {
    const uint64_t archiveFileId = m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_no_archive_routes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_requester_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  m_catalogue->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_requester_group_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);

  const std::list<common::dataStructures::RequesterGroupMountRule> rules = m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = "username";
  requesterIdentity.group = requesterGroupName;
  m_catalogue->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_requester_mount_rule_overide) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string requesterRuleComment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance_name";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    requesterRuleComment);

  const std::list<common::dataStructures::RequesterMountRule> requesterRules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, requesterRules.size());

  const common::dataStructures::RequesterMountRule requesterRule = requesterRules.front();

  ASSERT_EQ(requesterName, requesterRule.name);
  ASSERT_EQ(mountPolicyName, requesterRule.mountPolicy);
  ASSERT_EQ(requesterRuleComment, requesterRule.comment);
  ASSERT_EQ(m_admin.username, requesterRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterRule.creationLog.host);
  ASSERT_EQ(requesterRule.creationLog, requesterRule.lastModificationLog);

  const std::string requesterGroupRuleComment = "Create mount rule for requester group";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    requesterGroupRuleComment);

  const std::list<common::dataStructures::RequesterGroupMountRule> requesterGroupRules =
    m_catalogue->getRequesterGroupMountRules();
  ASSERT_EQ(1, requesterGroupRules.size());

  const common::dataStructures::RequesterGroupMountRule requesterGroupRule = requesterGroupRules.front();

  ASSERT_EQ(requesterName, requesterGroupRule.name);
  ASSERT_EQ(mountPolicyName, requesterGroupRule.mountPolicy);
  ASSERT_EQ(requesterGroupRuleComment, requesterGroupRule.comment);
  ASSERT_EQ(m_admin.username, requesterGroupRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterGroupRule.creationLog.host);
  ASSERT_EQ(requesterGroupRule.creationLog, requesterGroupRule.lastModificationLog);

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  m_catalogue->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);
}

TEST_P(cta_catalogue_CatalogueTest, prepareToRetrieveFileUsingArchiveFileId) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";
  const std::string diskInstanceName2 = "disk_instance_2";

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
  const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
  {
    auto it = vidToTape.find(m_tape1.vid);
    ASSERT_TRUE(it != vidToTape.end());
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  {
    auto it = vidToTape.find(m_tape2.vid);
    ASSERT_TRUE(it != vidToTape.end());
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape2.vid, tape.vid);
    ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape2.vendor, tape.vendor);
    ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape2.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape2.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;


  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  uint64_t minArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge;
  uint64_t archivePriority = mountPolicyToAdd.archivePriority;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName1, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName1, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  log::LogContext dummyLc(m_dummyLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc);

  ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());
  ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

  // Check that the diskInstanceName mismatch detection works
  ASSERT_THROW(m_catalogue->prepareToRetrieveFile(diskInstanceName2, archiveFileId, requesterIdentity, cta::nullopt, dummyLc),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, prepareToRetrieveFileUsingArchiveFileId_disabledTapes) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  std::string disabledReason = "disabledReason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
  const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
  {
    auto it = vidToTape.find(m_tape1.vid);
    ASSERT_TRUE(it != vidToTape.end());
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  {
    auto it = vidToTape.find(m_tape2.vid);
    ASSERT_TRUE(it != vidToTape.end());
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape2.vid, tape.vid);
    ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape2.vendor, tape.vendor);
    ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape2.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape2.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);

    const auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  uint64_t minArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge;
  uint64_t archivePriority = mountPolicyToAdd.archivePriority;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName1, requesterName, comment);

  const std::list<common::dataStructures::RequesterMountRule> rules = m_catalogue->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName1, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  log::LogContext dummyLc(m_dummyLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  {
    const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = queueCriteria.archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, queueCriteria.archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, queueCriteria.archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  m_catalogue->setTapeDisabled(m_admin, m_tape1.vid, disabledReason);

  {
    const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(1, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, queueCriteria.archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  m_catalogue->setTapeDisabled(m_admin, m_tape2.vid, disabledReason);

  ASSERT_THROW(m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, prepareToRetrieveFileUsingArchiveFileId_returnNonSupersededFiles) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
  const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  //Create a superseder file
  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 1;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  ASSERT_TRUE(m_catalogue->getFileRecycleLogItor().hasMore());

  auto mountPolicyToAdd = getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  uint64_t minArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge;
  uint64_t archivePriority = mountPolicyToAdd.archivePriority;
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName1, requesterName, comment);

  log::LogContext dummyLc(m_dummyLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  {
    const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(1, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = queueCriteria.archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, queueCriteria.archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file2Written.vid, tapeFile1.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile1.copyNb);
  }

  std::string disabledReason = "disabled reason";
  m_catalogue->setTapeDisabled(m_admin, m_tape2.vid, disabledReason);

  ASSERT_THROW(m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, prepareToRetrieveFileUsingArchiveFileId_ActivityMountPolicy) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";
  const std::string diskInstanceName2 = "disk_instance_2";

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
  const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
  {
    auto it = vidToTape.find(m_tape1.vid);
    ASSERT_TRUE(it != vidToTape.end());
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  {
    auto it = vidToTape.find(m_tape2.vid);
    ASSERT_TRUE(it != vidToTape.end());
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape2.vid, tape.vid);
    ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape2.vendor, tape.vendor);
    ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape2.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape2.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;


  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  auto mountPolicyToAdd1 = getMountPolicy1();
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd1);
  auto mountPolicyToAdd2 = getMountPolicy2();
  m_catalogue->createMountPolicy(m_admin,mountPolicyToAdd2);

    const std::string comment = "Create mount rule for requester+activity";
    const std::string requesterName = "requester_name";
    const std::string activityRegex = "^activity_[a-zA-Z0-9-]+$";
    m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyToAdd1.name, diskInstanceName1, requesterName, activityRegex, comment);


    const std::string secondActivityRegex = "^activity_specific$";
    m_catalogue->createRequesterActivityMountRule(m_admin, mountPolicyToAdd2.name, diskInstanceName1, requesterName, secondActivityRegex, comment);


  {
    const std::list<common::dataStructures::RequesterActivityMountRule> rules = m_catalogue->getRequesterActivityMountRules();
    ASSERT_EQ(2, rules.size());
  }

  log::LogContext dummyLc(m_dummyLog);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  cta::optional<std::string> requestActivity = std::string("activity_retrieve");

  {
    const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, requestActivity, dummyLc);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());
    ASSERT_EQ(mountPolicyToAdd1.archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(mountPolicyToAdd1.minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
  }

  // Check that multiple matching policies returns the highest priority one for retrieve
  requestActivity = std::string("activity_specific");
  {
    const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, requestActivity, dummyLc);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());
    ASSERT_EQ(mountPolicyToAdd2.archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(mountPolicyToAdd2.minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
  }


  // Check that no matching activity detection works
  requestActivity = std::string("no_matching_activity");
  ASSERT_THROW(m_catalogue->prepareToRetrieveFile(diskInstanceName2, archiveFileId, requesterIdentity, requestActivity, dummyLc),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existance_archiveFileId) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.archiveFileId = 1234;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_fSeq_without_vid) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.fSeq = 1234;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_disk_file_id_without_instance) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  std::vector<std::string> diskFileIds;
  diskFileIds.push_back("disk_file_id");
  searchCriteria.diskFileIds = diskFileIds;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_existent_storage_class_without_disk_instance) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  {
    const auto s = storageClasses.front();

    ASSERT_EQ(m_storageClassSingleCopy.name, s.name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, s.nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, s.comment);

    const common::dataStructures::EntryLog creationLog = s.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = s.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existent_vid) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.vid = "non_existent_vid";

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_many_archive_files) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  m_catalogue->createTape(m_admin, tape1);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(1, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;
  m_catalogue->createTape(m_admin, tape2);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(1, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    const auto tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const auto vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(tape1.vid, tape.vid);
      ASSERT_EQ(tape1.mediaType, tape.mediaType);
      ASSERT_EQ(tape1.vendor, tape.vendor);
      ASSERT_EQ(tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName1, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      const auto &tape = it->second;
      ASSERT_EQ(tape2.vid, tape.vid);
      ASSERT_EQ(tape2.mediaType, tape.mediaType);
      ASSERT_EQ(tape2.vendor, tape.vendor);
      ASSERT_EQ(tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const auto creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const auto lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  const std::string tapeDrive = "tape_drive";

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    // Tape copy 1 written to tape
    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();

    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
  }
  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(0, it->second.lastFSeq);
    }
  }

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy2;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    // Tape copy 2 written to tape
    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();

    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy2.emplace(fileWrittenUP.release());
  }
  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy2);
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345678");
    searchCriteria.diskFileIds = diskFileIds;
    searchCriteria.vid = tape1.vid;

    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    const auto idAndFile = m.find(1);
    ASSERT_NE(m.end(), idAndFile);
    const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
    ASSERT_EQ(searchCriteria.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(searchCriteria.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(searchCriteria.diskFileIds->front(), archiveFile.diskFileId);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    ASSERT_EQ(searchCriteria.vid, archiveFile.tapeFiles.begin()->vid);
  }

  {
    auto archiveFileItor = m_catalogue->getArchiveFilesItor();
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(m_storageClassDualCopy.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  // Look at all files on tape 1
  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }
    }
  }

  // Look at all files on tape 1
  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }
    }
  }

  // Look at all files on tape 2
  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape2.vid;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten2;
      fileWritten2.archiveFileId = i;
      fileWritten2.diskInstance = diskInstance;
      fileWritten2.diskFileId = diskFileId.str();

      fileWritten2.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten2.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten2.size = archiveFileSize;
      fileWritten2.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten2.storageClassName = m_storageClassDualCopy.name;
      fileWritten2.vid = tape2.vid;
      fileWritten2.fSeq = i;
      fileWritten2.blockId = i * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten2.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten2.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten2.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten2.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten2.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten2.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten2.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten2.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  {
    const uint64_t startFseq = 1;
    auto archiveFileItor = m_catalogue->getArchiveFilesForRepackItor(tape1.vid, startFseq);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid     = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid     = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(m_storageClassDualCopy.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  for(uint32_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles;
    const auto archiveFiles = m_catalogue->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = m_storageClassDualCopy.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint32_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles / 2; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = m_storageClassDualCopy.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint32_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = nbArchiveFiles / 2 + 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = nbArchiveFiles / 2 + 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = m_storageClassDualCopy.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 10;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ(10, m.begin()->first);
    ASSERT_EQ(10, m.begin()->second.archiveFileID);

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345687");
    searchCriteria.diskFileIds = diskFileIds;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ("12345687", m.begin()->second.diskFileId);

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = nbArchiveFiles + 1234;
    ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(0, summary.totalBytes);
    ASSERT_EQ(0, summary.totalFiles);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->setTapeDisabled(m_admin, tape1.vid, "unit Test");

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(1, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(0, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->modifyTapeState(m_admin, tape1.vid, common::dataStructures::Tape::ACTIVE,cta::nullopt);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->setTapeFull(m_admin, tape1.vid, true);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(1, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(0, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->setTapeFull(m_admin, tape1.vid, false);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, DISABLED_concurrent_filesWrittenToTape_many_archive_files) {
  using namespace cta;

  std::unique_ptr<cta::catalogue::Catalogue> catalogue2;

  try {
    catalogue::CatalogueFactory *const *const catalogueFactoryPtrPtr = GetParam();

    if(nullptr == catalogueFactoryPtrPtr) {
      throw exception::Exception("Global pointer to the catalogue factory pointer for unit-tests in null");
    }

    if(nullptr == (*catalogueFactoryPtrPtr)) {
      throw exception::Exception("Global pointer to the catalogue factoryfor unit-tests in null");
    }

    catalogue2 = (*catalogueFactoryPtrPtr)->create();

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }

  class Barrier {
  public:
    Barrier(unsigned int count) : m_exited(false) {
      pthread_barrier_init(&m_barrier, nullptr, count);
    }
    ~Barrier() {
      pthread_barrier_destroy(&m_barrier);
    }
    void wait() {
      pthread_barrier_wait(&m_barrier);
    }
    void exit() {
      threading::MutexLocker lock(m_mtx);
      m_exited = true;
    }

    bool hasExited() {
      threading::MutexLocker lock(m_mtx);
      return m_exited;
    }

    threading::Mutex m_mtx;
    pthread_barrier_t m_barrier;
    bool m_exited;
  };

  class filesWrittenThread : public threading::Thread {
  public:
    filesWrittenThread(
        cta::catalogue::Catalogue *const cat,
        Barrier &barrier,
        const uint64_t nbArchiveFiles,
        const uint64_t batchSize,
        const common::dataStructures::StorageClass &storageClass,
        const uint64_t &archiveFileSize,
        const checksum::ChecksumBlob &checksumBlob,
        const std::string &vid,
        const uint64_t &copyNb,
        const std::string &tapeDrive,
        const std::string &diskInstance) :
          m_cat(cat), m_barrier(barrier), m_nbArchiveFiles(nbArchiveFiles), m_batchSize(batchSize), m_storageClass(storageClass), m_archiveFileSize(archiveFileSize),
          m_checksumBlob(checksumBlob), m_vid(vid), m_copyNb(copyNb), m_tapeDrive(tapeDrive),m_diskInstance(diskInstance) { }

    void run() override {
      for(uint64_t batch=0;batch< 1 + (m_nbArchiveFiles-1)/m_batchSize;++batch) {
        uint64_t bs = m_nbArchiveFiles - (m_batchSize*batch);
        if (bs> m_batchSize) {
          bs = m_batchSize;
        }
        std::set<catalogue::TapeItemWrittenPointer> tapeFilesWritten;
        for(uint64_t i= 0 ; i < bs; i++) {
          // calculate this file's archive_file_id and fseq numbers
          const uint64_t fn_afid = 1 + m_batchSize*batch + i;
          const uint64_t fn_seq = (m_copyNb == 1) ? fn_afid : 1 + m_batchSize*batch + (bs-i-1);
          std::ostringstream diskFileId;
          diskFileId << (12345677 + fn_afid);
          std::ostringstream diskFilePath;
          diskFilePath << "/public_dir/public_file_" << fn_afid;

          // Tape this batch to tape
          auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
          auto & fileWritten = *fileWrittenUP;
          fileWritten.archiveFileId = fn_afid;
          fileWritten.diskInstance = m_diskInstance;
          fileWritten.diskFileId = diskFileId.str();

          fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
          fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
          fileWritten.size = m_archiveFileSize;
          fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
          fileWritten.storageClassName = m_storageClass.name;
          fileWritten.vid = m_vid;
          fileWritten.fSeq = fn_seq;
          fileWritten.blockId = fn_seq * 100;
          fileWritten.copyNb = m_copyNb;
          fileWritten.tapeDrive = m_tapeDrive;
          tapeFilesWritten.emplace(fileWrittenUP.release());
        }
        m_barrier.wait();
        try {
          m_cat->filesWrittenToTape(tapeFilesWritten);
        } catch(std::exception &) {
          m_barrier.exit();
          m_barrier.wait();
          throw;
        }
        m_barrier.wait();
        if (m_barrier.hasExited()) {
          return;
        }
      }
    }

    cta::catalogue::Catalogue *const m_cat;
    Barrier &m_barrier;
    const uint64_t m_nbArchiveFiles;
    const uint64_t m_batchSize;
    const common::dataStructures::StorageClass m_storageClass;
    const uint64_t m_archiveFileSize;
    const checksum::ChecksumBlob m_checksumBlob;
    const std::string m_vid;
    const uint64_t m_copyNb;
    const std::string m_tapeDrive;
    const std::string m_diskInstance;
  };

  class filesWrittenRunner {
  public:
    filesWrittenRunner(filesWrittenThread &th) : m_th(th), m_waited(false) { m_th.start(); }
    ~filesWrittenRunner() {
      if (!m_waited) {
        try {
          m_th.wait();
        } catch(...) {
          // nothing
        }
      }
    }
    void wait() {
      m_waited = true;
      m_th.wait();
    }
    filesWrittenThread &m_th;
    bool m_waited;
  };

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  m_catalogue->createTape(m_admin, tape1);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;
  m_catalogue->createTape(m_admin, tape2);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    const auto tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const auto vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      ASSERT_NE(vidToTape.end(), it);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(tape1.vid, tape.vid);
      ASSERT_EQ(tape1.mediaType, tape.mediaType);
      ASSERT_EQ(tape1.vendor, tape.vendor);
      ASSERT_EQ(tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName1, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      const auto &tape = it->second;
      ASSERT_EQ(tape2.vid, tape.vid);
      ASSERT_EQ(tape2.mediaType, tape.mediaType);
      ASSERT_EQ(tape2.vendor, tape.vendor);
      ASSERT_EQ(tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName2, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const auto creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const auto lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  common::dataStructures::StorageClass storageClass;

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapeDrive1 = "tape_drive1";
  const std::string tapeDrive2 = "tape_drive2";

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 200; // Must be a multiple of batchsize for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  const uint64_t batchsize = 20;

  checksum::ChecksumBlob checksumBlob;
  checksumBlob.insert(checksum::ADLER32, "9876");

  {
    Barrier barrier(2);
    filesWrittenThread a(m_catalogue.get(), barrier, nbArchiveFiles, batchsize, storageClass, archiveFileSize, checksumBlob, vid1, 1, tapeDrive1,diskInstance);
    filesWrittenThread b(catalogue2.get(), barrier, nbArchiveFiles, batchsize, storageClass, archiveFileSize, checksumBlob, vid2, 2, tapeDrive2,diskInstance);

    filesWrittenRunner r1(a);
    filesWrittenRunner r2(b);
    r1.wait();
    r2.wait();
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345678");
    searchCriteria.diskFileIds = diskFileIds;
    searchCriteria.vid = tape1.vid;

    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    const auto idAndFile = m.find(1);
    ASSERT_NE(m.end(), idAndFile);
    const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
    ASSERT_EQ(searchCriteria.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(searchCriteria.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(searchCriteria.diskFileIds->front(), archiveFile.diskFileId);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    ASSERT_EQ(searchCriteria.vid, archiveFile.tapeFiles.begin()->vid);
  }

  auto afidToSeq = [](const uint64_t l_nbTot, const uint64_t l_batchsize, const uint64_t l_afid, uint64_t &l_seq1, uint64_t &l_seq2) {
    l_seq1 = l_afid;
    uint64_t batch = (l_afid-1)/l_batchsize;
    uint64_t bidx = (l_afid-1)%l_batchsize;
    uint64_t bs = l_nbTot - batch*l_batchsize;
    if (bs>l_batchsize) {
      bs = l_batchsize;
    }
    l_seq2 = batch*l_batchsize + (bs-bidx);
  };

  {
    auto archiveFileItor = m_catalogue->getArchiveFilesItor();
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);

      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "2468");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = seq1;
      fileWritten1.blockId = seq1 * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.fSeq = seq2;
      fileWritten2.blockId = seq2 * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(storageClass.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  {
    const uint64_t startFseq = 1;
    auto archiveFileItor = m_catalogue->getArchiveFilesForRepackItor(tape1.vid, startFseq);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "2468");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = seq1;
      fileWritten1.blockId = seq1 * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.fSeq = seq2;
      fileWritten2.blockId = seq2 * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(storageClass.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  for(uint64_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles;
    const auto archiveFiles = m_catalogue->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      uint64_t seq = (copyNb==1) ? seq1 : seq2;
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = seq;
      fileWritten.blockId = seq * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint64_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles / 2; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      uint64_t seq = (copyNb==1) ? seq1 : seq2;
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = seq;
      fileWritten.blockId = seq * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint64_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = nbArchiveFiles / 2 + 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = nbArchiveFiles / 2 + 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      uint64_t seq = (copyNb==1) ? seq1 : seq2;
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = seq;
      fileWritten.blockId = seq * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 10;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ(10, m.begin()->first);
    ASSERT_EQ(10, m.begin()->second.archiveFileID);

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345687");
    searchCriteria.diskFileIds = diskFileIds;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ("12345687", m.begin()->second.diskFileId);

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ("/public_dir/public_file_10", m.begin()->second.diskFileInfo.path);

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = nbArchiveFiles + 1234;
    ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(0, summary.totalBytes);
    ASSERT_EQ(0, summary.totalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_1_tape_copy) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_1_tape_copy_deleteStorageClass) {
  using namespace cta;

  const std::string diskInstance = "disk_instance";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_THROW(m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name), catalogue::UserSpecifiedStorageClassUsedByArchiveFiles);
  ASSERT_THROW(m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_file_recycle_log_deleteStorageClass) {
  using namespace cta;

  log::LogContext dummyLc(m_dummyLog);

  const std::string diskInstance = "disk_instance";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    ASSERT_EQ(1, tapes.size());
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    cta::common::dataStructures::DeleteArchiveRequest deletedArchiveReq;
    deletedArchiveReq.archiveFile = archiveFile;
    deletedArchiveReq.diskInstance = diskInstance;
    deletedArchiveReq.archiveFileID = archiveFileId;
    deletedArchiveReq.diskFileId = file1Written.diskFileId;
    deletedArchiveReq.recycleTime = time(nullptr);
    deletedArchiveReq.requester = cta::common::dataStructures::RequesterIdentity(m_admin.username,"group");
    deletedArchiveReq.diskFilePath = "/path/";
    m_catalogue->moveArchiveFileToRecycleLog(deletedArchiveReq,dummyLc);
  }

  ASSERT_THROW(m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name), catalogue::UserSpecifiedStorageClassUsedByFileRecycleLogs);
  ASSERT_THROW(m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name), exception::UserError);

  {
    //reclaim the tape to delete the files from the recycle log and delete the storage class
    m_catalogue->setTapeFull(m_admin,m_tape1.vid,true);
    m_catalogue->reclaimTape(m_admin,m_tape1.vid,dummyLc);
  }

  ASSERT_NO_THROW(m_catalogue->deleteStorageClass(m_storageClassSingleCopy.name));
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->getTapes().size());
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_same_copy_number) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_sintance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 1;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->getTapes().size());
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    const auto &tapeFile = *archiveFile.tapeFiles.begin();

    ASSERT_TRUE(file1Written.vid == tapeFile.vid || file2Written.vid == tapeFile.vid);

    {
      const auto &fileWritten = file1Written.vid == tapeFile.vid ? file1Written : file2Written;

      ASSERT_EQ(fileWritten.vid, tapeFile.vid);
      ASSERT_EQ(fileWritten.fSeq, tapeFile.fSeq);
      ASSERT_EQ(fileWritten.blockId, tapeFile.blockId);
      ASSERT_EQ(fileWritten.checksumBlob, tapeFile.checksumBlob);
      ASSERT_EQ(fileWritten.copyNb, tapeFile.copyNb);
    }
  }
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_same_copy_number_same_tape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape1.vid;
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 1;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(1, m_catalogue->getTapes().size());
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(2, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    const auto &tapeFile = *archiveFile.tapeFiles.begin();

    ASSERT_TRUE(file1Written.fSeq == tapeFile.fSeq || file2Written.fSeq == tapeFile.fSeq);

    {
      const auto &fileWritten = file1Written.fSeq == tapeFile.fSeq ? file1Written : file2Written;

      ASSERT_EQ(fileWritten.vid, tapeFile.vid);
      ASSERT_EQ(fileWritten.fSeq, tapeFile.fSeq);
      ASSERT_EQ(fileWritten.blockId, tapeFile.blockId);
      ASSERT_EQ(fileWritten.checksumBlob, tapeFile.checksumBlob);
      ASSERT_EQ(fileWritten.copyNb, tapeFile.copyNb);
    }
  }
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_same_fseq_same_tape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape1.vid;
  file2Written.fSeq                 = file1Written.fSeq;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet), exception::TapeFseqMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_sizes) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize1 = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize1;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  const uint64_t archiveFileSize2 = 2;

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize2;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet), catalogue::FileSizeMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_checksum_types) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(checksum::CRC32, "1234");
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet), exception::ChecksumTypeMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_checksum_values) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }


  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(checksum::ADLER32, "5678");
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet), exception::ChecksumValueMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->getArchiveFilesItor();
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    auto mItor = m.find(file1Written.archiveFileId);
    ASSERT_NE(m.end(), mItor);

    const common::dataStructures::ArchiveFile archiveFile = mItor->second;

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->getTapes().size());
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->getArchiveFilesItor();
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    {
      auto mItor = m.find(file1Written.archiveFileId);
      ASSERT_NE(m.end(), mItor);

      const common::dataStructures::ArchiveFile archiveFile = mItor->second;

      ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(file2Written.size, archiveFile.fileSize);
      ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

      ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

      ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

      ASSERT_EQ(2, archiveFile.tapeFiles.size());

      auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
      ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
      ASSERT_EQ(file1Written.vid, tapeFile1.vid);
      ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
      ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
      ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
      ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

      auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
      ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
      ASSERT_EQ(file2Written.vid, tapeFile2.vid);
      ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
      ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
      ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
      ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
    }
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE("disk_instance", archiveFileId, dummyLc);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile_by_archive_file_id_of_another_disk_instance) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createTape(m_admin, m_tape2);
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->getArchiveFilesItor();
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    auto mItor = m.find(file1Written.archiveFileId);
    ASSERT_NE(m.end(), mItor);

    const common::dataStructures::ArchiveFile archiveFile = mItor->second;

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->getTapes().size());
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->getArchiveFilesItor();
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    {
      auto mItor = m.find(file1Written.archiveFileId);
      ASSERT_NE(m.end(), mItor);

      const common::dataStructures::ArchiveFile archiveFile = mItor->second;

      ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(file2Written.size, archiveFile.fileSize);
      ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

      ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

      ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

      ASSERT_EQ(2, archiveFile.tapeFiles.size());

      auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
      ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
      ASSERT_EQ(file1Written.vid, tapeFile1.vid);
      ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
      ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
      ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
      ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

      auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
      ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
      ASSERT_EQ(file2Written.vid, tapeFile2.vid);
      ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
      ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
      ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
      ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
    }
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  log::LogContext dummyLc(m_dummyLog);
  ASSERT_THROW(m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE("another_disk_instance", archiveFileId, dummyLc), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile_by_archive_file_id_non_existent) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE("disk_instance", 12345678, dummyLc);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_non_existent_tape) {
  using namespace cta;

  std::set<std::string> vids = {{"non_existent_tape"}};
  ASSERT_THROW(m_catalogue->getTapesByVid(vids), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_no_vids) {
  using namespace cta;

  std::set<std::string> vids;
  ASSERT_TRUE(m_catalogue->getTapesByVid(vids).empty());
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_1_tape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 1;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToTapeMap = m_catalogue->getTapesByVid(allVids);
  ASSERT_EQ(nbTapes, vidToTapeMap.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto tapeItor = vidToTapeMap.find(vid.str());
    ASSERT_NE(vidToTapeMap.end(), tapeItor);

    ASSERT_EQ(vid.str(), tapeItor->second.vid);
    ASSERT_EQ(m_tape1.mediaType, tapeItor->second.mediaType);
    ASSERT_EQ(m_tape1.vendor, tapeItor->second.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tapeItor->second.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tapeItor->second.tapePoolName);
    ASSERT_EQ(m_vo.name, tapeItor->second.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tapeItor->second.capacityInBytes);
    ASSERT_EQ(m_tape1.state, tapeItor->second.state);
    ASSERT_EQ(m_tape1.full, tapeItor->second.full);

    ASSERT_FALSE(tapeItor->second.isFromCastor);
    ASSERT_EQ(0, tapeItor->second.readMountCount);
    ASSERT_EQ(0, tapeItor->second.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tapeItor->second.comment);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_350_tapes) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 310;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToTapeMap = m_catalogue->getTapesByVid(allVids);
  ASSERT_EQ(nbTapes, vidToTapeMap.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto tapeItor = vidToTapeMap.find(vid.str());
    ASSERT_NE(vidToTapeMap.end(), tapeItor);

    ASSERT_EQ(vid.str(), tapeItor->second.vid);
    ASSERT_EQ(m_tape1.mediaType, tapeItor->second.mediaType);
    ASSERT_EQ(m_tape1.vendor, tapeItor->second.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tapeItor->second.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tapeItor->second.tapePoolName);
    ASSERT_EQ(m_vo.name, tapeItor->second.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tapeItor->second.capacityInBytes);
    ASSERT_EQ(m_tape1.state, tapeItor->second.state);
    ASSERT_EQ(m_tape1.full, tapeItor->second.full);

    ASSERT_FALSE(tapeItor->second.isFromCastor);
    ASSERT_EQ(0, tapeItor->second.readMountCount);
    ASSERT_EQ(0, tapeItor->second.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tapeItor->second.comment);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getVidToLogicalLibrary_no_vids) {
  using namespace cta;

  std::set<std::string> vids;
  ASSERT_TRUE(m_catalogue->getVidToLogicalLibrary(vids).empty());
}

TEST_P(cta_catalogue_CatalogueTest, getVidToLogicalLibrary_1_tape) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 1;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToLogicalLibrary = m_catalogue->getVidToLogicalLibrary(allVids);
  ASSERT_EQ(nbTapes, vidToLogicalLibrary.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto itor = vidToLogicalLibrary.find(vid.str());
    ASSERT_NE(vidToLogicalLibrary.end(), itor);

    ASSERT_EQ(m_tape1.logicalLibraryName, itor->second);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getVidToLogicalLibrary_310_tapes) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 310;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToLogicalLibrary = m_catalogue->getVidToLogicalLibrary(allVids);
  ASSERT_EQ(nbTapes, vidToLogicalLibrary.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto itor = vidToLogicalLibrary.find(vid.str());
    ASSERT_NE(vidToLogicalLibrary.end(), itor);

    ASSERT_EQ(m_tape1.logicalLibraryName, itor->second);
  }
}


TEST_P(cta_catalogue_CatalogueTest, getAllDiskSystems_no_systems) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());
}

TEST_P(cta_catalogue_CatalogueTest, getAllDiskSystems_many_diskSystems) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;

  const uint32_t nbDiskSystems = 16;

  for(uint32_t i = 0; i < nbDiskSystems; i++) {
    std::ostringstream name;
    name << "DiskSystem" << std::setfill('0') << std::setw(5) << i;
    const std::string diskSystemComment = "Create disk system " + name.str();
    m_catalogue->createDiskSystem(m_admin, name.str(), fileRegexp,
      freeSpaceQueryURL, refreshInterval + i, targetedFreeSpace + i, sleepTime + i, diskSystemComment);
  }

  auto diskSystemsList = m_catalogue->getAllDiskSystems();
  ASSERT_EQ(nbDiskSystems, diskSystemsList.size());

  for(uint32_t i = 0; i < nbDiskSystems; i++) {
    std::ostringstream name;
    name << "DiskSystem" << std::setfill('0') << std::setw(5) << i;
    const std::string diskSystemComment = "Create disk system " + name.str();
    ASSERT_NO_THROW(diskSystemsList.at(name.str()));
    const auto diskSystem = diskSystemsList.at(name.str());

    ASSERT_EQ(name.str(), diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval + i, diskSystem.refreshInterval );
    ASSERT_EQ(targetedFreeSpace + i, diskSystem.targetedFreeSpace);
    ASSERT_EQ(diskSystemComment, diskSystem.comment);
  }
}

TEST_P(cta_catalogue_CatalogueTest, diskSystemExists_emptyString) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "";

  ASSERT_THROW(m_catalogue->diskSystemExists(name), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_emptyStringDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment),
    catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_emptyStringFileRegexp) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment),
    catalogue::UserSpecifiedAnEmptyStringFileRegexp);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_emptyStringFresSpaceQueryURL) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment),
    catalogue::UserSpecifiedAnEmptyStringFreeSpaceQueryURL);
}


TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_zeroRefreshInterval) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 0;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment),
    catalogue::UserSpecifiedAZeroRefreshInterval);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_zeroTargetedFreeSpace) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 0;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment),
    catalogue::UserSpecifiedAZeroTargetedFreeSpace);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_emptyStringComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "";

  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_9_exabytes_targetedFreeSpace) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 9L * 1000 * 1000 * 1000 * 1000 * 1000 * 1000;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  const auto diskSystemList = m_catalogue->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());

  {
    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(sleepTime, diskSystem.sleepTime);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_sleepTimeHandling) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 0;
  const std::string comment = "disk system comment";

  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment),
    catalogue::UserSpecifiedAZeroSleepTime);

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, std::numeric_limits<int64_t>::max(), comment);

  const auto diskSystemList = m_catalogue->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());

  {
    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), diskSystem.sleepTime);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}


TEST_P(cta_catalogue_CatalogueTest, createDiskSystem_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  const auto diskSystemList = m_catalogue->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());
  ASSERT_THROW(m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment), exception::UserError);

}

TEST_P(cta_catalogue_CatalogueTest, deleteDiskSystem) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  const auto diskSystemList = m_catalogue->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());

  const auto &diskSystem = diskSystemList.front();
  ASSERT_EQ(name, diskSystem.name);
  ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
  ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
  ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
  ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
  ASSERT_EQ(comment, diskSystem.comment);

  const auto creationLog = diskSystem.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskSystem.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteDiskSystem(diskSystem.name);
  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteDiskSystem_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());
  ASSERT_THROW(m_catalogue->deleteDiskSystem("non_existent_disk_system"), catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFileRegexp) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedFileRegexp = "modified_fileRegexp";
  m_catalogue->modifyDiskSystemFileRegexp(m_admin, name, modifiedFileRegexp);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(modifiedFileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFileRegexp_emptyStringDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const std::string modifiedFileRegexp = "modified_fileRegexp";
  ASSERT_THROW(m_catalogue->modifyDiskSystemFileRegexp(m_admin, diskSystemName, modifiedFileRegexp),
    catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFileRegexp_nonExistentDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const std::string modifiedFileRegexp = "modified_fileRegexp";
  ASSERT_THROW(m_catalogue->modifyDiskSystemFileRegexp(m_admin, diskSystemName, modifiedFileRegexp),
    catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFileRegexp_emptyStringFileRegexp) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

 const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedFileRegexp = "";
  ASSERT_THROW(m_catalogue->modifyDiskSystemFileRegexp(m_admin, name, modifiedFileRegexp),
    catalogue::UserSpecifiedAnEmptyStringFileRegexp);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFreeSpaceQueryURL) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedFreeSpaceQueryURL = "modified_freeSpaceQueryURL";
  m_catalogue->modifyDiskSystemFreeSpaceQueryURL(m_admin, name, modifiedFreeSpaceQueryURL);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(modifiedFreeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFreeSpaceQueryURL_emptyStringDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const std::string modifiedFreeSpaceQueryURL = "modified_freeSpaceQueryURL";
  ASSERT_THROW(m_catalogue->modifyDiskSystemFreeSpaceQueryURL(m_admin, diskSystemName, modifiedFreeSpaceQueryURL),
    catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFreeSpaceQueryURL_nonExistentDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const std::string modifiedFreeSpaceQueryURL = "modified_freeSpaceQueryURL";
  ASSERT_THROW(m_catalogue->modifyDiskSystemFreeSpaceQueryURL(m_admin, diskSystemName, modifiedFreeSpaceQueryURL),
    catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemFreeSpaceQueryURL_emptyStringFreeSpaceQueryURL) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

 const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedFreeSpaceQueryURL = "";
  ASSERT_THROW(m_catalogue->modifyDiskSystemFreeSpaceQueryURL(m_admin, name, modifiedFreeSpaceQueryURL),
    catalogue::UserSpecifiedAnEmptyStringFreeSpaceQueryURL);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemRefreshInterval) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedRefreshInterval = 128;
  m_catalogue->modifyDiskSystemRefreshInterval(m_admin, name, modifiedRefreshInterval);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(modifiedRefreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemRefreshInterval_emptyStringDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const uint64_t modifiedRefreshInterval = 128;
  ASSERT_THROW(m_catalogue->modifyDiskSystemRefreshInterval(m_admin, diskSystemName, modifiedRefreshInterval),
    catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemRefreshInterval_nonExistentDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const uint64_t modifiedRefreshInterval = 128;
  ASSERT_THROW(m_catalogue->modifyDiskSystemRefreshInterval(m_admin, diskSystemName, modifiedRefreshInterval),
    catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemRefreshInterval_zeroRefreshInterval) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

 const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedRefreshInterval = 0;
  ASSERT_THROW(m_catalogue->modifyDiskSystemRefreshInterval(m_admin, name, modifiedRefreshInterval),
    catalogue::UserSpecifiedAZeroRefreshInterval);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemTargetedFreeSpace) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedTargetedFreeSpace = 128;
  m_catalogue->modifyDiskSystemTargetedFreeSpace(m_admin, name, modifiedTargetedFreeSpace);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(modifiedTargetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemTargetedFreeSpace_emptyStringDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const uint64_t modifiedTargetedFreeSpace = 128;
  ASSERT_THROW(m_catalogue->modifyDiskSystemTargetedFreeSpace(m_admin, diskSystemName, modifiedTargetedFreeSpace),
    catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemTargetedFreeSpace_nonExistentDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const uint64_t modifiedTargetedFreeSpace = 128;
  ASSERT_THROW(m_catalogue->modifyDiskSystemTargetedFreeSpace(m_admin, diskSystemName, modifiedTargetedFreeSpace),
    catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemTargetedFreeSpace_zeroTargetedFreeSpace) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

 const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedTargetedFreeSpace = 0;
  ASSERT_THROW(m_catalogue->modifyDiskSystemTargetedFreeSpace(m_admin, name, modifiedTargetedFreeSpace),
    catalogue::UserSpecifiedAZeroTargetedFreeSpace);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "modified_comment";
  m_catalogue->modifyDiskSystemComment(m_admin, name, modifiedComment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(modifiedComment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemComment_emptyStringDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const std::string modifiedComment = "modified_comment";
  ASSERT_THROW(m_catalogue->modifyDiskSystemComment(m_admin, diskSystemName, modifiedComment),
    catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemComment_nonExistentDiskSystemName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const std::string modifiedComment = "modified_comment";
  ASSERT_THROW(m_catalogue->modifyDiskSystemComment(m_admin, diskSystemName, modifiedComment),
    catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskSystemCommentL_emptyStringComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskSystems().empty());

 const std::string name = "disk_system_name";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->createDiskSystem(m_admin, name, fileRegexp,
    freeSpaceQueryURL, refreshInterval, targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "";
  ASSERT_THROW(m_catalogue->modifyDiskSystemComment(m_admin, name, modifiedComment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, getAllDiskInstances_empty) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllDiskInstances().empty());
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstance) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, name, comment);

  const auto diskInstanceList = m_catalogue->getAllDiskInstances();
  ASSERT_EQ(1, diskInstanceList.size());

  const auto &diskInstance = diskInstanceList.front();
  ASSERT_EQ(diskInstance.name, name);
  ASSERT_EQ(diskInstance.comment, comment);
  
  const auto creationLog = diskInstance.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstance.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstance_twice) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, name, comment);

  const auto diskInstanceList = m_catalogue->getAllDiskInstances();
  ASSERT_EQ(1, diskInstanceList.size());

  const auto &diskInstance = diskInstanceList.front();
  ASSERT_EQ(diskInstance.name, name);
  ASSERT_EQ(diskInstance.comment, comment);
  
  const auto creationLog = diskInstance.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstance.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->createDiskInstance(m_admin, name, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstance_emptyName) {
  using namespace cta;

  const std::string comment = "disk_instance_comment";
  ASSERT_THROW(m_catalogue->createDiskInstance(m_admin, "", comment),  catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstance_emptyComment) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  ASSERT_THROW(m_catalogue->createDiskInstance(m_admin, name, ""),  catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, deleteDiskInstance) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, name, comment);

  const auto diskInstanceList = m_catalogue->getAllDiskInstances();
  ASSERT_EQ(1, diskInstanceList.size());

  const auto &diskInstance = diskInstanceList.front();
  ASSERT_EQ(diskInstance.name, name);
  ASSERT_EQ(diskInstance.comment, comment);
  
  const auto creationLog = diskInstance.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstance.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteDiskInstance(name);
  ASSERT_TRUE(m_catalogue->getAllDiskInstances().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteDiskInstance_nonExisting) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  ASSERT_THROW(m_catalogue->deleteDiskInstance(name), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceComment) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, name, comment);
  {
    const auto diskInstanceList = m_catalogue->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, comment);
  
    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstance.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "modified_disk_instance_comment";
  m_catalogue->modifyDiskInstanceComment(m_admin, name, modifiedComment);
  
  {
    const auto diskInstanceList = m_catalogue->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, modifiedComment);
  
    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceComment_emptyName) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, name, comment);
  {
    const auto diskInstanceList = m_catalogue->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, comment);
  
    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstance.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->modifyDiskInstanceComment(m_admin, "", comment), 
    catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceComment_emptyComment) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, name, comment);
  {
    const auto diskInstanceList = m_catalogue->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, comment);
  
    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstance.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->modifyDiskInstanceComment(m_admin, name, ""), 
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceComment_nonExisting) {
  using namespace cta;

  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  ASSERT_THROW(m_catalogue->modifyDiskInstanceComment(m_admin, name, comment), 
    catalogue::UserSpecifiedANonExistentDiskInstance);
}


TEST_P(cta_catalogue_CatalogueTest, createDiskInstanceSpace) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
  ASSERT_EQ(1, diskInstanceSpaceList.size());

  const auto &diskInstanceSpace = diskInstanceSpaceList.front();
  ASSERT_EQ(diskInstanceSpace.name, name);
  ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
  ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
  ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
  ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
  ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
  
  ASSERT_EQ(diskInstanceSpace.comment, comment);
  
  const auto creationLog = diskInstanceSpace.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstanceSpace_twice) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
  ASSERT_EQ(1, diskInstanceSpaceList.size());

  const auto &diskInstanceSpace = diskInstanceSpaceList.front();
  ASSERT_EQ(diskInstanceSpace.name, name);
  ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
  ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
  ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
  ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
  ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
  
  ASSERT_EQ(diskInstanceSpace.comment, comment);
  
  const auto creationLog = diskInstanceSpace.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);


  ASSERT_THROW(m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstanceSpace_nonExistantDiskInstance) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment), 
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstanceSpace_emptyName) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->createDiskInstanceSpace(m_admin, "", diskInstance, freeSpaceQueryURL, refreshInterval, comment),
    catalogue::UserSpecifiedAnEmptyStringDiskInstanceSpaceName);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstanceSpace_emptyComment) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);
  
  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, ""),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstanceSpace_emptyFreeSpaceQueryURL) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);
  
  const std::string name = "disk_instance_space_name";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, "", refreshInterval, comment),
    catalogue::UserSpecifiedAnEmptyStringFreeSpaceQueryURL);
}

TEST_P(cta_catalogue_CatalogueTest, createDiskInstanceSpace_zeroRefreshInterval) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);
  
  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, 0, comment),
    catalogue::UserSpecifiedAZeroRefreshInterval);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceComment) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }

  const std::string newDiskInstanceSpaceComment = "disk_instance_comment_2";
  m_catalogue->modifyDiskInstanceSpaceComment(m_admin, name, diskInstance, newDiskInstanceSpaceComment);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, newDiskInstanceSpaceComment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceComment_empty) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }
  ASSERT_THROW(m_catalogue->modifyDiskInstanceSpaceComment(m_admin, name, diskInstance, ""), catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceComment_nonExistingSpace) {
  using namespace cta;

  const std::string name = "disk_instance_space_name";
  const std::string diskInstance = "disk_instance_name";
  const std::string comment = "disk_instance_space_comment";
  ASSERT_THROW(m_catalogue->modifyDiskInstanceSpaceComment(m_admin, name, diskInstance, comment), catalogue::UserSpecifiedANonExistentDiskInstanceSpace);
}


TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceQueryURL) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }

  const std::string newFreeSpaceQueryURL = "new_free_space_query_URL";
  m_catalogue->modifyDiskInstanceSpaceQueryURL(m_admin, name, diskInstance, newFreeSpaceQueryURL);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, newFreeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceQueryURL_empty) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }
  ASSERT_THROW(m_catalogue->modifyDiskInstanceSpaceQueryURL(m_admin, name, diskInstance, ""), catalogue::UserSpecifiedAnEmptyStringFreeSpaceQueryURL);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceQueryURL_nonExistingSpace) {
  using namespace cta;

  const std::string name = "disk_instance_space_name";
  const std::string diskInstance = "disk_instance_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  ASSERT_THROW(m_catalogue->modifyDiskInstanceSpaceQueryURL(m_admin, name, diskInstance, freeSpaceQueryURL), catalogue::UserSpecifiedANonExistentDiskInstanceSpace);
}


TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceRefreshInterval) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }

  const uint64_t newRefreshInterval = 35;
  m_catalogue->modifyDiskInstanceSpaceRefreshInterval(m_admin, name, diskInstance, newRefreshInterval);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, newRefreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceRefreshInterval_zeroInterval) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }
  ASSERT_THROW(m_catalogue->modifyDiskInstanceSpaceRefreshInterval(m_admin, name, diskInstance, 0), catalogue::UserSpecifiedAZeroRefreshInterval);
}

TEST_P(cta_catalogue_CatalogueTest, modifyDiskInstanceSpaceRefreshInterval_nonExistingSpace) {
  using namespace cta;

  const std::string name = "disk_instance_space_name";
  const std::string diskInstance = "disk_instance_name";
  const uint64_t refreshInterval = 32;
  ASSERT_THROW(m_catalogue->modifyDiskInstanceSpaceRefreshInterval(m_admin, name, diskInstance, refreshInterval), catalogue::UserSpecifiedANonExistentDiskInstanceSpace);
}

TEST_P(cta_catalogue_CatalogueTest, deleteDiskInstanceSpace) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);
  {
    const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);
    
    ASSERT_EQ(diskInstanceSpace.comment, comment);
    
    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  m_catalogue->deleteDiskInstanceSpace(name, diskInstance);
  const auto diskInstanceSpaceList = m_catalogue->getAllDiskInstanceSpaces();
  ASSERT_EQ(0, diskInstanceSpaceList.size());
}

TEST_P(cta_catalogue_CatalogueTest, deleteDiskInstanceSpace_notExisting) {
  using namespace cta;

  const std::string diskInstance = "disk_instance_name";
  const std::string name = "disk_instance_space_name";
  
  ASSERT_THROW(m_catalogue->deleteDiskInstanceSpace(name, diskInstance), catalogue::UserSpecifiedANonExistentDiskInstanceSpace); 
}

TEST_P(cta_catalogue_CatalogueTest, getNbFilesOnTape_no_tape_files) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_EQ(0, m_catalogue->getNbFilesOnTape(m_tape1.vid));
}

TEST_P(cta_catalogue_CatalogueTest, getNbFilesOnTape_one_tape_file) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_EQ(1, m_catalogue->getNbFilesOnTape(m_tape1.vid));
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_no_tape_files) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_NO_THROW(m_catalogue->checkTapeForLabel(m_tape1.vid));
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_one_tape_file) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_THROW(m_catalogue->checkTapeForLabel(m_tape1.vid), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_one_tape_file_reclaimed_tape) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_THROW(m_catalogue->checkTapeForLabel(m_tape1.vid), exception::UserError);

  log::LogContext dummyLc(m_dummyLog);
  m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(diskInstanceName1, archiveFileId, dummyLc);

  m_catalogue->setTapeFull(m_admin, m_tape1.vid, true);
  m_catalogue->reclaimTape(m_admin, m_tape1.vid,dummyLc);

  ASSERT_NO_THROW(m_catalogue->checkTapeForLabel(m_tape1.vid));
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_not_in_the_catalogue) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->checkTapeForLabel(m_tape1.vid), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_empty_vid) {
  using namespace cta;

  const std::string vid = "";
  ASSERT_THROW(m_catalogue->checkTapeForLabel(vid), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTape_full_lastFSeq_0_no_tape_files) {
  using namespace cta;

  log::LogContext dummyLc(m_dummyLog);

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeFull(m_admin, m_tape1.vid, true);
  m_catalogue->reclaimTape(m_admin, m_tape1.vid, dummyLc);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_FALSE(tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTape_not_full_lastFSeq_0_no_tape_files) {
  using namespace cta;

  log::LogContext dummyLc(m_dummyLog);

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, m_tape1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  ASSERT_THROW(m_catalogue->reclaimTape(m_admin, m_tape1.vid, dummyLc), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTape_full_lastFSeq_1_no_tape_files) {
  using namespace cta;

  log::LogContext dummyLc(m_dummyLog);

  const std::string diskInstanceName1 = "disk_instance_1";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(diskInstanceName1, file1Written.archiveFileId, dummyLc);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeFull(m_admin, m_tape1.vid, true);
  m_catalogue->reclaimTape(m_admin, m_tape1.vid, dummyLc);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTape_full_lastFSeq_1_one_tape_file) {
  using namespace cta;
  log::LogContext dummyLc(m_dummyLog);

  const std::string diskInstanceName1 = "disk_instance_1";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(file1Written.size, tape.dataOnTapeInBytes);
    ASSERT_EQ(file1Written.size, tape.masterDataInBytes);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeFull(m_admin, m_tape1.vid, true);
  ASSERT_THROW(m_catalogue->reclaimTape(m_admin, m_tape1.vid, dummyLc), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, ping) {
  using namespace cta;

  m_catalogue->ping();
}

TEST_P(cta_catalogue_CatalogueTest, getSchemaVersion) {
  using namespace cta;

  const auto schemaDbVersion = m_catalogue->getSchemaVersion();
  ASSERT_EQ((uint64_t)CTA_CATALOGUE_SCHEMA_VERSION_MAJOR,schemaDbVersion.getSchemaVersion<catalogue::SchemaVersion::MajorMinor>().first);
  ASSERT_EQ((uint64_t)CTA_CATALOGUE_SCHEMA_VERSION_MINOR,schemaDbVersion.getSchemaVersion<catalogue::SchemaVersion::MajorMinor>().second);
}

TEST_P(cta_catalogue_CatalogueTest, createVirtualOrganization) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));
}

TEST_P(cta_catalogue_CatalogueTest, createVirtualOrganizationAlreadyExists) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));
  ASSERT_THROW(m_catalogue->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createVirtualOrganizationEmptyComment) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();
  vo.comment = "";

  ASSERT_THROW(m_catalogue->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createVirtualOrganizationEmptyName) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  vo.name = "";
  vo.comment = "comment";

  ASSERT_THROW(m_catalogue->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteVirtualOrganization) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  ASSERT_NO_THROW(m_catalogue->deleteVirtualOrganization(vo.name));
}

TEST_P(cta_catalogue_CatalogueTest, deleteVirtualOrganizationUsedByTapePool) {
  using namespace cta;

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  ASSERT_THROW(m_catalogue->deleteVirtualOrganization(m_vo.name),exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteVirtualOrganizationNameDoesNotExist) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  ASSERT_THROW(m_catalogue->deleteVirtualOrganization("DOES_NOT_EXIST"),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteVirtualOrganizationUsedByStorageClass) {
  using namespace cta;

  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);
  ASSERT_THROW(m_catalogue->deleteVirtualOrganization(m_vo.name),exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getVirtualOrganizations) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  std::list<common::dataStructures::VirtualOrganization> vos = m_catalogue->getVirtualOrganizations();
  ASSERT_EQ(1,vos.size());

  auto &voRetrieved = vos.front();
  ASSERT_EQ(vo.name,voRetrieved.name);
  ASSERT_EQ(vo.readMaxDrives,voRetrieved.readMaxDrives);
  ASSERT_EQ(vo.writeMaxDrives,voRetrieved.writeMaxDrives);
  ASSERT_EQ(vo.comment,voRetrieved.comment);
  ASSERT_EQ(m_admin.host,voRetrieved.creationLog.host);
  ASSERT_EQ(m_admin.username,voRetrieved.creationLog.username);
  ASSERT_EQ(m_admin.host,voRetrieved.lastModificationLog.host);
  ASSERT_EQ(m_admin.username,voRetrieved.lastModificationLog.username);


  ASSERT_NO_THROW(m_catalogue->deleteVirtualOrganization(vo.name));
  vos = m_catalogue->getVirtualOrganizations();
  ASSERT_EQ(0,vos.size());
}

TEST_P(cta_catalogue_CatalogueTest, modifyVirtualOrganizationName) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  std::string newVoName = "NewVoName";

  ASSERT_NO_THROW(m_catalogue->modifyVirtualOrganizationName(m_admin,vo.name,newVoName));

  auto vos = m_catalogue->getVirtualOrganizations();

  auto voFront = vos.front();
  ASSERT_EQ(newVoName,voFront.name);
}

TEST_P(cta_catalogue_CatalogueTest, modifyVirtualOrganizationNameVoDoesNotExists) {
  using namespace cta;

  ASSERT_THROW(m_catalogue->modifyVirtualOrganizationName(m_admin,"DOES_NOT_EXIST","NEW_NAME"),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyVirtualOrganizationNameThatAlreadyExists) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  std::string vo2Name = "vo2";
  std::string vo1Name = vo.name;

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  vo.name = vo2Name;
  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  ASSERT_THROW(m_catalogue->modifyVirtualOrganizationName(m_admin,vo1Name,vo2Name),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyVirtualOrganizationComment) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  std::string newComment = "newComment";

  ASSERT_NO_THROW(m_catalogue->modifyVirtualOrganizationComment(m_admin,vo.name,newComment));

  auto vos = m_catalogue->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(newComment,frontVo.comment);

  ASSERT_THROW(m_catalogue->modifyVirtualOrganizationComment(m_admin,"DOES not exists","COMMENT_DOES_NOT_EXIST"),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyVirtualOrganizationReadMaxDrives) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  uint64_t readMaxDrives = 42;
  ASSERT_NO_THROW(m_catalogue->modifyVirtualOrganizationReadMaxDrives(m_admin,vo.name,readMaxDrives));

  auto vos = m_catalogue->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(readMaxDrives,frontVo.readMaxDrives);

  ASSERT_THROW(m_catalogue->modifyVirtualOrganizationReadMaxDrives(m_admin,"DOES not exists",readMaxDrives),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyVirtualOrganizationWriteMaxDrives) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  uint64_t writeMaxDrives = 42;
  ASSERT_NO_THROW(m_catalogue->modifyVirtualOrganizationWriteMaxDrives(m_admin,vo.name,writeMaxDrives));

  auto vos = m_catalogue->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(writeMaxDrives,frontVo.writeMaxDrives);

  ASSERT_THROW(m_catalogue->modifyVirtualOrganizationWriteMaxDrives(m_admin,"DOES not exists",writeMaxDrives),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyVirtualOrganizationMaxFileSize) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo = getVo();

  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));

  uint64_t maxFileSize = 1;
  ASSERT_NO_THROW(m_catalogue->modifyVirtualOrganizationMaxFileSize(m_admin,vo.name,maxFileSize));

  auto vos = m_catalogue->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(maxFileSize,frontVo.maxFileSize);

  ASSERT_THROW(m_catalogue->modifyVirtualOrganizationMaxFileSize(m_admin,"DOES not exists", maxFileSize),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getVirtualOrganizationOfTapepool) {
  using namespace cta;

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  common::dataStructures::VirtualOrganization vo = getVo();

  m_catalogue->createVirtualOrganization(m_admin,vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  cta::common::dataStructures::VirtualOrganization voFromTapepool = m_catalogue->getVirtualOrganizationOfTapepool(m_tape1.tapePoolName);
  ASSERT_EQ(vo,voFromTapepool);

  ASSERT_THROW(m_catalogue->getVirtualOrganizationOfTapepool("DOES_NOT_EXIST"),cta::exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, updateDiskFileId) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, m_tape1);
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenSet);

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  const std::string newDiskFileId = "9012";
  m_catalogue->updateDiskFileId(file1Written.archiveFileId, file1Written.diskInstance, newDiskFileId);

  {
    const common::dataStructures::ArchiveFile archiveFile = m_catalogue->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(newDiskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }
}

TEST_P(cta_catalogue_CatalogueTest, moveFilesToRecycleLog) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file"<<i;

    // Tape copy 1 written to tape
    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassSingleCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
  }
  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    ASSERT_TRUE(m_catalogue->getArchiveFilesItor().hasMore());
  }
  log::LogContext dummyLc(m_dummyLog);
  for(auto & tapeItemWritten: tapeFilesWrittenCopy1){
    cta::catalogue::TapeFileWritten * tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    cta::common::dataStructures::DeleteArchiveRequest req;
    req.requester.name = m_admin.username;
    req.archiveFileID = tapeItem->archiveFileId;
    req.diskFileId = tapeItem->diskFileId;
    req.diskFilePath = tapeItem->diskFilePath;
    req.diskInstance = tapeItem->diskInstance;
    req.archiveFile = m_catalogue->getArchiveFileById(tapeItem->archiveFileId);
    ASSERT_NO_THROW(m_catalogue->moveArchiveFileToRecycleLog(req,dummyLc));
  }
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  std::vector<common::dataStructures::FileRecycleLog> deletedArchiveFiles;
  {
    auto itor = m_catalogue->getFileRecycleLogItor();
    while(itor.hasMore()){
      deletedArchiveFiles.push_back(itor.next());
    }
  }

  //And test that these files are there.
  //Run the unit test for all the databases
  ASSERT_EQ(nbArchiveFiles,deletedArchiveFiles.size());

  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {

    auto deletedArchiveFile = deletedArchiveFiles[i-1];

    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file"<<i;

    ASSERT_EQ(i,deletedArchiveFile.archiveFileId);
    ASSERT_EQ(diskInstance,deletedArchiveFile.diskInstanceName);
    ASSERT_EQ(diskFileId.str(),deletedArchiveFile.diskFileId);
    ASSERT_EQ(diskFilePath.str(),deletedArchiveFile.diskFilePath);
    ASSERT_EQ(PUBLIC_DISK_USER,deletedArchiveFile.diskFileUid);
    ASSERT_EQ(PUBLIC_DISK_GROUP,deletedArchiveFile.diskFileGid);
    ASSERT_EQ(archiveFileSize,deletedArchiveFile.sizeInBytes);
    ASSERT_EQ(cta::checksum::ChecksumBlob(checksum::ADLER32, "1357"),deletedArchiveFile.checksumBlob);
    ASSERT_EQ(m_storageClassSingleCopy.name, deletedArchiveFile.storageClassName);
    ASSERT_EQ(diskFileId.str(),deletedArchiveFile.diskFileIdWhenDeleted);
    ASSERT_EQ(cta::catalogue::InsertFileRecycleLog::getDeletionReasonLog(m_admin.username,diskInstance),deletedArchiveFile.reasonLog);
    ASSERT_EQ(tape1.vid, deletedArchiveFile.vid);
    ASSERT_EQ(i,deletedArchiveFile.fSeq);
    ASSERT_EQ(i * 100,deletedArchiveFile.blockId);
    ASSERT_EQ(1, deletedArchiveFile.copyNb);
  }

  //Let's try the deletion of the files from the recycle-bin.
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    m_catalogue->deleteFilesFromRecycleLog(tape1.vid,dummyLc);
  }

  {
    auto itor = m_catalogue->getFileRecycleLogItor();
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTapeRemovesFilesFromRecycleLog) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapeDrive = "tape_drive";
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file"<<i;

    // Tape copy 1 written to tape
    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassSingleCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
  }
  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    ASSERT_TRUE(m_catalogue->getArchiveFilesItor().hasMore());
  }
  log::LogContext dummyLc(m_dummyLog);
  for(auto & tapeItemWritten: tapeFilesWrittenCopy1){
    cta::catalogue::TapeFileWritten * tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    cta::common::dataStructures::DeleteArchiveRequest req;
    req.archiveFileID = tapeItem->archiveFileId;
    req.diskFileId = tapeItem->diskFileId;
    req.diskFilePath = tapeItem->diskFilePath;
    req.diskInstance = tapeItem->diskInstance;
    req.archiveFile = m_catalogue->getArchiveFileById(tapeItem->archiveFileId);
    ASSERT_NO_THROW(m_catalogue->moveArchiveFileToRecycleLog(req,dummyLc));
  }
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  std::vector<common::dataStructures::FileRecycleLog> deletedArchiveFiles;
  {
    auto itor = m_catalogue->getFileRecycleLogItor();
    while(itor.hasMore()){
      deletedArchiveFiles.push_back(itor.next());
    }
  }

  //And test that these files are in the recycle log
  ASSERT_EQ(nbArchiveFiles,deletedArchiveFiles.size());

  ASSERT_TRUE(m_catalogue->getFileRecycleLogItor().hasMore());
  //Reclaim the tape
  m_catalogue->setTapeFull(m_admin, tape1.vid, true);
  m_catalogue->reclaimTape(m_admin, tape1.vid, dummyLc);
  {
    auto itor = m_catalogue->getFileRecycleLogItor();
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_P(cta_catalogue_CatalogueTest, emptyFileRecycleLogItorTest) {
  using namespace cta;
  auto itor = m_catalogue->getFileRecycleLogItor();
  ASSERT_FALSE(itor.hasMore());
  ASSERT_THROW(itor.next(),cta::exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, getFileRecycleLogItorVidNotExists) {
  using namespace cta;
  auto itor = m_catalogue->getFileRecycleLogItor();
  ASSERT_FALSE(m_catalogue->getFileRecycleLogItor().hasMore());

  catalogue::RecycleTapeFileSearchCriteria criteria;
  criteria.vid = "NOT_EXISTS";

  ASSERT_THROW(m_catalogue->getFileRecycleLogItor(criteria),exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, filesArePutInTheFileRecycleLogInsteadOfBeingSuperseded) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file"<<i;

    // Tape copy 1 written to tape
    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassSingleCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
  }
  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    ASSERT_TRUE(m_catalogue->getArchiveFilesItor().hasMore());
    ASSERT_FALSE(m_catalogue->getFileRecycleLogItor().hasMore());
  }
  log::LogContext dummyLc(m_dummyLog);
  //Archive the same files but in a new tape
  for(auto & tapeItemWritten: tapeFilesWrittenCopy1){
    cta::catalogue::TapeFileWritten * tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    tapeItem->vid = tape2.vid;
  }
  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  //Change the vid back to the first one to test the content of the file recycle log afterwards
  for(auto & tapeItemWritten: tapeFilesWrittenCopy1){
    cta::catalogue::TapeFileWritten * tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    tapeItem->vid = tape1.vid;
  }
  //Check that the new files written exist on the catalogue
  {
    auto archiveFilesItor = m_catalogue->getArchiveFilesItor();
    bool hasArchiveFilesItor = false;
    while(archiveFilesItor.hasMore()){
      hasArchiveFilesItor = true;
      //The vid is the destination one
      ASSERT_EQ(tape2.vid,archiveFilesItor.next().tapeFiles.at(1).vid);
    }
    ASSERT_TRUE(hasArchiveFilesItor);
  }
  //Check that the old files are in the file recycle logs
  std::list<common::dataStructures::FileRecycleLog> fileRecycleLogs;
  {
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor();
    while(fileRecycleLogItor.hasMore()){
      fileRecycleLogs.push_back(fileRecycleLogItor.next());
    }
    ASSERT_FALSE(fileRecycleLogs.empty());
    //Now we check the consistency of what is returned by the file recycle log
    for(auto fileRecycleLog: fileRecycleLogs){
      //First, get the correct file written to tape associated to the current fileRecycleLog
      auto tapeFilesWrittenCopy1Itor = std::find_if(tapeFilesWrittenCopy1.begin(), tapeFilesWrittenCopy1.end(),[fileRecycleLog](const cta::catalogue::TapeItemWrittenPointer & item){
        cta::catalogue::TapeFileWritten * fileWrittenPtr = static_cast<cta::catalogue::TapeFileWritten *>(item.get());
        return fileWrittenPtr->archiveFileId == fileRecycleLog.archiveFileId;
      });

      ASSERT_NE(tapeFilesWrittenCopy1.end(), tapeFilesWrittenCopy1Itor);
      cta::catalogue::TapeFileWritten * fileWrittenPtr = static_cast<cta::catalogue::TapeFileWritten *>(tapeFilesWrittenCopy1Itor->get());
      ASSERT_EQ(fileRecycleLog.vid,tape1.vid);
      ASSERT_EQ(fileRecycleLog.fSeq,fileWrittenPtr->fSeq);
      ASSERT_EQ(fileRecycleLog.blockId,fileWrittenPtr->blockId);
      ASSERT_EQ(fileRecycleLog.copyNb,fileWrittenPtr->copyNb);
      ASSERT_EQ(fileRecycleLog.archiveFileId,fileWrittenPtr->archiveFileId);
      ASSERT_EQ(fileRecycleLog.diskInstanceName,fileWrittenPtr->diskInstance);
      ASSERT_EQ(fileRecycleLog.diskFileId,fileWrittenPtr->diskFileId);
      ASSERT_EQ(fileRecycleLog.diskFileIdWhenDeleted,fileWrittenPtr->diskFileId);
      ASSERT_EQ(fileRecycleLog.diskFileUid,fileWrittenPtr->diskFileOwnerUid);
      ASSERT_EQ(fileRecycleLog.diskFileGid,fileWrittenPtr->diskFileGid);
      ASSERT_EQ(fileRecycleLog.sizeInBytes,fileWrittenPtr->size);
      ASSERT_EQ(fileRecycleLog.checksumBlob,fileWrittenPtr->checksumBlob);
      ASSERT_EQ(fileRecycleLog.storageClassName,fileWrittenPtr->storageClassName);
      ASSERT_EQ(fileRecycleLog.reconciliationTime,fileRecycleLog.archiveFileCreationTime);
      ASSERT_EQ(cta::nullopt, fileRecycleLog.collocationHint);
      ASSERT_EQ(cta::nullopt, fileRecycleLog.diskFilePath);
      ASSERT_EQ(cta::catalogue::InsertFileRecycleLog::getRepackReasonLog(),fileRecycleLog.reasonLog);
    }
  }
  {
    //Check the vid search criteria
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);
    int nbFileRecycleLogs = 0;
    while(fileRecycleLogItor.hasMore()){
      nbFileRecycleLogs++;
      fileRecycleLogItor.next();
    }
    ASSERT_EQ(nbArchiveFiles,nbFileRecycleLogs);
  }
  {
    //Check the diskFileId search criteria
    std::string diskFileId = "12345678";
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskFileIds = std::vector<std::string>();
    criteria.diskFileIds->push_back(diskFileId);
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_EQ(diskFileId,fileRecycleLog.diskFileId);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the non existing diskFileId search criteria
    std::string diskFileId = "DOES_NOT_EXIST";
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskFileIds = std::vector<std::string>();
    criteria.diskFileIds->push_back(diskFileId);
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the archiveID search criteria
    uint64_t archiveFileId = 1;
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.archiveFileId = archiveFileId;
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_EQ(archiveFileId,fileRecycleLog.archiveFileId);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the non existing archiveFileId search criteria
    uint64_t archiveFileId = -1;
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.archiveFileId = archiveFileId;
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the copynb search criteria
    uint64_t copynb = 1;
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.copynb = copynb;
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);
    int nbFileRecycleLogs = 0;
    while(fileRecycleLogItor.hasMore()){
      nbFileRecycleLogs++;
      fileRecycleLogItor.next();
    }
    ASSERT_EQ(nbArchiveFiles,nbFileRecycleLogs);
  }
  {
    //Check the disk instance search criteria
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskInstance = diskInstance;
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);
    int nbFileRecycleLogs = 0;
    while(fileRecycleLogItor.hasMore()){
      nbFileRecycleLogs++;
      fileRecycleLogItor.next();
    }
    ASSERT_EQ(nbArchiveFiles,nbFileRecycleLogs);
  }
  {
    //Check multiple search criteria together
    uint64_t copynb = 1;
    uint64_t archiveFileId = 1;
    std::string diskFileId = "12345678";
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskInstance = diskInstance;
    criteria.copynb = copynb;
    criteria.archiveFileId = archiveFileId;
    criteria.diskFileIds = std::vector<std::string>();
    criteria.diskFileIds->push_back(diskFileId);
    criteria.vid = tape1.vid;

    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor(criteria);

    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_EQ(archiveFileId, fileRecycleLog.archiveFileId);
    ASSERT_EQ(diskFileId, fileRecycleLog.diskFileId);
    ASSERT_EQ(copynb, fileRecycleLog.copyNb);
    ASSERT_EQ(tape1.vid, fileRecycleLog.vid);
    ASSERT_EQ(diskInstance, fileRecycleLog.diskInstanceName);

    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
}

TEST_P(cta_catalogue_CatalogueTest, sameFileWrittenToSameTapePutThePreviousCopyOnTheFileRecycleLog) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;

  m_catalogue->createTape(m_admin, tape1);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

  std::ostringstream diskFileId;
  diskFileId << 12345677;

  std::ostringstream diskFilePath;
  diskFilePath << "/test/file1";

  // Two files same archiveFileId and CopyNb on the same tape
  auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & fileWritten = *fileWrittenUP;
  fileWritten.archiveFileId = 1;
  fileWritten.diskInstance = diskInstance;
  fileWritten.diskFileId = diskFileId.str();
  fileWritten.diskFilePath = diskFilePath.str();
  fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
  fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
  fileWritten.size = archiveFileSize;
  fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
  fileWritten.storageClassName = m_storageClassSingleCopy.name;
  fileWritten.vid = tape1.vid;
  fileWritten.fSeq = 1;
  fileWritten.blockId = 1 * 100;
  fileWritten.copyNb = 1;
  fileWritten.tapeDrive = tapeDrive;
  tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);

  auto & tapeItemWritten =  *(tapeFilesWrittenCopy1.begin());
  cta::catalogue::TapeFileWritten * tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
  tapeItem->fSeq = 2;
  tapeItem->blockId = 2 *100 + 1;

  m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    auto archiveFilesItor = m_catalogue->getArchiveFilesItor();
    ASSERT_TRUE(archiveFilesItor.hasMore());
    //The file with fseq 2 is on the active archive files of CTA
    ASSERT_EQ(2,archiveFilesItor.next().tapeFiles.at(1).fSeq);
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    //The previous file (fSeq = 1) is on the recycle log
    ASSERT_EQ(1,fileRecycleLogItor.next().fSeq);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getTapeDriveNames) {
  using namespace cta;

  const std::list<std::string> tapeDriveNames = {"VDSTK11", "VDSTK12", "VDSTK21", "VDSTK22"};
  for (const auto& name : tapeDriveNames) {
    const auto tapeDrive = getTapeDriveWithMandatoryElements(name);
    m_catalogue->createTapeDrive(tapeDrive);
  }
  const auto storedTapeDriveNames = m_catalogue->getTapeDriveNames();
  ASSERT_EQ(tapeDriveNames, storedTapeDriveNames);
  for (const auto& name : tapeDriveNames) {
    m_catalogue->deleteTapeDrive(name);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getAllTapeDrives) {
  std::list<std::string> tapeDriveNames;
  // Create 100 tape drives
  for (size_t i = 0; i < 100; i++) {
    std::stringstream ss;
    ss << "VDSTK" << std::setw(5) << std::setfill('0') << i;
    tapeDriveNames.push_back(ss.str());
  }
  std::list<cta::common::dataStructures::TapeDrive> tapeDrives;
  for (const auto& name : tapeDriveNames) {
    const auto tapeDrive = getTapeDriveWithMandatoryElements(name);
    m_catalogue->createTapeDrive(tapeDrive);
    tapeDrives.push_back(tapeDrive);
  }
  auto storedTapeDrives = m_catalogue->getTapeDrives();
  ASSERT_EQ(tapeDriveNames.size(), storedTapeDrives.size());
  while (!storedTapeDrives.empty()) {
    const auto storedTapeDrive = storedTapeDrives.front();
    const auto tapeDrive = tapeDrives.front();
    storedTapeDrives.pop_front();
    tapeDrives.pop_front();
    ASSERT_EQ(tapeDrive, storedTapeDrive);
  }
  for (const auto& name : tapeDriveNames) {
    m_catalogue->deleteTapeDrive(name);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getTapeDrive) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  const auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive);
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, getTapeDriveWithEmptyEntryLog) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.creationLog = cta::common::dataStructures::EntryLog("", "", 0);
  m_catalogue->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive.value().creationLog);
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, failToGetTapeDrive) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(wrongName);
  ASSERT_FALSE(storedTapeDrive);
  m_catalogue->deleteTapeDrive(tapeDriveName);
}

TEST_P(cta_catalogue_CatalogueTest, failToDeleteTapeDrive) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->createTapeDrive(tapeDrive);
  m_catalogue->deleteTapeDrive(wrongName);
  auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  m_catalogue->deleteTapeDrive(tapeDriveName);
  storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive);
}

TEST_P(cta_catalogue_CatalogueTest, getTapeDriveWithAllElements) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  const auto tapeDrive = getTapeDriveWithAllElements(tapeDriveName);
  m_catalogue->createTapeDrive(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, multipleTapeDrives) {
  using namespace cta;

  const std::string tapeDriveName1 = "VDSTK11";
  const std::string tapeDriveName2 = "VDSTK12";
  const auto tapeDrive1 = getTapeDriveWithMandatoryElements(tapeDriveName1);
  const auto tapeDrive2 = getTapeDriveWithAllElements(tapeDriveName2);
  m_catalogue->createTapeDrive(tapeDrive1);
  m_catalogue->createTapeDrive(tapeDrive2);
  const auto storedTapeDrive1 = m_catalogue->getTapeDrive(tapeDrive1.driveName);
  const auto storedTapeDrive2 = m_catalogue->getTapeDrive(tapeDrive2.driveName);
  ASSERT_EQ(tapeDrive1, storedTapeDrive1);
  ASSERT_EQ(tapeDrive2, storedTapeDrive2);
  m_catalogue->deleteTapeDrive(tapeDrive1.driveName);
  m_catalogue->deleteTapeDrive(tapeDrive2.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, setDesiredStateEmpty) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.reasonUpDown = "Previous reason";
  m_catalogue->createTapeDrive(tapeDrive);
  common::dataStructures::DesiredDriveState desiredState;
  m_catalogue->setDesiredTapeDriveState(tapeDrive.driveName, desiredState);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive.value().reasonUpDown));
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value() ,tapeDrive.reasonUpDown.value());
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, setDesiredStateWithEmptyReason) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->createTapeDrive(tapeDrive);
  common::dataStructures::DesiredDriveState desiredState;
  desiredState.reason = "";
  m_catalogue->setDesiredTapeDriveState(tapeDrive.driveName, desiredState);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  // SqlLite (InMemory) returns an empty string and Oracle returns a nullopt
  if (storedTapeDrive.value().reasonUpDown) {
    ASSERT_TRUE(storedTapeDrive.value().reasonUpDown.value().empty());
  } else {
    ASSERT_FALSE(static_cast<bool>(storedTapeDrive.value().reasonUpDown));
  }
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, setDesiredState) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->createTapeDrive(tapeDrive);
  common::dataStructures::DesiredDriveState desiredState;
  desiredState.up = false;
  desiredState.forceDown = true;
  desiredState.reason = "reason";
  desiredState.comment = "comment";
  m_catalogue->setDesiredTapeDriveState(tapeDrive.driveName, desiredState);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().desiredUp , desiredState.up);
  ASSERT_EQ(storedTapeDrive.value().desiredForceDown , desiredState.forceDown);
  ASSERT_EQ(storedTapeDrive.value().reasonUpDown.value() , desiredState.reason);
  ASSERT_EQ(storedTapeDrive.value().userComment.value() , desiredState.comment);
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDriveStatistics) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Transferring;
  m_catalogue->createTapeDrive(tapeDrive);
  common::dataStructures::TapeDriveStatistics statistics;
  statistics.bytesTransferedInSession = 123456789;
  statistics.filesTransferedInSession = 987654321;
  statistics.lastModificationLog = common::dataStructures::EntryLog(
        "NO_USER", tapeDrive.host, time(nullptr));
  m_catalogue->updateTapeDriveStatistics(tapeDrive.driveName, tapeDrive.host, tapeDrive.logicalLibrary,
      statistics);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(storedTapeDrive.value().bytesTransferedInSession.value(), statistics.bytesTransferedInSession);
  ASSERT_EQ(storedTapeDrive.value().filesTransferedInSession.value(), statistics.filesTransferedInSession);
  ASSERT_EQ(storedTapeDrive.value().lastModificationLog.value() , statistics.lastModificationLog);
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDriveStatisticsInNoTransferingStatus) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;
  m_catalogue->createTapeDrive(tapeDrive);
  common::dataStructures::TapeDriveStatistics statistics;
  statistics.bytesTransferedInSession = 123456789;
  statistics.filesTransferedInSession = 987654321;
  statistics.lastModificationLog = common::dataStructures::EntryLog(
        "NO_USER", tapeDrive.host, time(nullptr));
  m_catalogue->updateTapeDriveStatistics(tapeDrive.driveName, tapeDrive.host, tapeDrive.logicalLibrary,
      statistics);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_FALSE(storedTapeDrive.value().bytesTransferedInSession);
  ASSERT_FALSE(storedTapeDrive.value().filesTransferedInSession);
  ASSERT_FALSE(storedTapeDrive.value().lastModificationLog);
  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusSameAsPrevious) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Up;
  m_catalogue->createTapeDrive(tapeDrive);
  // We update keeping the same status, so it has to update only the lastModificationLog
  common::dataStructures::TapeDrive newTapeDrive;
  newTapeDrive.driveName = tapeDrive.driveName;
  newTapeDrive.driveStatus = tapeDrive.driveStatus;
  // We use a different MountType to check it doesn't update this value in the database
  newTapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  newTapeDrive.host = tapeDrive.host;
  newTapeDrive.logicalLibrary = tapeDrive.logicalLibrary;
  // bytesTransferedInSession and filesTransferedInSession shouldn't be updated in DB for status different of transfering
  newTapeDrive.bytesTransferedInSession = 123456;
  newTapeDrive.filesTransferedInSession = 987654;
  newTapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  m_catalogue->updateTapeDriveStatus(newTapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(newTapeDrive.driveName, storedTapeDrive.value().driveName);
  ASSERT_EQ(newTapeDrive.driveStatus, storedTapeDrive.value().driveStatus);
  ASSERT_NE(newTapeDrive.mountType, storedTapeDrive.value().mountType);  // Not update this value
  ASSERT_EQ(newTapeDrive.host, storedTapeDrive.value().host);
  ASSERT_EQ(newTapeDrive.logicalLibrary, storedTapeDrive.value().logicalLibrary);
  ASSERT_EQ(newTapeDrive.lastModificationLog.value(), storedTapeDrive.value().lastModificationLog.value());
  ASSERT_FALSE(storedTapeDrive.value().bytesTransferedInSession);
  ASSERT_FALSE(storedTapeDrive.value().filesTransferedInSession);
  // Disk reservations are not updated by updateTapeDriveStatus()
  ASSERT_EQ(tapeDrive.diskSystemName, storedTapeDrive.value().diskSystemName);
  ASSERT_EQ(tapeDrive.reservedBytes, storedTapeDrive.value().reservedBytes);

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusSameTransferingAsPrevious) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Transferring;
  m_catalogue->createTapeDrive(tapeDrive);
  // We update keeping the same status, so it has to update only the lastModificationLog
  common::dataStructures::TapeDrive newTapeDrive;
  newTapeDrive.driveName = tapeDrive.driveName;
  newTapeDrive.driveStatus = tapeDrive.driveStatus;
  // We use a different MountType to check it doesn't update this value in the database
  newTapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  newTapeDrive.host = tapeDrive.host;
  newTapeDrive.logicalLibrary = tapeDrive.logicalLibrary;
  newTapeDrive.bytesTransferedInSession = 123456;
  newTapeDrive.filesTransferedInSession = 987654;
  newTapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  m_catalogue->updateTapeDriveStatus(newTapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_TRUE(static_cast<bool>(storedTapeDrive));
  ASSERT_EQ(newTapeDrive.driveName, storedTapeDrive.value().driveName);
  ASSERT_EQ(newTapeDrive.driveStatus, storedTapeDrive.value().driveStatus);
  ASSERT_NE(newTapeDrive.mountType, storedTapeDrive.value().mountType);  // Not update this value
  ASSERT_EQ(newTapeDrive.host, storedTapeDrive.value().host);
  ASSERT_EQ(newTapeDrive.logicalLibrary, storedTapeDrive.value().logicalLibrary);
  ASSERT_EQ(newTapeDrive.lastModificationLog.value(), storedTapeDrive.value().lastModificationLog.value());
  ASSERT_EQ(newTapeDrive.bytesTransferedInSession.value(), storedTapeDrive.value().bytesTransferedInSession.value());
  ASSERT_EQ(newTapeDrive.filesTransferedInSession.value(), storedTapeDrive.value().filesTransferedInSession.value());
  // It will keep names and bytes, because it isn't in state UP
  ASSERT_NE(storedTapeDrive.value().reservedBytes, 0);
  ASSERT_NE(storedTapeDrive.value().diskSystemName, "");

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusDown) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  m_catalogue->createTapeDrive(tapeDrive);

  tapeDrive.sessionId = 0;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  tapeDrive.sessionStartTime = 0;
  tapeDrive.mountStartTime = 0;
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = time(nullptr);
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::NoMount;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;
  tapeDrive.desiredUp = false;
  tapeDrive.desiredForceDown = false;
  tapeDrive.currentVid = "";
  tapeDrive.currentTapePool = "";
  tapeDrive.currentVo = "";
  tapeDrive.currentActivity = nullopt_t();
  tapeDrive.reasonUpDown = "testing";

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusUp) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  // If we are changing state, then all should be reset.
  tapeDrive.sessionId = 0;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  tapeDrive.sessionStartTime = 0;
  tapeDrive.mountStartTime = 0;
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = time(nullptr);
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::NoMount;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Up;
  tapeDrive.currentVid = "";
  tapeDrive.currentTapePool = "";
  tapeDrive.currentVo = "";
  tapeDrive.currentActivity = nullopt_t();

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusProbing) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  // If we are changing state, then all should be reset.
  tapeDrive.sessionId = 0;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  tapeDrive.sessionStartTime = 0;
  tapeDrive.mountStartTime = 0;
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = 0;
  tapeDrive.probeStartTime = time(nullptr);
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::NoMount;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Probing;
  tapeDrive.currentVid = "";
  tapeDrive.currentTapePool = "";
  tapeDrive.currentVo = "";
  tapeDrive.currentActivity = nullopt_t();

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusStarting) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  // If we are changing state, then all should be reset.
  tapeDrive.sessionId = 1234;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  tapeDrive.sessionStartTime = time(nullptr);
  tapeDrive.mountStartTime = 0;
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = 0;
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.startStartTime = time(nullptr);
  tapeDrive.lastModificationLog = tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Starting;
  tapeDrive.currentVid = "vid";
  tapeDrive.currentTapePool = "tapepool";
  tapeDrive.currentVo = "vo";
  tapeDrive.currentActivity = "activity";

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusMounting) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  tapeDrive.sessionId = 1234;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  // driveState.sessionstarttime = inputs.reportTime;
  tapeDrive.mountStartTime = time(nullptr);
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = 0;
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Mounting;
  tapeDrive.currentVid = "vid";
  tapeDrive.currentTapePool = "tapepool";
  tapeDrive.currentVo = "vo";

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusTransfering) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  tapeDrive.sessionId = 1234;
  tapeDrive.bytesTransferedInSession = 123456;
  tapeDrive.filesTransferedInSession = 987654;
  // tapeDrive.sessionstarttime = inputs.reportTime;
  // tapeDrive.mountstarttime = inputs.reportTime;
  tapeDrive.transferStartTime = time(nullptr);
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = 0;
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Transferring;
  tapeDrive.currentVid = "vid";
  tapeDrive.currentTapePool = "tapepool";
  tapeDrive.currentVo = "vo";

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusUnloading) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  tapeDrive.sessionId = 12354;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  tapeDrive.sessionStartTime = 0;
  tapeDrive.mountStartTime = 0;
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = time(nullptr);
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = 0;
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Unloading;
  tapeDrive.currentVid = "vid";
  tapeDrive.currentTapePool = "tapepool";
  tapeDrive.currentVo = "vo";

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusUnmounting) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  tapeDrive.sessionId = 1234;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  tapeDrive.sessionStartTime = 0;
  tapeDrive.mountStartTime = 0;
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = time(nullptr);
  tapeDrive.drainingStartTime = 0;
  tapeDrive.downOrUpStartTime = 0;
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Unmounting;
  tapeDrive.currentVid = "vid";
  tapeDrive.currentTapePool = "tapepool";
  tapeDrive.currentVo = "vo";

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, updateTapeDriveStatusDrainingToDisk) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  auto tapeDrive = getTapeDriveWithMandatoryElements(tapeDriveName);
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::Down;  // To force a change of state
  m_catalogue->createTapeDrive(tapeDrive);

  // If we are changing state, then all should be reset. We are not supposed to
  // know the direction yet.
  tapeDrive.sessionId = 1234;
  tapeDrive.bytesTransferedInSession = 0;
  tapeDrive.filesTransferedInSession = 0;
  tapeDrive.sessionStartTime = 0;
  tapeDrive.mountStartTime = 0;
  tapeDrive.transferStartTime = 0;
  tapeDrive.unloadStartTime = 0;
  tapeDrive.unmountStartTime = 0;
  tapeDrive.drainingStartTime = time(nullptr);
  tapeDrive.downOrUpStartTime = 0;
  tapeDrive.probeStartTime = 0;
  tapeDrive.cleanupStartTime = 0;
  tapeDrive.shutdownTime = 0;
  tapeDrive.lastModificationLog = tapeDrive.lastModificationLog = common::dataStructures::EntryLog(
    "NO_USER", tapeDrive.host, time(nullptr));
  tapeDrive.mountType = common::dataStructures::MountType::ArchiveForUser;
  tapeDrive.driveStatus = common::dataStructures::DriveStatus::DrainingToDisk;
  tapeDrive.currentVid = "vid";
  tapeDrive.currentTapePool = "tapepool";
  tapeDrive.currentVo = "vo";

  m_catalogue->updateTapeDriveStatus(tapeDrive);
  const auto storedTapeDrive = m_catalogue->getTapeDrive(tapeDrive.driveName);
  ASSERT_EQ(tapeDrive, storedTapeDrive.value());

  m_catalogue->deleteTapeDrive(tapeDrive.driveName);
}

TEST_P(cta_catalogue_CatalogueTest, getTapeDriveConfig) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";

  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};

  m_catalogue->createTapeDriveConfig(tapeDriveName, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  auto driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig.value();
  ASSERT_EQ(daemonUserName.category(), category);
  ASSERT_EQ(daemonUserName.value(), value);
  ASSERT_EQ(daemonUserName.source(), source);
  m_catalogue->deleteTapeDriveConfig(tapeDriveName, daemonUserName.key());
}

TEST_P(cta_catalogue_CatalogueTest, getAllDrivesConfigs) {
  std::list<cta::catalogue::Catalogue::DriveConfig> tapeDriveConfigs;
  // Create 100 tape drives
  for (size_t i = 0; i < 100; i++) {
    std::stringstream ss;
    ss << "VDSTK" << std::setw(5) << std::setfill('0') << i;

    cta::SourcedParameter<std::string> daemonUserName {
      "taped", "DaemonUserName", "cta", "Compile time default"};
    m_catalogue->createTapeDriveConfig(ss.str(), daemonUserName.category(), daemonUserName.key(),
      daemonUserName.value(), daemonUserName.source());
    tapeDriveConfigs.push_back({ss.str(), daemonUserName.category(), daemonUserName.key(), daemonUserName.value(),
      daemonUserName.source()});
    cta::SourcedParameter<std::string> defaultConfig {
      "taped", "defaultConfig", "cta", "Random Default Config for Testing"};
    m_catalogue->createTapeDriveConfig(ss.str(), defaultConfig.category(), defaultConfig.key(),
      defaultConfig.value(), defaultConfig.source());
    tapeDriveConfigs.push_back({ss.str(), defaultConfig.category(), defaultConfig.key(), defaultConfig.value(),
      defaultConfig.source()});
  }
  const auto drivesConfigs = m_catalogue->getTapeDriveConfigs();
  ASSERT_EQ(tapeDriveConfigs.size(), drivesConfigs.size());
  for (const auto& dc : drivesConfigs) {
    m_catalogue->deleteTapeDriveConfig(dc.tapeDriveName, dc.keyName);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setSourcedParameterWithEmptyValue) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";

  cta::SourcedParameter<std::string> raoLtoOptions {
    "taped", "RAOLTOAlgorithmOptions","","Compile time default"
  };

  m_catalogue->createTapeDriveConfig(tapeDriveName, raoLtoOptions.category(), raoLtoOptions.key(),
    raoLtoOptions.value(), raoLtoOptions.source());
  auto driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, raoLtoOptions.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig.value();
  ASSERT_EQ(raoLtoOptions.category(), category);
  ASSERT_EQ("", value);
  ASSERT_EQ(raoLtoOptions.source(), source);
  m_catalogue->deleteTapeDriveConfig(tapeDriveName, raoLtoOptions.key());

  cta::SourcedParameter<std::string> backendPath{
    "ObjectStore", "BackendPath"};

  m_catalogue->createTapeDriveConfig(tapeDriveName, backendPath.category(), backendPath.key(),
    backendPath.value(), backendPath.source());
  driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, backendPath.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  std::tie(category, value, source) = driveConfig.value();
  ASSERT_EQ(backendPath.category(), category);
  ASSERT_EQ("", value);
  ASSERT_EQ("", source);
  m_catalogue->deleteTapeDriveConfig(tapeDriveName, backendPath.key());
}

TEST_P(cta_catalogue_CatalogueTest, failTogetTapeDriveConfig) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const std::string wrongKey = "wrongKey";
  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};

  m_catalogue->createTapeDriveConfig(tapeDriveName, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  auto driveConfig = m_catalogue->getTapeDriveConfig(wrongName, daemonUserName.key());
  ASSERT_FALSE(driveConfig);
  driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, wrongKey);
  ASSERT_FALSE(driveConfig);
  driveConfig = m_catalogue->getTapeDriveConfig(wrongName, wrongKey);
  ASSERT_FALSE(driveConfig);
  m_catalogue->deleteTapeDriveConfig(tapeDriveName, daemonUserName.key());
}

TEST_P(cta_catalogue_CatalogueTest, failTodeleteTapeDriveConfig) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const std::string wrongKey = "wrongKey";
  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};
  m_catalogue->createTapeDriveConfig(tapeDriveName, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->deleteTapeDriveConfig(wrongName, daemonUserName.key());
  auto driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  m_catalogue->deleteTapeDriveConfig(tapeDriveName, wrongKey);
  driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  m_catalogue->deleteTapeDriveConfig(wrongName, wrongKey);
  driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  // Good deletion
  m_catalogue->deleteTapeDriveConfig(tapeDriveName, daemonUserName.key());
  driveConfig = m_catalogue->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_FALSE(driveConfig);
}

TEST_P(cta_catalogue_CatalogueTest, multipleDriveConfig) {
  using namespace cta;

  const std::string tapeDriveName1 = "VDSTK11";
  const std::string tapeDriveName2 = "VDSTK12";

  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};
  cta::SourcedParameter<std::string> daemonGroupName {
    "taped", "DaemonGroupName", "tape", "Compile time default"};

  // Combinations of tapeDriveName1/2 and daemonUserName and daemonGroupName
  m_catalogue->createTapeDriveConfig(tapeDriveName1, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->createTapeDriveConfig(tapeDriveName1, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());
  m_catalogue->createTapeDriveConfig(tapeDriveName2, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->createTapeDriveConfig(tapeDriveName2, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());
  auto driveConfig1UserName = m_catalogue->getTapeDriveConfig(tapeDriveName1, daemonUserName.key());
  auto driveConfig2UserName = m_catalogue->getTapeDriveConfig(tapeDriveName2, daemonUserName.key());
  auto driveConfig1GroupName = m_catalogue->getTapeDriveConfig(tapeDriveName1, daemonGroupName.key());
  auto driveConfig2GroupName = m_catalogue->getTapeDriveConfig(tapeDriveName2, daemonGroupName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig1UserName));
  ASSERT_TRUE(static_cast<bool>(driveConfig2UserName));
  ASSERT_TRUE(static_cast<bool>(driveConfig1GroupName));
  ASSERT_TRUE(static_cast<bool>(driveConfig2GroupName));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig1UserName.value();
  ASSERT_EQ(daemonUserName.category(), category);
  ASSERT_EQ(daemonUserName.value(), value);
  ASSERT_EQ(daemonUserName.source(), source);
  std::tie(category, value, source) = driveConfig2UserName.value();
  ASSERT_EQ(daemonUserName.category(), category);
  ASSERT_EQ(daemonUserName.value(), value);
  ASSERT_EQ(daemonUserName.source(), source);
  std::tie(category, value, source) = driveConfig1GroupName.value();
  ASSERT_EQ(daemonGroupName.category(), category);
  ASSERT_EQ(daemonGroupName.value(), value);
  ASSERT_EQ(daemonGroupName.source(), source);
  std::tie(category, value, source) = driveConfig2GroupName.value();
  ASSERT_EQ(daemonGroupName.category(), category);
  ASSERT_EQ(daemonGroupName.value(), value);
  ASSERT_EQ(daemonGroupName.source(), source);
  m_catalogue->deleteTapeDriveConfig(tapeDriveName1, daemonUserName.key());
  m_catalogue->deleteTapeDriveConfig(tapeDriveName1, daemonGroupName.key());
  m_catalogue->deleteTapeDriveConfig(tapeDriveName2, daemonUserName.key());
  m_catalogue->deleteTapeDriveConfig(tapeDriveName2, daemonGroupName.key());
}

TEST_P(cta_catalogue_CatalogueTest, getNamesAndKeysOfMultipleDriveConfig) {
  using namespace cta;

  const std::string tapeDriveName1 = "VDSTK11";
  const std::string tapeDriveName2 = "VDSTK12";

  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};
  cta::SourcedParameter<std::string> daemonGroupName {
    "taped", "DaemonGroupName", "tape", "Compile time default"};

  // Combinations of tapeDriveName1/2 and daemonUserName and daemonGroupName
  m_catalogue->createTapeDriveConfig(tapeDriveName1, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->createTapeDriveConfig(tapeDriveName1, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());
  m_catalogue->createTapeDriveConfig(tapeDriveName2, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->createTapeDriveConfig(tapeDriveName2, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());

  const auto configurationTapeNamesAndKeys = m_catalogue->getTapeDriveConfigNamesAndKeys();

  for (const auto& nameAndKey : configurationTapeNamesAndKeys) {
    m_catalogue->deleteTapeDriveConfig(nameAndKey.first, nameAndKey.second);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeDriveConfig) {
  using namespace cta;

  const std::string tapeDriveName = "VDSTK11";
  // Both share same key
  cta::SourcedParameter<std::string> daemonUserName1 {
    "taped1", "DaemonUserName", "cta1", "Compile time1 default"};
  cta::SourcedParameter<std::string> daemonUserName2 {
    "taped2", "DaemonUserName", "cta2", "Compile time2 default"};

  m_catalogue->createTapeDriveConfig(tapeDriveName, daemonUserName1.category(), daemonUserName1.key(),
    daemonUserName1.value(), daemonUserName1.source());
  const auto driveConfig1 = m_catalogue->getTapeDriveConfig(tapeDriveName, daemonUserName1.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig1));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig1.value();
  ASSERT_NE(daemonUserName2.category(), category);
  ASSERT_NE(daemonUserName2.value(), value);
  ASSERT_NE(daemonUserName2.source(), source);
  m_catalogue->modifyTapeDriveConfig(tapeDriveName, daemonUserName2.category(), daemonUserName2.key(),
    daemonUserName2.value(), daemonUserName2.source());
  const auto driveConfig2 = m_catalogue->getTapeDriveConfig(tapeDriveName, daemonUserName1.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig2));
  std::tie(category, value, source) = driveConfig2.value();
  ASSERT_EQ(daemonUserName2.category(), category);
  ASSERT_EQ(daemonUserName2.value(), value);
  ASSERT_EQ(daemonUserName2.source(), source);
  m_catalogue->deleteTapeDriveConfig(tapeDriveName, daemonUserName1.key());
}

TEST_P(cta_catalogue_CatalogueTest, DeleteTapeFileCopyUsingArchiveID) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.archiveFileId = 1;
    auto archiveFileForDeletion = m_catalogue->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    //The deleted file (fSeq = 1) is on the recycle log
    auto recycleFileLog = fileRecycleLogItor.next();
    ASSERT_EQ(1, recycleFileLog.fSeq);
    ASSERT_EQ(tape1.vid, recycleFileLog.vid);
    ASSERT_EQ(1, recycleFileLog.archiveFileId);
    ASSERT_EQ(1, recycleFileLog.copyNb);
    ASSERT_EQ(1 * 100, recycleFileLog.blockId);
    ASSERT_EQ("(Deleted using cta-admin tf rm) " + reason, recycleFileLog.reasonLog);
    ASSERT_EQ(std::string("/test/file1"), recycleFileLog.diskFilePath.value());
  }

  {
    //get last archive file copy for deletions should fail
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.archiveFileId = 1;
    ASSERT_THROW(m_catalogue->getArchiveFileForDeletion(criteria), exception::UserError);
  }
}

TEST_P(cta_catalogue_CatalogueTest, DeleteTapeFileCopyDoesNotExist) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  {
    //delete copy of file that does not exist should fail
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.archiveFileId = 1;
    ASSERT_THROW(m_catalogue->getArchiveFileForDeletion(criteria), exception::UserError);
  }
}

TEST_P(cta_catalogue_CatalogueTest, DeleteTapeFileCopyUsingFXID) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    //The previous file (fSeq = 1) is on the recycle log
    auto recycleFileLog = fileRecycleLogItor.next();
    ASSERT_EQ(1, recycleFileLog.fSeq);
    ASSERT_EQ(tape1.vid, recycleFileLog.vid);
    ASSERT_EQ(1, recycleFileLog.archiveFileId);
    ASSERT_EQ(1, recycleFileLog.copyNb);
    ASSERT_EQ(1 * 100, recycleFileLog.blockId);
    ASSERT_EQ("(Deleted using cta-admin tf rm) " + reason, recycleFileLog.reasonLog);
    ASSERT_EQ(std::string("/test/file1"), recycleFileLog.diskFilePath.value());
  }

  {
    //delete last copy of file should fail
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    ASSERT_THROW(m_catalogue->getArchiveFileForDeletion(criteria), exception::UserError);
  }
}

TEST_P(cta_catalogue_CatalogueTest, RestoreTapeFileCopy) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
  }


  {
    //restore copy of file on tape1
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.vid = tape1.vid;

    m_catalogue->restoreFileInRecycleLog(searchCriteria, "0"); //new FID does not matter because archive file still exists in catalogue
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    //assert both copies present
    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    //assert recycle log is empty
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor();
    ASSERT_FALSE(fileRecycleLogItor.hasMore());

  }
}

TEST_P(cta_catalogue_CatalogueTest, RestoreRewrittenTapeFileCopyFails) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1

    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
  }

    // Rewrite deleted copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 2;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  {
    //restore copy of file on tape1
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.vid = tape1.vid;

    ASSERT_THROW(m_catalogue->restoreFileInRecycleLog(searchCriteria, "0"), catalogue::UserSpecifiedExistingDeletedFileCopy);
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    //assert only two copies present
    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    //assert recycle log still contains deleted copy
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());

  }
}

TEST_P(cta_catalogue_CatalogueTest, RestoreVariousDeletedTapeFileCopies) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const std::string tapePoolName3 = "tape_pool_name_3";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName3, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassTripleCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  auto tape3 = m_tape3;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;
  tape3.tapePoolName = tapePoolName3;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);
  m_catalogue->createTape(m_admin, tape3);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassTripleCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassTripleCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a third copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassTripleCopy.name;
    fileWritten.vid = tape3.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 3;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert all copies written
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(3, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape2
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
  }


  {
    //try to restore all deleted copies should give an error
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;

    ASSERT_THROW(m_catalogue->restoreFileInRecycleLog(searchCriteria, "0"), cta::exception::UserError);

  }
}

TEST_P(cta_catalogue_CatalogueTest, RestoreArchiveFileAndCopy) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = "disk_instance";
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

  m_catalogue->createMediaType(m_admin, m_mediaType);
  m_catalogue->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->createTape(m_admin, tape1);
  m_catalogue->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete archive file
    common::dataStructures::DeleteArchiveRequest deleteRequest;
    deleteRequest.archiveFileID = 1;
    deleteRequest.archiveFile = m_catalogue->getArchiveFileById(1);
    deleteRequest.diskInstance = diskInstance;
    deleteRequest.diskFileId = std::to_string(12345677);
    deleteRequest.diskFilePath = "/test/file1";

    log::LogContext dummyLc(m_dummyLog);
    m_catalogue->moveArchiveFileToRecycleLog(deleteRequest, dummyLc);
    ASSERT_THROW(m_catalogue->getArchiveFileById(1), cta::exception::Exception);
  }


  {
    //restore copy of file on tape1
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.vid = tape1.vid;

    m_catalogue->restoreFileInRecycleLog(searchCriteria, std::to_string(12345678)); //previous fid + 1

    //assert archive file has been restored in the catalogue
    auto archiveFile = m_catalogue->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    ASSERT_EQ(archiveFile.diskFileId, std::to_string(12345678));
    ASSERT_EQ(archiveFile.diskInstance, diskInstance);
    ASSERT_EQ(archiveFile.storageClass, m_storageClassDualCopy.name);

    //assert recycle log has the other tape file copy
    auto fileRecycleLogItor = m_catalogue->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_FALSE(fileRecycleLogItor.hasMore());

  }
}


} // namespace unitTests
