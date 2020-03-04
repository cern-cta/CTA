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

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/CatalogueTest.hpp"
#include "catalogue/UserSpecifiedANonEmptyLogicalLibrary.hpp"
#include "catalogue/UserSpecifiedANonEmptyTape.hpp"
#include "catalogue/UserSpecifiedANonExistentLogicalLibrary.hpp"
#include "catalogue/UserSpecifiedANonExistentTape.hpp"
#include "catalogue/UserSpecifiedANonExistentDiskSystem.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringDiskSystemName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringFileRegexp.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringFreeSpaceQueryURL.hpp"
#include "catalogue/UserSpecifiedAZeroRefreshInterval.hpp"
#include "catalogue/UserSpecifiedAZeroTargetedFreeSpace.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringComment.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringDiskInstanceName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringLogicalLibraryName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringMediaType.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringStorageClassName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringTapePoolName.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringUsername.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringVendor.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringVid.hpp"
#include "catalogue/UserSpecifiedAnEmptyStringVo.hpp"
#include "catalogue/UserSpecifiedAnEmptyTapePool.hpp"
#include "catalogue/UserSpecifiedAZeroCapacity.hpp"
#include "catalogue/UserSpecifiedAZeroCopyNb.hpp"
#include "catalogue/UserSpecifiedStorageClassUsedByArchiveFiles.hpp"
#include "catalogue/UserSpecifiedStorageClassUsedByArchiveRoutes.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/Constants.hpp"
#include "rdbms/wrapper/ConnFactoryFactory.hpp"
#include "rdbms/Conn.hpp"
#include "common/threading/Thread.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"

#include <algorithm>
#include <gtest/gtest.h>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <set>

namespace unitTests {

const uint32_t PUBLIC_DISK_USER = 9751;
const uint32_t PUBLIC_DISK_GROUP = 9752;
const uint32_t DISK_FILE_OWNER_UID = 9753;
const uint32_t DISK_FILE_GID = 9754;
const uint32_t NON_EXISTENT_DISK_FILE_OWNER_UID = 9755;
const uint32_t NON_EXISTENT_DISK_FILE_GID = 9756;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta_catalogue_CatalogueTest::cta_catalogue_CatalogueTest():
  m_dummyLog("dummy", "dummy") {
  m_localAdmin.username = "local_admin_user";
  m_localAdmin.host = "local_admin_host";

  m_admin.username = "admin_user_name";
  m_admin.host = "admin_host";
}

//------------------------------------------------------------------------------
// Setup
//------------------------------------------------------------------------------
void cta_catalogue_CatalogueTest::SetUp() {
  using namespace cta;
  using namespace cta::catalogue;

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
        log::LogContext dummyLc(m_dummyLog);
        m_catalogue->deleteArchiveFile(archiveFile.diskInstance, archiveFile.archiveFileID, dummyLc);
      }
    }
    {
      const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
      for(auto &tape: tapes) {
        m_catalogue->deleteTape(tape.vid);
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
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// TearDown
//------------------------------------------------------------------------------
void cta_catalogue_CatalogueTest::TearDown() {
  m_catalogue.reset();
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
  cta::catalogue::ArchiveFileItor &itor) {
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

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

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

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  m_catalogue->createAdminUser(m_localAdmin, m_admin.username, "comment 1");

  ASSERT_THROW(m_catalogue->createAdminUser(m_localAdmin, m_admin.username,
    "comment 2"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminUser) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

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

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  const std::string adminUsername = "";
  const std::string createAdminUserComment = "Create admin user";
  ASSERT_THROW(m_catalogue->createAdminUser(m_localAdmin, adminUsername, createAdminUserComment),
    catalogue::UserSpecifiedAnEmptyStringUsername);
}

TEST_P(cta_catalogue_CatalogueTest, createAdminUser_emptyStringComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  const std::string createAdminUserComment = "";
  ASSERT_THROW(m_catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, deleteAdminUser_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());
  ASSERT_THROW(m_catalogue->deleteAdminUser("non_existent_admin_user"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

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

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

  const std::string adminUsername = "";
  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->modifyAdminUserComment(m_localAdmin, adminUsername, modifiedComment),
    catalogue::UserSpecifiedAnEmptyStringUsername);
}

TEST_P(cta_catalogue_CatalogueTest, modifyAdminUserComment_emptyStringComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

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

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

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

  ASSERT_TRUE(m_catalogue->getAdminUsers().empty());

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

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::list<common::dataStructures::StorageClass> storageClasses =
    m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
  ASSERT_EQ(storageClass.name, storageClasses.front().name);
  ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

  const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_same_twice) {
  using namespace cta;

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_emptyStringDiskInstanceName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass),
    catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_emptyStringStorageClassName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass),
    catalogue::UserSpecifiedAnEmptyStringStorageClassName);
}

TEST_P(cta_catalogue_CatalogueTest, createStorageClass_emptyStringComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "";
  ASSERT_THROW(m_catalogue->createStorageClass(m_admin, storageClass),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, deleteStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::list<common::dataStructures::StorageClass> storageClasses =
    m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
  ASSERT_EQ(storageClass.name, storageClasses.front().name);
  ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

  const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteStorageClass(storageClass.name);
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteStorageClass_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_THROW(m_catalogue->deleteStorageClass("non_existent_storage_class"),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassNbCopies) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedNbCopies = 5;
  m_catalogue->modifyStorageClassNbCopies(m_admin, storageClass.name, modifiedNbCopies);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(modifiedNbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassNbCopies_nonExistentStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  const std::string diskInstance = "disk_instance";
  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 5;
  ASSERT_THROW(m_catalogue->modifyStorageClassNbCopies(m_admin, storageClassName, nbCopies),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyStorageClassComment(m_admin, storageClass.name, modifiedComment);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(modifiedComment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassComment_nonExistentStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  const std::string diskInstance = "disk_instance";
  const std::string storageClassName = "storage_class";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyStorageClassComment(m_admin, storageClassName, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(storageClass.name, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newStorageClassName = "new_storage_class_name";
  m_catalogue->modifyStorageClassName(m_admin, storageClass.name, newStorageClassName);

  {
    const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());

    ASSERT_EQ(storageClass.diskInstance, storageClasses.front().diskInstance);
    ASSERT_EQ(newStorageClassName, storageClasses.front().name);
    ASSERT_EQ(storageClass.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(storageClass.comment, storageClasses.front().comment);

    const common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassName_nonExistentStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  const std::string diskInstance = "disk_instance";
  const std::string currentStorageClassName = "storage_class";
  const std::string newStorageClassName = "new_storage_class";
  ASSERT_THROW(m_catalogue->modifyStorageClassName(m_admin, currentStorageClassName, newStorageClassName),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyStorageClassName_newNameAlreadyExists) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  
  m_catalogue->createStorageClass(m_admin,storageClass);
  
  common::dataStructures::StorageClass storageClass2;
  storageClass2.diskInstance = "disk_instance";
  storageClass2.name = "storage_class2";
  storageClass2.nbCopies = 2;
  storageClass2.comment = "Create storage class";
  
  m_catalogue->createStorageClass(m_admin,storageClass2);
  
  //Try to rename the first storage class with the name of the second one
  ASSERT_THROW(m_catalogue->modifyStorageClassName(m_admin,storageClass.name,storageClass2.name),exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  ASSERT_TRUE(m_catalogue->tapePoolExists(tapePoolName));
      
  const auto pools = m_catalogue->getTapePools();
      
  ASSERT_EQ(1, pools.size());
      
  const auto &pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(vo, pool.vo);
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
  
  const common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_null_supply) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply;
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  ASSERT_TRUE(m_catalogue->tapePoolExists(tapePoolName));
      
  const auto pools = m_catalogue->getTapePools();
      
  ASSERT_EQ(1, pools.size());
      
  const auto &pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(vo, pool.vo);
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
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  const auto pools = m_catalogue->getTapePools();

  ASSERT_EQ(1, pools.size());

  const auto &pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(vo, pool.vo);
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

  const common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteTapePool(pool.name);
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool_notEmpty) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  ASSERT_TRUE(m_catalogue->tapeExists(vid));

  const auto tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->deleteTapePool(tapePoolName), catalogue::UserSpecifiedAnEmptyTapePool);
  ASSERT_THROW(m_catalogue->deleteTapePool(tapePoolName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_emptyStringTapePoolName) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_emptyStringVO) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const std::string vo = "";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment),
    catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_CatalogueTest, createTapePool_emptyStringComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->tapePoolExists(tapePoolName));

  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "";
  ASSERT_THROW(m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment),
    catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, deleteTapePool_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_THROW(m_catalogue->deleteTapePool("non_existent_tape_pool"), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolVo) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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

  const std::string modifiedVo = "modified_vo";
  m_catalogue->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(modifiedVo, pool.vo);
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

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "";
  const std::string modifiedVo = "modified_vo";
  ASSERT_THROW(m_catalogue->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolVo_emptyStringVo) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolNbPartialTapes) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());
      
    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
    ASSERT_EQ(vo, pool.vo);
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
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "";
  const uint64_t modifiedNbPartialTapes = 5;
  ASSERT_THROW(m_catalogue->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, modifiedNbPartialTapes),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolNbPartialTapes_nonExistentTapePool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 5;
  ASSERT_THROW(m_catalogue->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, nbPartialTapes),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
    ASSERT_EQ(vo, pool.vo);
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
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "";
  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->modifyTapePoolComment(m_admin, tapePoolName, modifiedComment),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolComment_emptyStringComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyTapePoolComment(m_admin, tapePoolName, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapePoolEncryption) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
    ASSERT_EQ(vo, pool.vo);
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
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const bool isEncrypted = false;
  ASSERT_THROW(m_catalogue->setTapePoolEncryption(m_admin, tapePoolName, isEncrypted), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolSupply) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
    ASSERT_EQ(vo, pool.vo);
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
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "";
  const std::string modifiedSupply = "Modified supply";
  ASSERT_THROW(m_catalogue->modifyTapePoolSupply(m_admin, tapePoolName, modifiedSupply),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolSupply_emptyStringSupply) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();
      
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
    ASSERT_EQ(vo, pool.vo);
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
      
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
      
  const std::string tapePoolName = "tape_pool";
  const std::string supply = "value for the supply pool mechanism";
  ASSERT_THROW(m_catalogue->modifyTapePoolSupply(m_admin, tapePoolName, supply), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  ASSERT_TRUE(m_catalogue->getArchiveRoutes(storageClass.name, tapePoolName).empty());

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName, comment);
      
  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.name, route.storageClassName);
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
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes(
      storageClass.name, tapePoolName);
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.name, route.storageClassName);
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

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
    ASSERT_EQ(vo, pool.vo);
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

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "";
  const std::string newTapePoolName = "new_tape_pool";
  ASSERT_THROW(m_catalogue->modifyTapePoolName(m_admin, tapePoolName, newTapePoolName),
    catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapePoolName_emptyStringNewTapePoolName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapePools().empty());

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, comment);

  {
    const auto pools = m_catalogue->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
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
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string storageClassName = "";
  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClassName, copyNb,
   tapePoolName, comment), catalogue::UserSpecifiedAnEmptyStringStorageClassName);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_zeroCopyNb) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 0;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb,
    tapePoolName, comment), catalogue::UserSpecifiedAZeroCopyNb);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_emptyStringTapePoolName) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "";
  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb,
    tapePoolName, comment), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_emptyStringComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb,
    tapePoolName, comment), catalogue::UserSpecifiedAnEmptyStringComment);
}
  
TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_non_existent_storage_class) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string storageClassName = "storage_class";

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClassName, copyNb, tapePoolName,
    comment), exception::UserError);
}
  
TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_non_existent_tape_pool) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";

  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb,
    tapePoolName, comment), exception::UserError);
}
  
TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_same_twice) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName, comment);
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb,
    tapePoolName, comment), exception::Exception);
}
  
TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_two_routes_same_pool) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb1 = 1;
  const std::string comment1 = "Create archive route for copy 1";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb1, tapePoolName, comment1);

  const uint32_t copyNb2 = 2;
  const std::string comment2 = "Create archive route for copy 2";
  ASSERT_THROW(m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb2,
    tapePoolName, comment2), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveRoute) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName, comment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteArchiveRoute(storageClass.name, copyNb);

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveRoute_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());
  ASSERT_THROW(m_catalogue->deleteArchiveRoute("non_existent_storage_class", 1234),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createArchiveRoute_deleteStorageClass) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName, comment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->deleteStorageClass(storageClass.name),
    catalogue::UserSpecifiedStorageClassUsedByArchiveRoutes);

  ASSERT_THROW(m_catalogue->deleteStorageClass(storageClass.name),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteTapePoolName) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string anotherTapePoolName = "another_tape_pool";
  m_catalogue->createTapePool(m_admin, anotherTapePoolName, vo, nbPartialTapes, isEncrypted, supply,
    "Create another tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName, comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyArchiveRouteTapePoolName(m_admin, storageClass.name, copyNb,
    anotherTapePoolName);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(anotherTapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteTapePoolName_nonExistentArchiveRoute) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  ASSERT_THROW(m_catalogue->modifyArchiveRouteTapePoolName(m_admin, storageClass.name,
    copyNb, tapePoolName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteComment) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    comment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  
    const common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyArchiveRouteComment(m_admin, storageClass.name, copyNb,
    modifiedComment);

  {
    const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();
      
    ASSERT_EQ(1, routes.size());
      
    const common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(storageClass.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(modifiedComment, route.comment);

    const common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyArchiveRouteComment_nonExistentArchiveRoute) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());
  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->modifyArchiveRouteComment(m_admin, storageClass.name, copyNb,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);
      
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

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary_disabled_true) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled(true);
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);
      
  const std::list<common::dataStructures::LogicalLibrary> libs =
    m_catalogue->getLogicalLibraries();
      
  ASSERT_EQ(1, libs.size());
      
  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_TRUE(lib.isDisabled);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary_disabled_false) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled(false);
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);
      
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
  
TEST_P(cta_catalogue_CatalogueTest, createLogicalLibrary_same_twice) {
  using namespace cta;
  
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);
  ASSERT_THROW(m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setLogicalLibraryDisabled_true) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);
      
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
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);
      
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
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
      
  const std::string logicalLibraryName = "logical_library";
  const std::string comment = "Create logical library";
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);
      
  const std::list<common::dataStructures::LogicalLibrary> libs =
    m_catalogue->getLogicalLibraries();
      
  ASSERT_EQ(1, libs.size());
      
  const common::dataStructures::LogicalLibrary lib = libs.front();
  ASSERT_EQ(logicalLibraryName, lib.name);
  ASSERT_EQ(comment, lib.comment);

  const common::dataStructures::EntryLog creationLog = lib.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);
  
  const common::dataStructures::EntryLog lastModificationLog =
    lib.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteLogicalLibrary(logicalLibraryName);
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteLogicalLibrary_non_existent) {
  using namespace cta;
      
  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
  ASSERT_THROW(m_catalogue->deleteLogicalLibrary("non_existent_logical_library"),
    catalogue::UserSpecifiedANonExistentLogicalLibrary);
}

TEST_P(cta_catalogue_CatalogueTest, deleteLogicalLibrary_non_empty) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, 
    comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(mediaType, tape.mediaType);
  ASSERT_EQ(vendor, tape.vendor);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(vo, tape.vo);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
  ASSERT_TRUE(readOnlyValue == tape.readOnly);
  ASSERT_FALSE(tape.isFromCastor);
  ASSERT_EQ(comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->deleteLogicalLibrary(logicalLibraryName), catalogue::UserSpecifiedANonEmptyLogicalLibrary);
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
  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, comment);

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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "";

  ASSERT_THROW(m_catalogue->tapeExists(vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, createTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  ASSERT_TRUE(m_catalogue->tapeExists(vid));

  const auto tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringVid) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "";

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), catalogue::UserSpecifiedAnEmptyStringVid);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringMediaType) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), catalogue::UserSpecifiedAnEmptyStringMediaType);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringVendor) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "media_type";
  const std::string vendor = "";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  
  const std::string comment = "Create tape";

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), catalogue::UserSpecifiedAnEmptyStringVendor);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringLogicalLibraryName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "";
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), catalogue::UserSpecifiedAnEmptyStringLogicalLibraryName);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringTapePoolName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "";
  const std::string vo = "vo";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_zeroCapacity) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = 0;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), catalogue::UserSpecifiedAZeroCapacity);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_emptyStringComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_FALSE(m_catalogue->tapeExists(vid));

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_non_existent_logical_library) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_non_existent_tape_pool) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createTape_9_exabytes_capacity) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  // The maximum size of an SQLite integer is a signed 64-bit integer
  const uint64_t capacityInBytes = 9L * 1000 * 1000 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  const auto tapes = m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto &tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_same_twice) {
  using namespace cta;

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, comment);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName,
    tapePoolName, capacityInBytes, disabledValue, fullValue, readOnlyValue,
    comment), exception::UserError);

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createTape_many_tapes) {
  using namespace cta;

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibrary = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t) 10 * 1000 * 1000 * 1000 * 1000;
  const bool disabled = true;
  const bool full = false;
  const bool readOnly = false;
  const std::string comment = "Create tape";

  ASSERT_TRUE(m_catalogue->getLogicalLibraries().empty());
  m_catalogue->createLogicalLibrary(m_admin, logicalLibrary, logicalLibraryIsDisabled, "Create logical library");

  ASSERT_TRUE(m_catalogue->getTapePools().empty());
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  const uint64_t nbTapes = 10;

  for(uint64_t i = 1; i <= nbTapes; i++) {
    std::ostringstream vid;
    vid << "vid" << i;

    m_catalogue->createTape(m_admin, vid.str(), mediaType, vendor, logicalLibrary, tapePoolName, capacityInBytes,
      disabled, full, readOnly, comment);

    {
      const auto pools = m_catalogue->getTapePools();
      ASSERT_EQ(1, pools.size());

      const auto &pool = pools.front();
      ASSERT_EQ(tapePoolName, pool.name);
      ASSERT_EQ(vo, pool.vo);
      ASSERT_EQ(i, pool.nbTapes);
      ASSERT_EQ(i * capacityInBytes, pool.capacityBytes);
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
      ASSERT_FALSE(vidToTape.end() == vidAndTapeItor);

      const common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibrary, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabled == tape.disabled);
      ASSERT_TRUE(full == tape.full);
      ASSERT_TRUE(readOnly == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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
    searchCriteria.mediaType = mediaType;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(mediaType, vidToTape.begin()->second.mediaType);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vendor = vendor;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(vendor, vidToTape.begin()->second.vendor);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.logicalLibrary = logicalLibrary;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(logicalLibrary, vidToTape.begin()->second.logicalLibraryName);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.tapePool = tapePoolName;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(tapePoolName, vidToTape.begin()->second.tapePoolName);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vo = vo;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(vo, vidToTape.begin()->second.vo);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.capacityInBytes = capacityInBytes;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(capacityInBytes, vidToTape.begin()->second.capacityInBytes);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.disabled = true;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_TRUE(vidToTape.begin()->second.disabled);
  }

  {
    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.full = false;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_FALSE(vidToTape.begin()->second.full);
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
    searchCriteria.logicalLibrary = logicalLibrary;
    searchCriteria.tapePool = tapePoolName;
    searchCriteria.capacityInBytes = capacityInBytes;
    searchCriteria.disabled = true;
    searchCriteria.full = false;
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes(searchCriteria);
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ("vid1", vidToTape.begin()->first);
    ASSERT_EQ("vid1", vidToTape.begin()->second.vid);
    ASSERT_EQ(logicalLibrary, vidToTape.begin()->second.logicalLibraryName);
    ASSERT_EQ(tapePoolName, vidToTape.begin()->second.tapePoolName);
    ASSERT_EQ(capacityInBytes, vidToTape.begin()->second.capacityInBytes);
    ASSERT_TRUE(vidToTape.begin()->second.disabled);
    ASSERT_FALSE(vidToTape.begin()->second.full);
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
      ASSERT_FALSE(vidToTape.end() == vidAndTapeItor);

      const common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibrary, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabled == tape.disabled);
      ASSERT_TRUE(full == tape.full);
      ASSERT_TRUE(readOnly == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance_name";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string vid1 = "vid1";
  const std::string vid2 = "vid2";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName,
      capacityInBytes, disabledValue, fullValue, readOnlyValue, comment);
    const auto tapes = cta_catalogue_CatalogueTest::tapeListToMap(m_catalogue->getTapes());
    ASSERT_EQ(1, tapes.size());

    const auto tapeItor = tapes.find(vid1);
    ASSERT_NE(tapes.end(), tapeItor);

    const common::dataStructures::Tape tape = tapeItor->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  const uint64_t fileSize = 1234 * 1000000000UL;
  {
    auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = 1234;
    file1Written.diskInstance         = storageClass.diskInstance;
    file1Written.diskFileId           = "5678";
    file1Written.diskFilePath         = "/public_dir/public_file";
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(checksum::ADLER32, 0x1000); // tests checksum with embedded zeros
    file1Written.storageClassName     = storageClass.name;
    file1Written.vid                  = vid1;
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
    ASSERT_EQ("vid1", vidToTape.begin()->first);
    ASSERT_EQ("vid1", vidToTape.begin()->second.vid);
  }

  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(fileSize, pool.dataBytes);
    ASSERT_EQ(1, pool.nbPhysicalFiles);
  }

  {
    m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName,
      capacityInBytes, disabledValue, fullValue, readOnlyValue, comment);
    const auto tapes = cta_catalogue_CatalogueTest::tapeListToMap(m_catalogue->getTapes());
    ASSERT_EQ(2, tapes.size());

    const auto tapeItor = tapes.find(vid2);
    ASSERT_NE(tapes.end(), tapeItor);

    const common::dataStructures::Tape tape = tapeItor->second;
    ASSERT_EQ(vid2, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(vo, pool.vo);
    ASSERT_EQ(2, pool.nbTapes);
    ASSERT_EQ(2*capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(fileSize, pool.dataBytes);
    ASSERT_EQ(1, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_CatalogueTest, deleteTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName,
    capacityInBytes, disabledValue, fullValue, readOnlyValue, 
    comment);

  const std::list<common::dataStructures::Tape> tapes =
    m_catalogue->getTapes();

  ASSERT_EQ(1, tapes.size());

  const common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(mediaType, tape.mediaType);
  ASSERT_EQ(vendor, tape.vendor);
  ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
  ASSERT_EQ(tapePoolName, tape.tapePoolName);
  ASSERT_EQ(vo, tape.vo);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_TRUE(disabledValue == tape.disabled);
  ASSERT_TRUE(fullValue == tape.full);
  ASSERT_TRUE(readOnlyValue == tape.readOnly);
  ASSERT_FALSE(tape.isFromCastor);
  ASSERT_EQ(comment, tape.comment);
  ASSERT_FALSE(tape.labelLog);
  ASSERT_FALSE(tape.lastReadLog);
  ASSERT_FALSE(tape.lastWriteLog);

  const common::dataStructures::EntryLog creationLog = tape.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog =
    tape.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteTape(tape.vid);
  ASSERT_TRUE(m_catalogue->getTapes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteNonEmptyTape) {
  using namespace cta;

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes, disabledValue, fullValue,
    readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
  {
    auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = 1234;
    file1Written.diskInstance         = storageClass.diskInstance;
    file1Written.diskFileId           = "5678";
    file1Written.diskFilePath         = "/public_dir/public_file";
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
    file1Written.storageClassName     = storageClass.name;
    file1Written.vid                  = vid;
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
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(fileSize, tape.dataOnTapeInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->deleteTape(vid), catalogue::UserSpecifiedANonEmptyTape);
  ASSERT_FALSE(m_catalogue->getTapes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, deleteTape_non_existent) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  ASSERT_THROW(m_catalogue->deleteTape("non_existent_tape"), catalogue::UserSpecifiedANonExistentTape);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeMediaType) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "mediaType";
  const std::string anotherMediaType = "another_media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeMediaType(m_admin, vid, anotherMediaType);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(anotherMediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeVendor) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "mediaType";
  const std::string vendor = "vendor";
  const std::string anotherVendor = "another_vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeVendor(m_admin, vid, anotherVendor);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(anotherVendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "mediaType";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string anotherLogicalLibraryName = "another_logical_library_name";
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createLogicalLibrary(m_admin, anotherLogicalLibraryName, logicalLibraryIsDisabled,
    "Create another logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeLogicalLibraryName(m_admin, vid, anotherLogicalLibraryName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(anotherLogicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  ASSERT_THROW(m_catalogue->modifyTapeLogicalLibraryName(m_admin, vid, logicalLibraryName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeTapePoolName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "mediaType";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const std::string anotherTapePoolName = "another_tape_pool_name";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTapePool(m_admin, anotherTapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create another tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->modifyTapeTapePoolName(m_admin, vid, anotherTapePoolName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(anotherTapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  ASSERT_THROW(m_catalogue->modifyTapeTapePoolName(m_admin, vid, tapePoolName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeCapacityInBytes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedCapacityInBytes = 2 * capacityInBytes;
  m_catalogue->modifyTapeCapacityInBytes(m_admin, vid, modifiedCapacityInBytes);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(modifiedCapacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeCapacityInBytes_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;

  ASSERT_THROW(m_catalogue->modifyTapeCapacityInBytes(m_admin, vid, capacityInBytes), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyTapeEncryptionKeyName) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
  m_catalogue->modifyTapeEncryptionKeyName(m_admin, vid, modifiedEncryptionKeyName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(modifiedEncryptionKeyName, tape.encryptionKeyName);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
  m_catalogue->modifyTapeEncryptionKeyName(m_admin, vid, modifiedEncryptionKeyName);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_FALSE((bool)tape.encryptionKeyName);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string encryptionKeyName = "encryption_key_name";

  ASSERT_THROW(m_catalogue->modifyTapeEncryptionKeyName(m_admin, vid, encryptionKeyName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeLabelled) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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
  m_catalogue->tapeLabelled(vid, labelDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string labelDrive = "drive";

  ASSERT_THROW(m_catalogue->tapeLabelled(vid, labelDrive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForArchive) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(comment, tape.comment);
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
  m_catalogue->tapeMountedForArchive(vid, modifiedDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(1, tape.writeMountCount);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(modifiedDrive, tape.lastWriteLog.value().drive);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
  
  for(int i=1; i<1024; i++) {
    m_catalogue->tapeMountedForArchive(vid, modifiedDrive);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(1024, tape.writeMountCount);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->tapeMountedForArchive(vid, drive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, tapeMountedForRetrieve) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(0, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(comment, tape.comment);
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
  m_catalogue->tapeMountedForRetrieve(vid, modifiedDrive);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(1, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_TRUE((bool)tape.lastReadLog);
    ASSERT_EQ(modifiedDrive, tape.lastReadLog.value().drive);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
  
  for(int i=1; i<1024; i++) {
    m_catalogue->tapeMountedForRetrieve(vid, modifiedDrive);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(1024, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->tapeMountedForRetrieve(vid, drive), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeFull) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeFull(m_admin, vid, true);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->setTapeFull(m_admin, vid, true), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, noSpaceLeftOnTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->noSpaceLeftOnTape(vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->noSpaceLeftOnTape(vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeReadOnly) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeReadOnly(m_admin, vid, true);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
  
   m_catalogue->setTapeReadOnly(m_admin, vid, false);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_FALSE(tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeReadOnly_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->setTapeReadOnly(m_admin, vid, true), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeReadOnlyOnError) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeReadOnlyOnError(vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeReadOnlyOnError_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->setTapeReadOnlyOnError(vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDisabled) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeDisabled(m_admin, vid, true);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(tape.disabled);
    ASSERT_FALSE(tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, setTapeIsFromCastorInUnitTests) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeIsFromCastorInUnitTests(vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_TRUE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
  
  // do it twice
  m_catalogue->setTapeIsFromCastorInUnitTests(vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_TRUE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->setTapeIsFromCastorInUnitTests(vid), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, setTapeDisabled_nonExistentTape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";

  ASSERT_THROW(m_catalogue->setTapeDisabled(m_admin, vid, true), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const bool readOnlyValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
   disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->tapeLabelled(vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);

  ASSERT_EQ(1, tapes.size());

  const catalogue::TapeForWriting tape = tapes.front();
  ASSERT_EQ(vid, tape.vid);
  ASSERT_EQ(mediaType, tape.mediaType);
  ASSERT_EQ(vendor, tape.vendor);
  ASSERT_EQ(tapePoolName, tape.tapePool);
  ASSERT_EQ(vo, tape.vo);
  ASSERT_EQ(0, tape.lastFSeq);
  ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(0, tape.dataOnTapeInBytes);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting_disabled_tape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
   disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->tapeLabelled(vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);

  ASSERT_EQ(0, tapes.size());
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting_full_tape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = true;
  const bool readOnlyValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
   disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->tapeLabelled(vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);

  ASSERT_EQ(0, tapes.size());
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting_read_only_tape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
   disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->tapeLabelled(vid, "tape_drive");

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);

  ASSERT_EQ(0, tapes.size());
}

TEST_P(cta_catalogue_CatalogueTest, getTapesForWriting_is_from_castor_tape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const bool readOnlyValue = false;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
   disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->tapeLabelled(vid, "tape_drive");
  {
    const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);
    ASSERT_EQ(1, tapes.size());
  }
  {
    m_catalogue->setTapeIsFromCastorInUnitTests(vid);
    const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);
    ASSERT_EQ(0, tapes.size());
  }
}

TEST_P(cta_catalogue_CatalogueTest, DISABLED_getTapesForWriting_no_labelled_tapes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid = "vid";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
   disabledValue, fullValue, readOnlyValue, comment);

  const std::list<catalogue::TapeForWriting> tapes = m_catalogue->getTapesForWriting(logicalLibraryName);

  ASSERT_TRUE(tapes.empty());
}

TEST_P(cta_catalogue_CatalogueTest, createMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  const std::list<common::dataStructures::MountPolicy> mountPolicies =
    m_catalogue->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

  ASSERT_EQ(name, mountPolicy.name);

  ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

  ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

  ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

  ASSERT_EQ(comment, mountPolicy.comment);

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

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  ASSERT_THROW(m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

  ASSERT_EQ(name, mountPolicy.name);

  ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

  ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
  ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

  ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

  ASSERT_EQ(comment, mountPolicy.comment);

  const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->deleteMountPolicy(name);

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

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedArchivePriority = archivePriority + 10;
  m_catalogue->modifyMountPolicyArchivePriority(m_admin, name, modifiedArchivePriority);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(modifiedArchivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
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

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMinArchiveRequestAge = minArchiveRequestAge + 10;
  m_catalogue->modifyMountPolicyArchiveMinRequestAge(m_admin, name, modifiedMinArchiveRequestAge);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(modifiedMinArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyArchiveMinRequestAge_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minArchiveRequestAge = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyArchiveMinRequestAge(m_admin, name, minArchiveRequestAge), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetreivePriority) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedRetrievePriority = retrievePriority + 10;
  m_catalogue->modifyMountPolicyRetrievePriority(m_admin, name, modifiedRetrievePriority);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(modifiedRetrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
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

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMinRetrieveRequestAge = minRetrieveRequestAge + 10;
  m_catalogue->modifyMountPolicyRetrieveMinRequestAge(m_admin, name, modifiedMinRetrieveRequestAge);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(modifiedMinRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyRetrieveMinRequestAge_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minRetrieveRequestAge = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyRetrieveMinRequestAge(m_admin, name, minRetrieveRequestAge), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyMaxDrivesAllowed) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedMaxDrivesAllowed = maxDrivesAllowed + 10;
  m_catalogue->modifyMountPolicyMaxDrivesAllowed(m_admin, name, modifiedMaxDrivesAllowed);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(modifiedMaxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyMaxDrivesAllowed_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t maxDrivesAllowed = 2;

  ASSERT_THROW(m_catalogue->modifyMountPolicyMaxDrivesAllowed(m_admin, name, maxDrivesAllowed), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;
  const std::string &comment = "Create mount policy";

  m_catalogue->createMountPolicy(
    m_admin,
    name,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    comment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(comment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->modifyMountPolicyComment(m_admin, name, modifiedComment);

  {
    const std::list<common::dataStructures::MountPolicy> mountPolicies = m_catalogue->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(name, mountPolicy.name);

    ASSERT_EQ(archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(maxDrivesAllowed, mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(modifiedComment, mountPolicy.comment);

    const common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_CatalogueTest, modifyMountPolicyComment_nonExistentMountPolicy) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const std::string comment = "Comment";

  ASSERT_THROW(m_catalogue->modifyMountPolicyComment(m_admin, name, comment), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createRequesterMountRule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);
  ASSERT_THROW(m_catalogue->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = "disk_instance";
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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string anotherMountPolicyName = "another_mount_policy";

  m_catalogue->createMountPolicy(
    m_admin,
    anotherMountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create another mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";

  ASSERT_THROW(m_catalogue->modifyRequesterMountRulePolicy(m_admin, diskInstanceName, requesterName, mountPolicyName),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesteMountRuleComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string anotherMountPolicyName = "another_mount_policy";

  m_catalogue->createMountPolicy(
    m_admin,
    anotherMountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create another mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";

  ASSERT_THROW(m_catalogue->modifyRequesterGroupMountRulePolicy(m_admin, diskInstanceName, requesterGroupName,
    mountPolicyName), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, modifyRequesterGroupMountRuleComment) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterGroupMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 4;
  const uint64_t retrievePriority = 5;
  const uint64_t minRetrieveRequestAge = 8;
  const uint64_t maxDrivesAllowed = 9;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  // Do not create any archive routes
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->checkAndGetNextArchiveFileId(storageClass.diskInstance, storageClass.name, requesterIdentity),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_no_mount_rules) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  const std::string diskInstanceName = "disk_instance_name";

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
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

  ASSERT_THROW(m_catalogue->checkAndGetNextArchiveFileId(storageClass.diskInstance, storageClass.name, requesterIdentity),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_requester_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
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
      m_catalogue->checkAndGetNextArchiveFileId(storageClass.diskInstance, storageClass.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_requester_group_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
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
    const uint64_t archiveFileId =
      m_catalogue->checkAndGetNextArchiveFileId(storageClass.diskInstance, storageClass.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_CatalogueTest, checkAndGetNextArchiveFileId_requester_mount_rule_overide) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
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
      m_catalogue->checkAndGetNextArchiveFileId(storageClass.diskInstance, storageClass.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_no_archive_routes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  // Do not create any archive routes
  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->getArchiveFileQueueCriteria(storageClass.diskInstance, storageClass.name, requesterIdentity),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_requester_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
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
  m_catalogue->getArchiveFileQueueCriteria(storageClass.diskInstance, storageClass.name, requesterIdentity);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_requester_group_mount_rule) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
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
  m_catalogue->getArchiveFileQueueCriteria(storageClass.diskInstance, storageClass.name, requesterIdentity);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFileQueueCriteria_requester_mount_rule_overide) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getRequesterMountRules().empty());

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapePoolName = "tape_pool";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->createArchiveRoute(m_admin, storageClass.name, copyNb, tapePoolName,
    archiveRouteComment);

  const std::list<common::dataStructures::ArchiveRoute> routes = m_catalogue->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(storageClass.name, route.storageClassName);
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
  m_catalogue->getArchiveFileQueueCriteria(storageClass.diskInstance, storageClass.name, requesterIdentity);
}

TEST_P(cta_catalogue_CatalogueTest, prepareToRetrieveFileUsingArchiveFileId) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";
  const std::string diskInstanceName2 = "disk_instance_2";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
  const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
  {
    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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
    auto it = vidToTape.find(vid2);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid2, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;    file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
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
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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
  ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);

  // Check that the diskInstanceName mismatch detection works
  ASSERT_THROW(m_catalogue->prepareToRetrieveFile(diskInstanceName2, archiveFileId, requesterIdentity, cta::nullopt, dummyLc),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, prepareToRetrieveFileUsingArchiveFileId_disabledTapes) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
  const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
  {
    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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
    auto it = vidToTape.find(vid2);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid2, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
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
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);

    const auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  const std::string mountPolicyName = "mount_policy";
  const uint64_t archivePriority = 1;
  const uint64_t minArchiveRequestAge = 2;
  const uint64_t retrievePriority = 3;
  const uint64_t minRetrieveRequestAge = 4;
  const uint64_t maxDrivesAllowed = 5;

  m_catalogue->createMountPolicy(
    m_admin,
    mountPolicyName,
    archivePriority,
    minArchiveRequestAge,
    retrievePriority,
    minRetrieveRequestAge,
    maxDrivesAllowed,
    "Create mount policy");

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
    ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = queueCriteria.archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == queueCriteria.archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == queueCriteria.archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  m_catalogue->setTapeDisabled(m_admin, vid1, true);

  {
    const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
    ASSERT_EQ(maxDrivesAllowed, queueCriteria.mountPolicy.maxDrivesAllowed);

    ASSERT_EQ(1, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == queueCriteria.archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  m_catalogue->setTapeDisabled(m_admin, vid2, true);

  ASSERT_THROW(m_catalogue->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, cta::nullopt, dummyLc),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existance_archiveFileId) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.archiveFileId = 1234;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_disk_file_group_without_instance) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskFileGid = DISK_FILE_GID;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existent_disk_file_group) {
  using namespace cta;
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskInstance = "non_existent_disk_instance";
  searchCriteria.diskFileGid = NON_EXISTENT_DISK_FILE_GID;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_disk_file_id_without_instance) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskFileId = "disk_file_id";

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existent_disk_file_id) {
  using namespace cta;
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskInstance = "non_existent_disk_instance";
  searchCriteria.diskFileId = "non_existent_disk_file_id";

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_disk_file_path_without_instance) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskFilePath = "disk_file_path";

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existent_disk_file_path) {
  using namespace cta;
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskInstance = "non_existent_disk_instance";
  searchCriteria.diskFilePath = "non_existent_disk_file_path";

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_disk_file_user_without_instance) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskFileOwnerUid = DISK_FILE_OWNER_UID;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existent_disk_file_user) {
  using namespace cta;
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskInstance = "non_existent_disk_instance";
  searchCriteria.diskFileOwnerUid = NON_EXISTENT_DISK_FILE_OWNER_UID;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_existent_storage_class_without_disk_instance) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getStorageClasses().empty());

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::list<common::dataStructures::StorageClass> storageClasses = m_catalogue->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  {
    const auto s = storageClasses.front();

    ASSERT_EQ(storageClass.diskInstance, s.diskInstance);
    ASSERT_EQ(storageClass.name, s.name);
    ASSERT_EQ(storageClass.nbCopies, s.nbCopies);
    ASSERT_EQ(storageClass.comment, s.comment);

    const common::dataStructures::EntryLog creationLog = s.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = s.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.storageClass = storageClass.name;

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existent_storage_class) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.diskInstance = "non_existent_disk_instance";
  searchCriteria.storageClass = "non_existent_storage_class";

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, getArchiveFiles_non_existent_tape_pool) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.tapePool = "non_existent_tape_pool";

  ASSERT_THROW(m_catalogue->getArchiveFilesItor(searchCriteria), exception::UserError);
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

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName1, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  m_catalogue->createTapePool(m_admin, tapePoolName2, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName1, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName2, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
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
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName1, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      const auto &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName2, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const std::string tapeDrive = "tape_drive";

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);
    std::ostringstream diskFilePath;
    diskFilePath << "/public_dir/public_file_" << i;

    // Tape copy 1 written to tape
    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = storageClass.diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = storageClass.name;
    fileWritten.vid = vid1;
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
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(vid1);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid1, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid2, it->second.vid);
      ASSERT_EQ(0, it->second.lastFSeq);
    }
  }

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy2;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);
    std::ostringstream diskFilePath;
    diskFilePath << "/public_dir/public_file_" << i;

    // Tape copy 2 written to tape
    auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = storageClass.diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = storageClass.name;
    fileWritten.vid = vid2;
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
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(vid1);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid1, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid2, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileId = std::to_string(12345678);
    searchCriteria.diskFilePath = "/public_dir/public_file_1";
    searchCriteria.diskFileOwnerUid = PUBLIC_DISK_USER;
    searchCriteria.diskFileGid = PUBLIC_DISK_GROUP;
    searchCriteria.storageClass = storageClass.name;
    searchCriteria.vid = vid1;
    searchCriteria.tapeFileCopyNb = 1;
    searchCriteria.tapePool = tapePoolName1;

    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    const auto idAndFile = m.find(1);
    ASSERT_FALSE(m.end() == idAndFile);
    const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
    ASSERT_EQ(searchCriteria.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(searchCriteria.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(searchCriteria.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(searchCriteria.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(searchCriteria.diskFileOwnerUid, static_cast<uint64_t>(archiveFile.diskFileInfo.owner_uid));
    ASSERT_EQ(searchCriteria.diskFileGid, static_cast<uint64_t>(archiveFile.diskFileInfo.gid));
    ASSERT_EQ(searchCriteria.storageClass, archiveFile.storageClass);
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
      fileWritten1.diskInstance = storageClass.diskInstance;
      fileWritten1.diskFileId = diskFileId.str();
      fileWritten1.diskFilePath = diskFilePath.str();
      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = vid1;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = vid2;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten1.diskFilePath, archiveFile.diskFileInfo.path);
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
    auto archiveFileItor = m_catalogue->getArchiveFilesForRepackItor(vid1, startFseq);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = storageClass.diskInstance;
      fileWritten1.diskFileId = diskFileId.str();
      fileWritten1.diskFilePath = diskFilePath.str();
      fileWritten1.diskFileOwnerUid     = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid     = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = vid1;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = vid2;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten1.diskFilePath, archiveFile.diskFileInfo.path);
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

  for(uint32_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? vid1 : vid2;
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
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten.diskFilePath, archiveFile.diskFileInfo.path);
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
    const std::string vid = copyNb == 1 ? vid1 : vid2;
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
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten.diskFilePath, archiveFile.diskFileInfo.path);
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
    const std::string vid = copyNb == 1 ? vid1 : vid2;
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
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten.diskFilePath, archiveFile.diskFileInfo.path);
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
    searchCriteria.diskInstance = storageClass.diskInstance;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileId = "12345687";
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
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFilePath = "/public_dir/public_file_10";
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
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileOwnerUid     = PUBLIC_DISK_USER;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileGid = PUBLIC_DISK_GROUP;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.storageClass = storageClass.name;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
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
    searchCriteria.vid = vid1;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.tapeFileCopyNb = 1;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.tapePool = tapePoolName1;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.tapePool = tapePoolName2;
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
        const std::string &tapeDrive) :
          m_cat(cat), m_barrier(barrier), m_nbArchiveFiles(nbArchiveFiles), m_batchSize(batchSize), m_storageClass(storageClass), m_archiveFileSize(archiveFileSize),
          m_checksumBlob(checksumBlob), m_vid(vid), m_copyNb(copyNb), m_tapeDrive(tapeDrive) { }

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
          fileWritten.diskInstance = m_storageClass.diskInstance;
          fileWritten.diskFileId = diskFileId.str();
          fileWritten.diskFilePath = diskFilePath.str();
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
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->createTapePool(m_admin, tapePoolName1, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  m_catalogue->createTapePool(m_admin, tapePoolName2, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName1, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName2, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  {
    const auto pools = m_catalogue->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
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
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName1, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      const auto &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName2, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

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
    filesWrittenThread a(m_catalogue.get(), barrier, nbArchiveFiles, batchsize, storageClass, archiveFileSize, checksumBlob, vid1, 1, tapeDrive1);
    filesWrittenThread b(catalogue2.get(), barrier, nbArchiveFiles, batchsize, storageClass, archiveFileSize, checksumBlob, vid2, 2, tapeDrive2);

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
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(vid1);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid1, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid2, it->second.vid);
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
    ASSERT_EQ(capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(vid1);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid1, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(vid2, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileId = std::to_string(12345678);
    searchCriteria.diskFilePath = "/public_dir/public_file_1";
    searchCriteria.diskFileOwnerUid = PUBLIC_DISK_USER;
    searchCriteria.diskFileGid = PUBLIC_DISK_GROUP;
    searchCriteria.storageClass = storageClass.name;
    searchCriteria.vid = vid1;
    searchCriteria.tapeFileCopyNb = 1;
    searchCriteria.tapePool = tapePoolName1;

    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, common::dataStructures::ArchiveFile> m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    const auto idAndFile = m.find(1);
    ASSERT_FALSE(m.end() == idAndFile);
    const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
    ASSERT_EQ(searchCriteria.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(searchCriteria.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(searchCriteria.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(searchCriteria.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(searchCriteria.diskFileOwnerUid, static_cast<uint64_t>(archiveFile.diskFileInfo.owner_uid));
    ASSERT_EQ(searchCriteria.diskFileGid, static_cast<uint64_t>(archiveFile.diskFileInfo.gid));
    ASSERT_EQ(searchCriteria.storageClass, archiveFile.storageClass);
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
      fileWritten1.diskInstance = storageClass.diskInstance;
      fileWritten1.diskFileId = diskFileId.str();
      fileWritten1.diskFilePath = diskFilePath.str();
      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "2468");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = vid1;
      fileWritten1.fSeq = seq1;
      fileWritten1.blockId = seq1 * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = vid2;
      fileWritten2.fSeq = seq2;
      fileWritten2.blockId = seq2 * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten1.diskFilePath, archiveFile.diskFileInfo.path);
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
    auto archiveFileItor = m_catalogue->getArchiveFilesForRepackItor(vid1, startFseq);
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
      fileWritten1.diskInstance = storageClass.diskInstance;
      fileWritten1.diskFileId = diskFileId.str();
      fileWritten1.diskFilePath = diskFilePath.str();
      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(checksum::ADLER32, "2468");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = vid1;
      fileWritten1.fSeq = seq1;
      fileWritten1.blockId = seq1 * 100;
      fileWritten1.copyNb = 1;

      catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = vid2;
      fileWritten2.fSeq = seq2;
      fileWritten2.blockId = seq2 * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten1.diskFilePath, archiveFile.diskFileInfo.path);
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
    const std::string vid = copyNb == 1 ? vid1 : vid2;
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
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
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
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten.diskFilePath, archiveFile.diskFileInfo.path);
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
    const std::string vid = copyNb == 1 ? vid1 : vid2;
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
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
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
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten.diskFilePath, archiveFile.diskFileInfo.path);
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
    const std::string vid = copyNb == 1 ? vid1 : vid2;
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
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
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
      ASSERT_FALSE(m.end() == idAndFile);
      const common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(fileWritten.diskFilePath, archiveFile.diskFileInfo.path);
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
    searchCriteria.diskInstance = storageClass.diskInstance;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileId = "12345687";
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
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFilePath = "/public_dir/public_file_10";
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
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileOwnerUid     = PUBLIC_DISK_USER;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.diskFileGid = PUBLIC_DISK_GROUP;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = storageClass.diskInstance;
    searchCriteria.storageClass = storageClass.name;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
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
    searchCriteria.vid = vid1;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.tapeFileCopyNb = 1;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.tapePool = tapePoolName1;
    auto archiveFileItor = m_catalogue->getArchiveFilesItor(searchCriteria);
    const auto m = archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const common::dataStructures::ArchiveFileSummary summary = m_catalogue->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.tapePool = tapePoolName2;
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

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_TRUE(m_catalogue->getArchiveRoutes().empty());

  ASSERT_THROW(m_catalogue->deleteStorageClass(storageClass.name),
    catalogue::UserSpecifiedStorageClassUsedByArchiveFiles);

  ASSERT_THROW(m_catalogue->deleteStorageClass(storageClass.name),
    exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
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
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
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

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
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
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    // If there are two or more tape copies with the same copy number then
    // only one of them will be active (<=> supersededByVid.empty()).
    ASSERT_EQ(1, std::count_if(archiveFile.tapeFiles.begin(), archiveFile.tapeFiles.end(),
        [](const cta::common::dataStructures::TapeFile &tf){ return tf.supersededByVid.empty();}));

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

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid1;
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
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    // If there are two or more tape copies with the same copy number then
    // only one of them will be active (<=> supersededByVid.empty()).
    ASSERT_EQ(1, std::count_if(archiveFile.tapeFiles.begin(), archiveFile.tapeFiles.end(),
        [](const cta::common::dataStructures::TapeFile &tf){ return tf.supersededByVid.empty();}));

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

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid1;
  file2Written.fSeq                 = file1Written.fSeq;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet), exception::TapeFseqMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_sizes) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize1 = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize1;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize2;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet),
    catalogue::FileSizeMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_checksum_types) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);      
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(checksum::CRC32, "1234");
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet), exception::ChecksumTypeMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_checksum_values) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "mediaType";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(checksum::ADLER32, "5678");
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->filesWrittenToTape(file2WrittenSet), exception::ChecksumValueMismatch);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_FALSE(m.end() == mItor);

    const common::dataStructures::ArchiveFile archiveFile = mItor->second;

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
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
      ASSERT_FALSE(m.end() == mItor);

      const common::dataStructures::ArchiveFile archiveFile = mItor->second;

      ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(file2Written.size, archiveFile.fileSize);
      ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

      ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
      ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

      ASSERT_EQ(2, archiveFile.tapeFiles.size());

      auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
      ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
      ASSERT_EQ(file1Written.vid, tapeFile1.vid);
      ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
      ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
      ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
      ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

      auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
      ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
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
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  log::LogContext dummyLc(m_dummyLog);
  m_catalogue->deleteArchiveFile("disk_instance", archiveFileId, dummyLc);
  
  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile_by_archive_file_id_of_another_disk_instance) {
  using namespace cta;

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid1, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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
      auto it = vidToTape.find(vid2);
      const common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(vid2, tape.vid);
      ASSERT_EQ(mediaType, tape.mediaType);
      ASSERT_EQ(vendor, tape.vendor);
      ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName, tape.tapePoolName);
      ASSERT_EQ(vo, tape.vo);
      ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
      ASSERT_TRUE(disabledValue == tape.disabled);
      ASSERT_TRUE(fullValue == tape.full);
      ASSERT_TRUE(readOnlyValue == tape.readOnly);
      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(comment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = "disk_instance";
  storageClass.name = "storage_class";
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_FALSE(m.end() == mItor);

    const common::dataStructures::ArchiveFile archiveFile = mItor->second;

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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
  file2Written.diskFilePath         = file1Written.diskFilePath;
  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file2Written.storageClassName     = storageClass.name;
  file2Written.vid                  = vid2;
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
      ASSERT_FALSE(m.end() == mItor);

      const common::dataStructures::ArchiveFile archiveFile = mItor->second;

      ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(file2Written.size, archiveFile.fileSize);
      ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

      ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
      ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

      ASSERT_EQ(2, archiveFile.tapeFiles.size());

      auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
      ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
      const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
      ASSERT_EQ(file1Written.vid, tapeFile1.vid);
      ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
      ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
      ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
      ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

      auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
      ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
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
    ASSERT_EQ(file2Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_FALSE(copyNbToTapeFile2Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  log::LogContext dummyLc(m_dummyLog);
  ASSERT_THROW(m_catalogue->deleteArchiveFile("another_disk_instance", archiveFileId, dummyLc), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, deleteArchiveFile_by_archive_file_id_non_existent) {
  using namespace cta;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  log::LogContext dummyLc(m_dummyLog);
  m_catalogue->deleteArchiveFile("disk_instance", 12345678, dummyLc);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_non_existent_tape) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  std::set<std::string> vids = {{"non_existent_tape"}};
  ASSERT_THROW(m_catalogue->getTapesByVid(vids), exception::Exception);
}

TEST_P(cta_catalogue_CatalogueTest, getTapesByVid_no_vids) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());
  std::set<std::string> vids;
  ASSERT_TRUE(m_catalogue->getTapesByVid(vids).empty());
}

TEST_P(cta_catalogue_CatalogueTest, getAllTapes_no_tapes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getAllTapes().empty());
}

TEST_P(cta_catalogue_CatalogueTest, getAllTapes_many_tapes) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 10;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();
    m_catalogue->createTape(m_admin, vid.str(), mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
      disabledValue, fullValue, readOnlyValue, tapeComment);
  }

  const auto vidToTapeMap = m_catalogue->getAllTapes();
  ASSERT_EQ(nbTapes, vidToTapeMap.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto tapeItor = vidToTapeMap.find(vid.str());
    ASSERT_NE(vidToTapeMap.end(), tapeItor);

    ASSERT_EQ(vid.str(), tapeItor->second.vid);
    ASSERT_EQ(mediaType, tapeItor->second.mediaType);
    ASSERT_EQ(vendor, tapeItor->second.vendor);
    ASSERT_EQ(logicalLibraryName, tapeItor->second.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tapeItor->second.tapePoolName);
    ASSERT_EQ(vo, tapeItor->second.vo);
    ASSERT_EQ(capacityInBytes, tapeItor->second.capacityInBytes);
    ASSERT_EQ(disabledValue, tapeItor->second.disabled);
    ASSERT_EQ(fullValue, tapeItor->second.full);
    ASSERT_EQ(readOnlyValue, tapeItor->second.readOnly);
    ASSERT_FALSE(tapeItor->second.isFromCastor);
    ASSERT_EQ(0, tapeItor->second.readMountCount);
    ASSERT_EQ(0, tapeItor->second.writeMountCount);
    ASSERT_EQ(tapeComment, tapeItor->second.comment);
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
  // The maximum size of an SQLite integer is a signed 64-bit integer
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

TEST_P(cta_catalogue_CatalogueTest, getNbFilesOnTape_no_tape_files) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
 
  ASSERT_EQ(0, m_catalogue->getNbFilesOnTape(vid));
}

TEST_P(cta_catalogue_CatalogueTest, getNbFilesOnTape_one_tape_file) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_EQ(1, m_catalogue->getNbFilesOnTape(vid1));
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_no_tape_files) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
 
  ASSERT_NO_THROW(m_catalogue->checkTapeForLabel(vid));
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_one_tape_file) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_THROW(m_catalogue->checkTapeForLabel(vid1), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_one_tape_file_reclaimed_tape) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_THROW(m_catalogue->checkTapeForLabel(vid1), exception::UserError);
  
  log::LogContext dummyLc(m_dummyLog);
  m_catalogue->deleteArchiveFile(diskInstanceName1, archiveFileId, dummyLc);
  
  m_catalogue->setTapeFull(m_admin, vid1, true);
  m_catalogue->reclaimTape(m_admin, vid1);
  
  ASSERT_NO_THROW(m_catalogue->checkTapeForLabel(vid1));
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_one_tape_file_superseded) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID234";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, vidToTape.size());

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  // Record initial tape file
    
  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.size                 = 1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.size, tapeFile1.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, vidToTape.size());

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(file1Written.size, tape.dataOnTapeInBytes);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  m_catalogue->setTapeFull(m_admin, vid1, true);
  
  // Record superseding tape file

  file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1WrittenAgain = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenAgainSet;
  file1WrittenAgainSet.insert(file1WrittenUP.release());
  file1WrittenAgain.archiveFileId        = archiveFileId;
  file1WrittenAgain.diskInstance         = storageClass.diskInstance;
  file1WrittenAgain.diskFileId           = "5678";
  file1WrittenAgain.diskFilePath         = "/public_dir/public_file";
  file1WrittenAgain.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1WrittenAgain.diskFileGid          = PUBLIC_DISK_GROUP;
  file1WrittenAgain.size                 = archiveFileSize;
  file1WrittenAgain.checksumBlob.insert(checksum::ADLER32, "1234");
  file1WrittenAgain.storageClassName     = storageClass.name;
  file1WrittenAgain.vid                  = vid2;
  file1WrittenAgain.fSeq                 = 1;
  file1WrittenAgain.blockId              = 4321;
  file1WrittenAgain.size                 = 1;
  file1WrittenAgain.copyNb               = 1;
  file1WrittenAgain.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenAgainSet);
  
  common::dataStructures::ArchiveFile repackedFile = m_catalogue->getArchiveFileById(archiveFileId);
  ASSERT_EQ(2, repackedFile.tapeFiles.size());
  for (auto &tf: repackedFile.tapeFiles) {
    if (tf.vid == vid1)
      ASSERT_EQ(vid2, tf.supersededByVid);
    else
      ASSERT_EQ("", tf.supersededByVid);
  }
  
  ASSERT_THROW(m_catalogue->checkTapeForLabel(vid1), exception::UserError);
 
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_not_in_the_catalogue) {
    using namespace cta;
    
  ASSERT_TRUE(m_catalogue->getTapes().empty());
  
  const std::string vid = "vid";
  
  ASSERT_THROW(m_catalogue->checkTapeForLabel(vid), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, checkTapeForLabel_empty_vid) {
    using namespace cta;
    
  ASSERT_TRUE(m_catalogue->getTapes().empty());
  
  const std::string vid = "";
  
  ASSERT_THROW(m_catalogue->checkTapeForLabel(vid), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTape_full_lastFSeq_0_no_tape_files) {
  using namespace cta;

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->setTapeFull(m_admin, vid, true);
  m_catalogue->reclaimTape(m_admin, vid);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_FALSE(tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
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

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string vid = "vid";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string comment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, comment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->reclaimTape(m_admin, vid), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTape_full_lastFSeq_1_no_tape_files) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  {
    log::LogContext dummyLc(m_dummyLog);
    m_catalogue->deleteArchiveFile(diskInstanceName1, file1Written.archiveFileId, dummyLc);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  m_catalogue->setTapeFull(m_admin, vid1, true);
  m_catalogue->reclaimTape(m_admin, vid1);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();

    ASSERT_EQ(1, tapes.size());

    const common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
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

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(file1Written.size, tape.dataOnTapeInBytes);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  m_catalogue->setTapeFull(m_admin, vid1, true);
  ASSERT_THROW(m_catalogue->reclaimTape(m_admin, vid1), exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, reclaimTape_full_lastFSeq_1_one_tape_file_superseded) {
  using namespace cta;

  const std::string diskInstanceName1 = "disk_instance_1";

  ASSERT_TRUE(m_catalogue->getTapes().empty());

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID234";
  const std::string mediaType = "media_type";
  const std::string vendor = "vendor";
  const std::string logicalLibraryName = "logical_library_name";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName = "tape_pool_name";
  const std::string vo = "vo";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const cta::optional<std::string> supply("value for the supply pool mechanism");
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = true;
  const bool fullValue = false;
  const bool readOnlyValue = true;
  const std::string createTapeComment = "Create tape";

  m_catalogue->createLogicalLibrary(m_admin, logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->createTapePool(m_admin, tapePoolName, vo, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->createTape(m_admin, vid1, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);
  m_catalogue->createTape(m_admin, vid2, mediaType, vendor, logicalLibraryName, tapePoolName, capacityInBytes,
    disabledValue, fullValue, readOnlyValue, createTapeComment);

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, vidToTape.size());

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  // Record initial tape file
    
  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->getArchiveFileById(archiveFileId), exception::Exception);

  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = diskInstanceName1;
  storageClass.name = "storage_class";
  storageClass.nbCopies = 1;
  storageClass.comment = "Create storage class";
  m_catalogue->createStorageClass(m_admin, storageClass);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = storageClass.diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFilePath         = "/public_dir/public_file";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(checksum::ADLER32, "1234");
  file1Written.storageClassName     = storageClass.name;
  file1Written.vid                  = vid1;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.size                 = 1;
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
    ASSERT_EQ(file1Written.diskFilePath, archiveFile.diskFileInfo.path);
    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_FALSE(copyNbToTapeFile1Itor == archiveFile.tapeFiles.end());
    const common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.size, tapeFile1.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const std::list<common::dataStructures::Tape> tapes = m_catalogue->getTapes();
    const std::map<std::string, common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(2, vidToTape.size());

    auto it = vidToTape.find(vid1);
    const common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(file1Written.size, tape.dataOnTapeInBytes);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(disabledValue == tape.disabled);
    ASSERT_TRUE(fullValue == tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
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

  m_catalogue->setTapeFull(m_admin, vid1, true);
  
  // Record superseding tape file

  file1WrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1WrittenAgain = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenAgainSet;
  file1WrittenAgainSet.insert(file1WrittenUP.release());
  file1WrittenAgain.archiveFileId        = archiveFileId;
  file1WrittenAgain.diskInstance         = storageClass.diskInstance;
  file1WrittenAgain.diskFileId           = "5678";
  file1WrittenAgain.diskFilePath         = "/public_dir/public_file";
  file1WrittenAgain.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1WrittenAgain.diskFileGid          = PUBLIC_DISK_GROUP;
  file1WrittenAgain.size                 = archiveFileSize;
  file1WrittenAgain.checksumBlob.insert(checksum::ADLER32, "1234");
  file1WrittenAgain.storageClassName     = storageClass.name;
  file1WrittenAgain.vid                  = vid2;
  file1WrittenAgain.fSeq                 = 1;
  file1WrittenAgain.blockId              = 4321;
  file1WrittenAgain.size                 = 1;
  file1WrittenAgain.copyNb               = 1;
  file1WrittenAgain.tapeDrive            = tapeDrive;
  m_catalogue->filesWrittenToTape(file1WrittenAgainSet);
  
  common::dataStructures::ArchiveFile repackedFile = m_catalogue->getArchiveFileById(archiveFileId);
  ASSERT_EQ(2, repackedFile.tapeFiles.size());
  for (auto &tf: repackedFile.tapeFiles) {
    if (tf.vid == vid1)
      ASSERT_EQ(vid2, tf.supersededByVid);
    else
      ASSERT_EQ("", tf.supersededByVid);
  }
  
  ASSERT_NO_THROW(m_catalogue->reclaimTape(m_admin, vid1));
 
  {
    //Test that the tape with vid1 is reclaimed
    common::dataStructures::Tape tape = m_catalogue->getTapes().front();
    ASSERT_EQ(vid1, tape.vid);
    ASSERT_EQ(mediaType, tape.mediaType);
    ASSERT_EQ(vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(vo, tape.vo);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(tape.disabled);
    ASSERT_FALSE(tape.full);
    ASSERT_TRUE(readOnlyValue == tape.readOnly);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(createTapeComment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);
  }
}

TEST_P(cta_catalogue_CatalogueTest, createModifyDeleteActivityWeight) {
  using namespace cta;

  const std::string diskInstanceName = "ExperimentEOS";
  const std::string activity1 = "Reco";  
  const std::string activity2 = "Grid";
  const double weight1 = 0.654;
  const double weight2 = 0.456;
  const std::string comment = "No comment.";

  m_catalogue->createActivitiesFairShareWeight(m_admin, diskInstanceName, activity1, weight1, comment);
      
  const auto activitiesList = m_catalogue->getActivitiesFairShareWeights();
      
  ASSERT_EQ(1, activitiesList.size());
  ASSERT_EQ(1, activitiesList.front().activitiesWeights.size());
  ASSERT_NO_THROW(activitiesList.front().activitiesWeights.at(activity1));
  ASSERT_EQ(weight1, activitiesList.front().activitiesWeights.at(activity1));

  m_catalogue->createActivitiesFairShareWeight(m_admin, diskInstanceName, activity2, weight2, comment);
  
  const auto activitiesList2 = m_catalogue->getActivitiesFairShareWeights();
  
  ASSERT_EQ(1, activitiesList2.size());
  ASSERT_EQ(2, activitiesList2.front().activitiesWeights.size());
  ASSERT_NO_THROW(activitiesList2.front().activitiesWeights.at(activity1));
  ASSERT_EQ(weight1, activitiesList2.front().activitiesWeights.at(activity1));
  ASSERT_NO_THROW(activitiesList2.front().activitiesWeights.at(activity2));
  ASSERT_EQ(weight2, activitiesList2.front().activitiesWeights.at(activity2));
  
  ASSERT_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, "NoSuchInstance", activity2, weight2, comment), cta::exception::UserError);
  ASSERT_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, diskInstanceName, "NoSuchActivity", weight2, comment), cta::exception::UserError);
  
  ASSERT_NO_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, diskInstanceName, activity1, weight2, comment));
  ASSERT_NO_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, diskInstanceName, activity2, weight1, comment));
  
  
  const auto activitiesList3 = m_catalogue->getActivitiesFairShareWeights();
  
  ASSERT_EQ(1, activitiesList3.size());
  ASSERT_EQ(2, activitiesList3.front().activitiesWeights.size());
  ASSERT_NO_THROW(activitiesList3.front().activitiesWeights.at(activity1));
  ASSERT_EQ(weight2, activitiesList3.front().activitiesWeights.at(activity1));
  ASSERT_NO_THROW(activitiesList3.front().activitiesWeights.at(activity2));
  ASSERT_EQ(weight1, activitiesList3.front().activitiesWeights.at(activity2));
  
  ASSERT_THROW(m_catalogue->deleteActivitiesFairShareWeight(m_admin, "NoSuchInstance", activity2), cta::exception::UserError);
  ASSERT_THROW(m_catalogue->deleteActivitiesFairShareWeight(m_admin, diskInstanceName, "NoSuchActivity"), cta::exception::UserError);
  
  ASSERT_NO_THROW(m_catalogue->deleteActivitiesFairShareWeight(m_admin, diskInstanceName, activity1));
  
  const auto activitiesList4 = m_catalogue->getActivitiesFairShareWeights();
      
  ASSERT_EQ(1, activitiesList4.size());
  ASSERT_EQ(1, activitiesList4.front().activitiesWeights.size());
  ASSERT_NO_THROW(activitiesList4.front().activitiesWeights.at(activity2));
  ASSERT_EQ(weight1, activitiesList4.front().activitiesWeights.at(activity2));
  
  ASSERT_NO_THROW(m_catalogue->deleteActivitiesFairShareWeight(m_admin, diskInstanceName, activity2));
  
  ASSERT_EQ(0, m_catalogue->getActivitiesFairShareWeights().size());
}

TEST_P(cta_catalogue_CatalogueTest, activitiesDataValidation) {
  using namespace cta;
  ASSERT_THROW(m_catalogue->createActivitiesFairShareWeight(m_admin, "", "Activity", 0.1, "No comment."), catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
  ASSERT_THROW(m_catalogue->createActivitiesFairShareWeight(m_admin, "DiskInstance", "", 0.1, "No comment."), catalogue::UserSpecifiedAnEmptyStringActivity);
  ASSERT_THROW(m_catalogue->createActivitiesFairShareWeight(m_admin, "DiskInstance", "Activity", 0.0, "No comment."), catalogue::UserSpecifiedAnOutOfRangeActivityWeight);
  ASSERT_THROW(m_catalogue->createActivitiesFairShareWeight(m_admin, "DiskInstance", "Activity", 1.1, "No comment."), catalogue::UserSpecifiedAnOutOfRangeActivityWeight);
  ASSERT_THROW(m_catalogue->createActivitiesFairShareWeight(m_admin, "DiskInstance", "Activity", 0.1, ""), catalogue::UserSpecifiedAnEmptyStringComment);
  
  ASSERT_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, "", "Activity", 0.1, "No comment."), catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
  ASSERT_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, "DiskInstance", "", 0.1, "No comment."), catalogue::UserSpecifiedAnEmptyStringActivity);
  ASSERT_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, "DiskInstance", "Activity", 0.0, "No comment."), catalogue::UserSpecifiedAnOutOfRangeActivityWeight);
  ASSERT_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, "DiskInstance", "Activity", 1.1, "No comment."), catalogue::UserSpecifiedAnOutOfRangeActivityWeight);
  ASSERT_THROW(m_catalogue->modifyActivitiesFairShareWeight(m_admin, "DiskInstance", "Activity", 0.1, ""), catalogue::UserSpecifiedAnEmptyStringComment);
  
  ASSERT_THROW(m_catalogue->deleteActivitiesFairShareWeight(m_admin, "", "Activity"), catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
  ASSERT_THROW(m_catalogue->deleteActivitiesFairShareWeight(m_admin, "DiskInstance", ""), catalogue::UserSpecifiedAnEmptyStringActivity);
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

  common::dataStructures::VirtualOrganization vo;
  vo.name = "VO";
  vo.comment = "Comment";
  
  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));
}

TEST_P(cta_catalogue_CatalogueTest, createVirtualOrganizationAlreadyExists) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo;
  vo.name = "VO";
  vo.comment = "Comment";
  
  ASSERT_NO_THROW(m_catalogue->createVirtualOrganization(m_admin,vo));
  ASSERT_THROW(m_catalogue->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createVirtualOrganizationEmptyComment) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo;
  vo.name = "VO";
  
  ASSERT_THROW(m_catalogue->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_CatalogueTest, createVirtualOrganizationEmptyName) {
  using namespace cta;

  common::dataStructures::VirtualOrganization vo;
  vo.comment = "comment";
  
  ASSERT_THROW(m_catalogue->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

} // namespace unitTests
