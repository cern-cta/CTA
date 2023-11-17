/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <list>
#include <iostream>

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#include "disk/DiskSystem.hpp"

namespace unitTests {

std::unique_ptr<cta::catalogue::Catalogue> CatalogueTestUtils::createCatalogue(
  cta::catalogue::CatalogueFactory *const *const catalogueFactoryPtrPtr, cta::log::LogContext *lc) {
  try {
    if (nullptr == catalogueFactoryPtrPtr) {
      throw cta::exception::Exception("Global pointer to the catalogue factory pointer for unit-tests in null");
    }
    if (nullptr == (*catalogueFactoryPtrPtr)) {
      throw cta::exception::Exception("Global pointer to the catalogue factoryfor unit-tests in null");
    }

    auto catalogue = (*catalogueFactoryPtrPtr)->create();
    CatalogueTestUtils::wipeDatabase(catalogue.get(), lc);
    return catalogue;
  } catch(cta::exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

void CatalogueTestUtils::wipeDatabase(cta::catalogue::Catalogue *catalogue, cta::log::LogContext *lc) {
  {
    const auto adminUsers = catalogue->AdminUser()->getAdminUsers();
    for(auto &adminUser: adminUsers) {
      catalogue->AdminUser()->deleteAdminUser(adminUser.name);
    }
  }
  {
    const auto archiveRoutes = catalogue->ArchiveRoute()->getArchiveRoutes();
    for(auto &archiveRoute: archiveRoutes) {
      catalogue->ArchiveRoute()->deleteArchiveRoute(archiveRoute.storageClassName,
        archiveRoute.copyNb);
    }
  }
  {
    const auto rules = catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
    for(const auto &rule: rules) {
      catalogue->RequesterActivityMountRule()->deleteRequesterActivityMountRule(rule.diskInstance, rule.name,
        rule.activityRegex);
    }
  }
  {
    const auto rules = catalogue->RequesterMountRule()->getRequesterMountRules();
    for(const auto &rule: rules) {
      catalogue->RequesterMountRule()->deleteRequesterMountRule(rule.diskInstance, rule.name);
    }
  }
  {
    const auto rules = catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
    for(const auto &rule: rules) {
      catalogue->RequesterGroupMountRule()->deleteRequesterGroupMountRule(rule.diskInstance, rule.name);
    }
  }
  {
    // The iterator returned from catalogue->ArchiveFile()->getArchiveFilesItor() will lock
    // an SQLite file database, so copy all of its results into a list in
    // order to release the lock before moving on to deleting database rows
    auto itor = catalogue->ArchiveFile()->getArchiveFilesItor();
    std::list<cta::common::dataStructures::ArchiveFile> archiveFiles;
    while(itor.hasMore()) {
      archiveFiles.push_back(itor.next());
    }

    for(const auto &archiveFile: archiveFiles) {
      catalogue->ArchiveFile()->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(archiveFile.diskInstance,
        archiveFile.archiveFileID, *lc);
    }
  }

  {
    //Delete all the entries from the recycle log table
    auto itor = catalogue->FileRecycleLog()->getFileRecycleLogItor();
    while(itor.hasMore()){
      catalogue->FileRecycleLog()->deleteFilesFromRecycleLog(itor.next().vid, *lc);
    }
  }
  {
    const auto tapes = catalogue->Tape()->getTapes();
    for(const auto &tape: tapes) {
      catalogue->Tape()->deleteTape(tape.vid);
    }
  }
  {
    const auto mediaTypes = catalogue->MediaType()->getMediaTypes();
    for(const auto &mediaType: mediaTypes) {
      catalogue->MediaType()->deleteMediaType(mediaType.name);
    }
  }
  {
    const auto storageClasses = catalogue->StorageClass()->getStorageClasses();
    for(const auto &storageClass: storageClasses) {
      catalogue->StorageClass()->deleteStorageClass(storageClass.name);
    }
  }
  {
    const auto tapePools = catalogue->TapePool()->getTapePools();
    for(const auto &tapePool: tapePools) {
      catalogue->TapePool()->deleteTapePool(tapePool.name);
    }
  }
  {
    const auto logicalLibraries = catalogue->LogicalLibrary()->getLogicalLibraries();
    for(const auto &logicalLibrary: logicalLibraries) {
      catalogue->LogicalLibrary()->deleteLogicalLibrary(logicalLibrary.name);
    }
    const auto physicalLibraries = catalogue->PhysicalLibrary()->getPhysicalLibraries();
    for(const auto &physicalLibrary: physicalLibraries) {
      catalogue->PhysicalLibrary()->deletePhysicalLibrary(physicalLibrary.name);
    }
  }
  {
    const auto mountPolicies = catalogue->MountPolicy()->getMountPolicies();
    for(const auto &mountPolicy: mountPolicies) {
      catalogue->MountPolicy()->deleteMountPolicy(mountPolicy.name);
    }
  }
  {
    const auto diskSystems = catalogue->DiskSystem()->getAllDiskSystems();
    for(const auto &ds: diskSystems) {
      catalogue->DiskSystem()->deleteDiskSystem(ds.name);
    }
  }
  {
    const auto diskInstanceSpaces = catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    for(const auto &dis: diskInstanceSpaces) {
      catalogue->DiskInstanceSpace()->deleteDiskInstanceSpace(dis.name, dis.diskInstance);
    }
  }
  {
    const auto virtualOrganizations = catalogue->VO()->getVirtualOrganizations();
    for(const auto &vo: virtualOrganizations) {
      catalogue->VO()->deleteVirtualOrganization(vo.name);
    }
  }
  {
    const auto diskInstances = catalogue->DiskInstance()->getAllDiskInstances();
    for(const auto &di: diskInstances) {
      catalogue->DiskInstance()->deleteDiskInstance(di.name);
    }
  }
  checkWipedDatabase(catalogue);
}

void CatalogueTestUtils::checkWipedDatabase(cta::catalogue::Catalogue *catalogue) {
  if(!catalogue->AdminUser()->getAdminUsers().empty()) {
    throw cta::exception::Exception("Found one of more admin users after emptying the database");
  }

  if(catalogue->ArchiveFile()->getArchiveFilesItor().hasMore()) {
    throw cta::exception::Exception("Found one of more archive files after emptying the database");
  }

  if(!catalogue->ArchiveRoute()->getArchiveRoutes().empty()) {
    throw cta::exception::Exception("Found one of more archive routes after emptying the database");
  }

  if(catalogue->FileRecycleLog()->getFileRecycleLogItor().hasMore()){
    throw cta::exception::Exception("Found one or more files in the file recycle log after emptying the database");
  }

  if(!catalogue->DiskSystem()->getAllDiskSystems().empty()) {
    throw cta::exception::Exception("Found one or more disk systems after emptying the database");
  }

  if (!catalogue->DiskInstance()->getAllDiskInstances().empty()) {
    throw cta::exception::Exception("Found one or more disk instances after emptying the database");
  }

  if(!catalogue->LogicalLibrary()->getLogicalLibraries().empty()) {
    throw cta::exception::Exception("Found one or more logical libraries after emptying the database");
  }

  if(!catalogue->PhysicalLibrary()->getPhysicalLibraries().empty()) {
    throw cta::exception::Exception("Found one or more physical libraries after emptying the database");
  }

  if(!catalogue->MediaType()->getMediaTypes().empty()) {
    throw cta::exception::Exception("Found one or more media types after emptying the database");
  }

  if(!catalogue->MountPolicy()->getMountPolicies().empty()) {
    throw cta::exception::Exception("Found one or more mount policies after emptying the database");
  }

  if(!catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty()) {
    throw cta::exception::Exception("Found one or more requester group mount rules after emptying the database");
  }

  if(!catalogue->RequesterMountRule()->getRequesterMountRules().empty()) {
    throw cta::exception::Exception("Found one or more requester mount rules after emptying the database");
  }

  if(!catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty()) {
    throw cta::exception::Exception("Found one or more requester activity mount rules after emptying the database");
  }

  if(!catalogue->StorageClass()->getStorageClasses().empty()) {
    throw cta::exception::Exception("Found one or more storage classes after emptying the database");
  }

  if(!catalogue->Tape()->getTapes().empty()) {
    throw cta::exception::Exception("Found one or more tapes after emptying the database");
  }

  if(!catalogue->TapePool()->getTapePools().empty()) {
    throw cta::exception::Exception("Found one or more tape pools after emptying the database");
  }

  if(!catalogue->VO()->getVirtualOrganizations().empty()) {
    throw cta::exception::Exception("Found one or more virtual organizations after emptying the database");
  }
}

cta::common::dataStructures::SecurityIdentity CatalogueTestUtils::getLocalAdmin() {
  cta::common::dataStructures::SecurityIdentity localAdmin;
  localAdmin.username = "local_admin_user";
  localAdmin.host = "local_admin_host";

  return localAdmin;
}

cta::common::dataStructures::SecurityIdentity CatalogueTestUtils::getAdmin() {
  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  return admin;
}

cta::common::dataStructures::DiskInstance CatalogueTestUtils::getDiskInstance() {
  cta::common::dataStructures::DiskInstance di;
  di.name = "disk instance";
  di.comment = "Creation of disk instance";
  return di;
}

cta::common::dataStructures::VirtualOrganization CatalogueTestUtils::getVo() {
  cta::common::dataStructures::VirtualOrganization vo;
  vo.name = "vo";
  vo.comment = "Creation of virtual organization vo";
  vo.readMaxDrives = 1;
  vo.writeMaxDrives = 1;
  vo.maxFileSize = 0;
  vo.diskInstanceName = getDiskInstance().name;
  vo.isRepackVo = false;
  return vo;
}

cta::common::dataStructures::VirtualOrganization CatalogueTestUtils::getAnotherVo() {
  cta::common::dataStructures::VirtualOrganization vo;
  vo.name = "anotherVo";
  vo.comment = "Creation of another virtual organization vo";
  vo.readMaxDrives = 1;
  vo.writeMaxDrives = 1;
  vo.maxFileSize = 0;
  vo.diskInstanceName = getDiskInstance().name;
  vo.isRepackVo = false;
  return vo;
}

cta::common::dataStructures::VirtualOrganization CatalogueTestUtils::getDefaultRepackVo() {
  cta::common::dataStructures::VirtualOrganization vo;
  vo.name = "repack_vo";
  vo.comment = "Creation of virtual organization vo for repacking";
  vo.readMaxDrives = 1;
  vo.writeMaxDrives = 1;
  vo.maxFileSize = 0;
  vo.diskInstanceName = getDiskInstance().name;
  vo.isRepackVo = true;
  return vo;
}

cta::common::dataStructures::StorageClass CatalogueTestUtils::getStorageClass() {
  cta::common::dataStructures::StorageClass storageClass;
  storageClass.name = "storage_class_single_copy";
  storageClass.nbCopies = 1;
  storageClass.vo.name = getVo().name;
  storageClass.comment = "Creation of storage class with 1 copy on tape";
  return storageClass;
}

cta::common::dataStructures::StorageClass CatalogueTestUtils::getAnotherStorageClass() {
  cta::common::dataStructures::StorageClass storageClass;
  storageClass.name = "another_storage_class";
  storageClass.nbCopies = 1;
  storageClass.vo.name = getVo().name;
  storageClass.comment = "Creation of another storage class";
  return storageClass;
}

cta::common::dataStructures::StorageClass CatalogueTestUtils::getStorageClassDualCopy() {
  cta::common::dataStructures::StorageClass storageClass;
  storageClass.name = "storage_class_dual_copy";
  storageClass.nbCopies = 2;
  storageClass.vo.name = getVo().name;
  storageClass.comment = "Creation of storage class with 2 copies on tape";
  return storageClass;
}

cta::common::dataStructures::StorageClass CatalogueTestUtils::getStorageClassTripleCopy() {
  cta::common::dataStructures::StorageClass storageClass;
  storageClass.name = "storage_class_triple_copy";
  storageClass.nbCopies = 3;
  storageClass.vo.name = getVo().name;
  storageClass.comment = "Creation of storage class with 3 copies on tape";
  return storageClass;
}

cta::common::dataStructures::PhysicalLibrary CatalogueTestUtils::getPhysicalLibrary1() {
  cta::common::dataStructures::PhysicalLibrary pl;
  pl.name                      = "pl_name_1";
  pl.manufacturer              = "manufacturer_1";
  pl.model                     = "model_1";
  pl.nbPhysicalCartridgeSlots  = 10;
  pl.nbPhysicalDriveSlots      = 10;
  pl.nbAvailableCartridgeSlots = 5;
  return pl;
}

cta::common::dataStructures::PhysicalLibrary CatalogueTestUtils::getPhysicalLibrary2() {
  cta::common::dataStructures::PhysicalLibrary pl;
  pl.name                      = "pl_name_2";
  pl.manufacturer              = "manufacturer_2";
  pl.model                     = "model_2";
  pl.nbPhysicalCartridgeSlots  = 10;
  pl.nbPhysicalDriveSlots      = 10;
  pl.type                      = "type_2";
  pl.guiUrl                    = "url_2";
  pl.webcamUrl                 = "webcam_2";
  pl.location                  = "location_2";
  pl.nbAvailableCartridgeSlots = 5;
  pl.comment                   = "comment_2";
  return pl;
}

cta::common::dataStructures::PhysicalLibrary CatalogueTestUtils::getPhysicalLibrary3() {
  cta::common::dataStructures::PhysicalLibrary pl;
  pl.name                      = "pl_name_3";
  pl.manufacturer              = "manufacturer_3";
  pl.model                     = "model_3";
  pl.nbPhysicalCartridgeSlots  = 15;
  pl.nbPhysicalDriveSlots      = 15;
  pl.type                      = "type_3";
  pl.guiUrl                    = "url_3";
  pl.webcamUrl                 = "webcam_3";
  pl.location                  = "location_3";
  pl.nbAvailableCartridgeSlots = 10;
  pl.comment                   = "comment_3";
  return pl;
}

cta::catalogue::MediaType CatalogueTestUtils::getMediaType() {
  cta::catalogue::MediaType mediaType;
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

cta::catalogue::CreateTapeAttributes CatalogueTestUtils::getTape1() {
  cta::catalogue::CreateTapeAttributes tape;
  tape.vid = "VIDONE";
  tape.mediaType = getMediaType().name;
  tape.vendor = "vendor";
  tape.logicalLibraryName = "logical_library";
  tape.tapePoolName = "tape_pool";
  tape.full = false;
  tape.state = cta::common::dataStructures::Tape::ACTIVE;
  tape.comment = "Creation of tape one";
  return tape;
}

cta::catalogue::CreateTapeAttributes CatalogueTestUtils::getTape2() {
  // Tape 2 is an exact copy of tape 1 except for its VID and comment
  auto tape = getTape1();
  tape.vid = "VIDTWO";
  tape.comment = "Creation of tape two";
  return tape;
}

cta::catalogue::CreateTapeAttributes CatalogueTestUtils::getTape3() {
  // Tape 3 is an exact copy of tape 1 except for its VID and comment
  auto tape = getTape1();
  tape.vid = "VIDTHREE";
  tape.comment = "Creation of tape three";
  return tape;
}

cta::catalogue::CreateMountPolicyAttributes CatalogueTestUtils::getMountPolicy1() {
  cta::catalogue::CreateMountPolicyAttributes mountPolicy;
  mountPolicy.name = "mount_policy";
  mountPolicy.archivePriority = 1;
  mountPolicy.minArchiveRequestAge = 2;
  mountPolicy.retrievePriority = 3;
  mountPolicy.minRetrieveRequestAge = 4;
  mountPolicy.comment = "Create mount policy";
  return mountPolicy;
}

cta::catalogue::CreateMountPolicyAttributes CatalogueTestUtils::getMountPolicy2() {
  // Higher priority mount policy
  cta::catalogue::CreateMountPolicyAttributes mountPolicy;
  mountPolicy.name = "mount_policy_2";
  mountPolicy.archivePriority = 2;
  mountPolicy.minArchiveRequestAge = 1;
  mountPolicy.retrievePriority = 4;
  mountPolicy.minRetrieveRequestAge = 3;
  mountPolicy.comment = "Create mount policy";
  return mountPolicy;
}

std::map<std::string, cta::catalogue::TapePool> CatalogueTestUtils::tapePoolListToMap(
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

std::map<std::string, cta::common::dataStructures::Tape> CatalogueTestUtils::tapeListToMap(
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

std::map<uint64_t, cta::common::dataStructures::ArchiveFile> CatalogueTestUtils::archiveFileItorToMap(
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

std::map<uint64_t, cta::common::dataStructures::ArchiveFile> CatalogueTestUtils::archiveFileListToMap(
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

std::map<std::string, cta::common::dataStructures::AdminUser> CatalogueTestUtils::adminUserListToMap(
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

}  // namespace unitTests