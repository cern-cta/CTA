/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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
#include <map>

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/DummyCatalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "catalogue/TapePool.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeDriveStatistics.hpp"
#include "common/threading/MutexLocker.hpp"

namespace cta {
namespace catalogue {

void DummyCatalogue::createAdminUser(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createArchiveRoute(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& tapePoolName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const bool isDisabled, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createMountPolicy(const common::dataStructures::SecurityIdentity& admin, const CreateMountPolicyAttributes & mountPolicy) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstanceName, const std::string& requesterGroupName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createRequesterMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstance, const std::string& requesterName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstance, const std::string& requesterName, const std::string &activityRegex, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createStorageClass(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::StorageClass& storageClass) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createTape(const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes & tape) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteMediaType(const std::string &name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<MediaTypeWithLogs> DummyCatalogue::getMediaTypes() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
MediaType DummyCatalogue::getMediaTypeByVid(const std::string & vid) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &cartridge) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint32_t> &nbWraps) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &minLPos) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &maxLPos) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createTapePool(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string & vo, const uint64_t nbPartialTapes, const bool encryptionValue, const std::optional<std::string> &supply, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteAdminUser(const std::string& username) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string& instanceName, const uint64_t archiveFileId, log::LogContext &lc) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteArchiveRoute(const std::string& storageClassName, const uint32_t copyNb) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteLogicalLibrary(const std::string& name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteMountPolicy(const std::string& name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteRequesterGroupMountRule(const std::string& diskInstanceName, const std::string& requesterGroupName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteRequesterMountRule(const std::string& diskInstanceName, const std::string& requesterName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteRequesterActivityMountRule(const std::string& diskInstanceName, const std::string& requesterName, const std::string &activityRegex) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteStorageClass(const std::string& storageClassName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteTape(const std::string& vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteTapePool(const std::string& name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteDiskSystem(const std::string &name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &fileRegexp) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t targetedFreeSpace) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t sleepTime) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceSpaceName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::list<common::dataStructures::DiskInstance> DummyCatalogue::getAllDiskInstances() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::deleteDiskInstance(const std::string &name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::list<common::dataStructures::DiskInstanceSpace> DummyCatalogue::getAllDiskInstanceSpaces() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::list<common::dataStructures::AdminUser> DummyCatalogue::getAdminUsers() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::ArchiveFile DummyCatalogue::getArchiveFileById(const uint64_t id) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
using ArchiveFileItor = CatalogueItor<common::dataStructures::ArchiveFile>;
ArchiveFileItor DummyCatalogue::getArchiveFilesItor(const TapeFileSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
using FileRecycleLogItor = CatalogueItor<common::dataStructures::FileRecycleLog>;
FileRecycleLogItor DummyCatalogue::getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::deleteFilesFromRecycleLog(const std::string & vid, log::LogContext & lc) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::deleteTapeDrive(const std::string &tapeDriveName) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category, const std::string &keyName, const std::string &value, const std::string &source) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::list<cta::catalogue::Catalogue::DriveConfig> DummyCatalogue::getTapeDriveConfigs() const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::list<std::pair<std::string, std::string>> DummyCatalogue::getTapeDriveConfigNamesAndKeys() const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category, const std::string &keyName, const std::string &value, const std::string &source) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::optional<std::tuple<std::string, std::string, std::string>> DummyCatalogue::getTapeDriveConfig( const std::string &tapeDriveName, const std::string &keyName) const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::list<common::dataStructures::ArchiveFile> DummyCatalogue::getFilesForRepack(const std::string &vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
ArchiveFileItor DummyCatalogue::getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::ArchiveRoute> DummyCatalogue::getArchiveRoutes() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::ArchiveRoute> DummyCatalogue::getArchiveRoutes(const std::string &storageClassName, const std::string &tapePoolName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::LogicalLibrary> DummyCatalogue::getLogicalLibraries() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::RequesterGroupMountRule> DummyCatalogue::getRequesterGroupMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::RequesterMountRule> DummyCatalogue::getRequesterMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::RequesterActivityMountRule> DummyCatalogue::getRequesterActivityMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::StorageClass> DummyCatalogue::getStorageClasses() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::StorageClass DummyCatalogue::getStorageClass(const std::string &name) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::ArchiveFileSummary DummyCatalogue::getTapeFileSummary(const TapeFileSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::ArchiveFile DummyCatalogue::getArchiveFileForDeletion(const TapeFileSearchCriteria &searchCriteria) const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
std::list<TapePool> DummyCatalogue::getTapePools(const TapePoolSearchCriteria &searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::optional<TapePool> DummyCatalogue::getTapePool(const std::string &tapePoolName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::optional<common::dataStructures::MountPolicy> DummyCatalogue::getMountPolicy(const std::string &mountPolicyName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::Tape> DummyCatalogue::getTapes(const TapeSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::map<std::string, std::string> DummyCatalogue::getVidToLogicalLibrary(const std::set<std::string> &vids) const { throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented"); }
common::dataStructures::Label::Format DummyCatalogue::getTapeLabelFormat(const std::string& vid) const { throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented"); }
std::list<TapeForWriting> DummyCatalogue::getTapesForWriting(const std::string& logicalLibraryName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
bool DummyCatalogue::isAdmin(const common::dataStructures::SecurityIdentity& admin) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::deleteVirtualOrganization(const std::string &voName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
std::list<common::dataStructures::VirtualOrganization> DummyCatalogue::getVirtualOrganizations() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::VirtualOrganization DummyCatalogue::getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::VirtualOrganization DummyCatalogue::getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& tapePoolName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& disabledReason) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minArchiveRequestAge) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t archivePriority) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minRetrieveRequestAge) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t retrievePriority) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string &activityRegex, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& mountPolicy) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& mountPolicy) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string &activityRegex, const std::string& mountPolicy) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyStorageClassName(const common::dataStructures::SecurityIdentity& admin, const std::string& currentName, const std::string& newName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbCopies) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::optional<std::string> &comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& encryptionKeyName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& verificationStatus) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapeMediaType(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& mediaType) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapeVendor(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& vendor) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& logicalLibraryName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbPartialTapes) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& supply) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& currentName, const std::string& newName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& tapePoolName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::noSpaceLeftOnTape(const std::string& vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::ping() { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::verifySchemaVersion() { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
SchemaVersion DummyCatalogue::getSchemaVersion() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
uint64_t DummyCatalogue::checkAndGetNextArchiveFileId(const std::string &diskInstanceName, const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::ArchiveFileQueueCriteria DummyCatalogue::getArchiveFileQueueCriteria(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
common::dataStructures::RetrieveFileQueueCriteria DummyCatalogue::prepareToRetrieveFile(const std::string& diskInstanceName, const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity& user, const std::optional<std::string>& activity, log::LogContext& lc, const std::optional<std::string> &mountPolicyName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, cta::log::LogContext & lc) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::checkTapeForLabel(const std::string& vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
uint64_t DummyCatalogue::getNbFilesOnTape(const std::string& vid) const  { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setTapeDisabled(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string & reason) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setTapeRepackingDisabled(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string & reason) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool fullValue) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setTapeDirty(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool dirtyValue) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setTapeDirty(const std::string & vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setTapeIsFromCastorInUnitTests(const std::string &vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const bool encryptionValue) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
bool DummyCatalogue::diskSystemExists(const std::string& name) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::tapeLabelled(const std::string& vid, const std::string& drive) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::tapeMountedForArchive(const std::string& vid, const std::string& drive) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::tapeMountedForRetrieve(const std::string& vid, const std::string& drive) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
bool DummyCatalogue::tapePoolExists(const std::string& tapePoolName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance, const std::string &diskFileId) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
void DummyCatalogue::modifyArchiveFileStorageClassId(const uint64_t archiveFileId, const std::string &newStorageClassName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
void DummyCatalogue::modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId, const std::string& fxId, const std::string &diskInstance) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }


// Special functions for unit tests.
void DummyCatalogue::addEnabledTape(const std::string & vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid]=common::dataStructures::Tape::ACTIVE;
}
void DummyCatalogue::addDisabledTape(const std::string & vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid]=common::dataStructures::Tape::DISABLED;
}
void DummyCatalogue::addRepackingTape(const std::string & vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid]=common::dataStructures::Tape::REPACKING;
}
void DummyCatalogue::modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const std::optional<common::dataStructures::Tape::State> & prev_state, const std::optional<std::string> & stateReason) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  if (prev_state.has_value() && prev_state.value() != m_tapeEnabling[vid]) {
    throw exception::Exception("Previous state mismatch");
  }
  m_tapeEnabling[vid]=state;
}
bool DummyCatalogue::tapeExists(const std::string& vid) const {
  return m_tapeEnabling.find(vid) != m_tapeEnabling.end();
}
common::dataStructures::Tape::State DummyCatalogue::getTapeState(const std::string & vid) const {
  return m_tapeEnabling.at(vid);
}
common::dataStructures::VidToTapeMap DummyCatalogue::getTapesByVid(const std::string& vid) const {
  std::set<std::string> vids = {vid};
  return getTapesByVid(vids);
}
common::dataStructures::VidToTapeMap DummyCatalogue::getTapesByVid(const std::set<std::string>& vids) const {
  // Minimal implementation of VidToMap for retrieve request unit tests. We just support
  // disabled status for the tapes.
  // If the tape is not listed, it is listed as enabled in the return value.
  threading::MutexLocker lm(m_tapeEnablingMutex);
  common::dataStructures::VidToTapeMap ret;
  for (const auto & v: vids) {
    try {
      ret[v].state = m_tapeEnabling.at(v);
    } catch (std::out_of_range &) {
      ret[v].state = common::dataStructures::Tape::ACTIVE;
    }
  }
  return ret;
}
std::list<common::dataStructures::MountPolicy> DummyCatalogue::getMountPolicies() const {
  std::list<common::dataStructures::MountPolicy> mountPolicies;
  common::dataStructures::MountPolicy mp1;
  mp1.name = "mountPolicy";
  mp1.archivePriority = 1;
  mp1.archiveMinRequestAge = 0;
  mp1.retrievePriority = 1;
  mp1.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);

  common::dataStructures::MountPolicy mp2;
  mp2.name = "moreAdvantageous";
  mp2.archivePriority = 2;
  mp2.archiveMinRequestAge = 0;
  mp2.retrievePriority = 2;
  mp2.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);
  return mountPolicies;
}

std::list<common::dataStructures::MountPolicy> DummyCatalogue::getCachedMountPolicies() const {
  std::list<common::dataStructures::MountPolicy> mountPolicies;
  common::dataStructures::MountPolicy mp1;
  mp1.name = "mountPolicy";
  mp1.archivePriority = 1;
  mp1.archiveMinRequestAge = 0;
  mp1.retrievePriority = 1;
  mp1.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);

  common::dataStructures::MountPolicy mp2;
  mp2.name = "moreAdvantageous";
  mp2.archivePriority = 2;
  mp2.archiveMinRequestAge = 0;
  mp2.retrievePriority = 2;
  mp2.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);
  return mountPolicies;
}

std::list<std::string> DummyCatalogue::getTapeDriveNames() const {
  return {m_tapeDriveStatus.driveName};
}

std::optional<common::dataStructures::TapeDrive> DummyCatalogue::getTapeDrive(const std::string &tapeDriveName) const {
  if (m_tapeDriveStatus.driveName != "") return m_tapeDriveStatus;
  common::dataStructures::TapeDrive tapeDriveStatus;
  const time_t reportTime = time(nullptr);

  tapeDriveStatus.driveName = tapeDriveName;
  tapeDriveStatus.host = "Dummy_Host";
  tapeDriveStatus.logicalLibrary = "Dummy_Library";

  tapeDriveStatus.downOrUpStartTime = reportTime;

  tapeDriveStatus.mountType = common::dataStructures::MountType::NoMount;
  tapeDriveStatus.driveStatus = common::dataStructures::DriveStatus::Down;
  tapeDriveStatus.desiredUp = false;
  tapeDriveStatus.desiredForceDown = false;

  tapeDriveStatus.diskSystemName = "Dummy_System";
  tapeDriveStatus.reservedBytes = 0;
  tapeDriveStatus.reservationSessionId = 0;


  return tapeDriveStatus;
}

std::list<common::dataStructures::TapeDrive> DummyCatalogue::getTapeDrives() const {
  std::list<common::dataStructures::TapeDrive> tapeDrives;
  const auto tapeDrive = getTapeDrive(m_tapeDriveStatus.driveName);
  if (tapeDrive.has_value()) tapeDrives.push_back(tapeDrive.value());
  return tapeDrives;
}

void DummyCatalogue::setDesiredTapeDriveState(const std::string&,
    const common::dataStructures::DesiredDriveState &desiredState) {
  m_tapeDriveStatus.desiredUp = desiredState.up;
  m_tapeDriveStatus.desiredForceDown = desiredState.forceDown;
  m_tapeDriveStatus.reasonUpDown = desiredState.reason;
}

void DummyCatalogue::setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
  const std::string &comment) {
  m_tapeDriveStatus.userComment = comment;
}

void DummyCatalogue::updateTapeDriveStatistics(const std::string& tapeDriveName,
  const std::string& host, const std::string& logicalLibrary,
  const common::dataStructures::TapeDriveStatistics& statistics) {
  m_tapeDriveStatus.driveName = tapeDriveName;
  m_tapeDriveStatus.host = host;
  m_tapeDriveStatus.logicalLibrary = logicalLibrary;
  m_tapeDriveStatus.bytesTransferedInSession = statistics.bytesTransferedInSession;
  m_tapeDriveStatus.filesTransferedInSession = statistics.filesTransferedInSession;
  m_tapeDriveStatus.lastModificationLog = statistics.lastModificationLog;
}

void DummyCatalogue::updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) {
  m_tapeDriveStatus = tapeDrive;
}

std::map<std::string, uint64_t> DummyCatalogue::getDiskSpaceReservations() const {
  std::map<std::string, uint64_t> ret;
  const auto tdNames = getTapeDriveNames();
  for (const auto& driveName : tdNames) {
    const auto tdStatus = getTapeDrive(driveName);
    if (tdStatus.value().diskSystemName) {
      // no need to check key, operator[] initializes missing values at zero for scalar types
      ret[tdStatus.value().diskSystemName.value()] += tdStatus.value().reservedBytes.value();
    }
  }
  return ret;
}

void DummyCatalogue::reserveDiskSpace(const std::string& driveName, const uint64_t mountId,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;

  log::ScopedParamContainer params(lc);
  params.add("driveName", driveName)
        .add("diskSystem", diskSpaceReservation.begin()->first)
        .add("reservationBytes", diskSpaceReservation.begin()->second)
        .add("mountId", mountId);
  lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): reservation request.");

  auto tdStatus = getTapeDrive(driveName);
  if (!tdStatus) return;

  if (!tdStatus.value().reservationSessionId) {
    tdStatus.value().reservationSessionId = mountId;
    tdStatus.value().reservedBytes = 0;
  }

  if (tdStatus.value().reservationSessionId != mountId) {
    tdStatus.value().reservationSessionId = mountId;
    tdStatus.value().reservedBytes = 0;
  }

  tdStatus.value().diskSystemName = diskSpaceReservation.begin()->first;
  tdStatus.value().reservedBytes.value() += diskSpaceReservation.begin()->second;
  updateTapeDriveStatus(tdStatus.value());
}

void DummyCatalogue::releaseDiskSpace(const std::string& driveName, const uint64_t mountId,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;

  log::ScopedParamContainer params(lc);
  params.add("driveName", driveName)
        .add("diskSystem", diskSpaceReservation.begin()->first)
        .add("reservationBytes", diskSpaceReservation.begin()->second)
        .add("mountId", mountId);
  lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): reservation release request.");

  auto tdStatus = getTapeDrive(driveName);

  if (!tdStatus) return;
  if (!tdStatus.value().reservationSessionId) {
    return;
  }
  if (tdStatus.value().reservationSessionId != mountId) {
    return;
  }
  auto& bytes = diskSpaceReservation.begin()->second;
  if (bytes > tdStatus.value().reservedBytes) throw NegativeDiskSpaceReservationReached(
    "In DriveState::subtractDiskSpaceReservation(): we would reach a negative reservation size.");
  tdStatus.value().diskSystemName = diskSpaceReservation.begin()->first;
  tdStatus.value().reservedBytes.value() -= bytes;
  updateTapeDriveStatus(tdStatus.value());
}


/*
  * Implemented for testing disk space reservation logic
  */
disk::DiskSystemList DummyCatalogue::getAllDiskSystems() const {
  return m_diskSystemList;
}

void DummyCatalogue::createDiskInstance(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  m_diskInstances[name] = {name, comment, common::dataStructures::EntryLog(), common::dataStructures::EntryLog()};
}

void DummyCatalogue::createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL, const uint64_t refreshInterval, const std::string &comment) {
  m_diskInstanceSpaces[name] = {name, diskInstance, freeSpaceQueryURL, refreshInterval, 0, 0, comment, common::dataStructures::EntryLog(), common::dataStructures::EntryLog()};
}

void DummyCatalogue::createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment)  {
  m_diskSystemList.push_back({name, m_diskInstanceSpaces.at(diskInstanceSpaceName), fileRegexp, targetedFreeSpace, sleepTime, common::dataStructures::EntryLog(), common::dataStructures::EntryLog{}, comment});
}

void DummyCatalogue::modifyDiskInstanceSpaceFreeSpace(const std::string &name, const std::string &diskInstance, const uint64_t freeSpace) {
  m_diskInstanceSpaces[name].freeSpace = freeSpace;
  m_diskInstanceSpaces[name].lastRefreshTime = time(nullptr);
}

}  // namespace catalogue
}  // namespace cta
