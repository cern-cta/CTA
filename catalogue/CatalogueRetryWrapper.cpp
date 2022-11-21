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

#include <memory>

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CatalogueRetryWrapper.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/retryOnLostConnection.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "catalogue/TapePool.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "disk/DiskSystem.hpp"

namespace cta {

namespace catalogue {

CatalogueRetryWrapper::CatalogueRetryWrapper(log::Logger &log, std::unique_ptr<Catalogue> catalogue,
  const uint32_t maxTriesToConnect):
  m_log(log),
  m_catalogue(std::move(catalogue)),
  m_maxTriesToConnect(maxTriesToConnect) {
}

void CatalogueRetryWrapper::tapeLabelled(const std::string &vid, const std::string &drive) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeLabelled(vid, drive);}, m_maxTriesToConnect);
}

uint64_t CatalogueRetryWrapper::checkAndGetNextArchiveFileId(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, storageClassName, user);}, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFileQueueCriteria CatalogueRetryWrapper::getArchiveFileQueueCriteria(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFileQueueCriteria(diskInstanceName, storageClassName, user);}, m_maxTriesToConnect);
}

std::list<TapeForWriting> CatalogueRetryWrapper::getTapesForWriting(const std::string &logicalLibraryName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapesForWriting(logicalLibraryName);}, m_maxTriesToConnect);
}

common::dataStructures::Label::Format CatalogueRetryWrapper::getTapeLabelFormat(const std::string& vid) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapeLabelFormat(vid);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->filesWrittenToTape(event);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::tapeMountedForArchive(const std::string &vid, const std::string &drive) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeMountedForArchive(vid, drive);}, m_maxTriesToConnect);
}

common::dataStructures::RetrieveFileQueueCriteria CatalogueRetryWrapper::prepareToRetrieveFile(const std::string& diskInstanceName, const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity& user, const std::optional<std::string>& activity, log::LogContext& lc, const std::optional<std::string> &mountPolicyName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->prepareToRetrieveFile(diskInstanceName, archiveFileId, user, activity, lc, mountPolicyName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::tapeMountedForRetrieve(const std::string &vid, const std::string &drive) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeMountedForRetrieve(vid, drive);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::noSpaceLeftOnTape(const std::string &vid) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->noSpaceLeftOnTape(vid);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createAdminUser(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createAdminUser(admin, username, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteAdminUser(const std::string &username) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteAdminUser(username);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::AdminUser> CatalogueRetryWrapper::getAdminUsers() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getAdminUsers();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyAdminUserComment(admin, username, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createVirtualOrganization(admin, vo);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteVirtualOrganization(const std::string &voName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteVirtualOrganization(voName);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::VirtualOrganization> CatalogueRetryWrapper::getVirtualOrganizations() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getVirtualOrganizations();}, m_maxTriesToConnect);
}

common::dataStructures::VirtualOrganization CatalogueRetryWrapper::getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getVirtualOrganizationOfTapepool(tapepoolName);}, m_maxTriesToConnect);
}

common::dataStructures::VirtualOrganization CatalogueRetryWrapper::getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getCachedVirtualOrganizationOfTapepool(tapepoolName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationName(admin,currentVoName,newVoName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationReadMaxDrives(admin,voName,readMaxDrives);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationWriteMaxDrives(admin,voName,writeMaxDrives);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationMaxFileSize(admin,voName,maxFileSize);}, m_maxTriesToConnect);
}


void CatalogueRetryWrapper::modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationComment(admin,voName,comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationDiskInstanceName(admin,voName,diskInstance);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createStorageClass(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::StorageClass &storageClass) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createStorageClass(admin, storageClass);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteStorageClass(const std::string &storageClassName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteStorageClass(storageClassName);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::StorageClass> CatalogueRetryWrapper::getStorageClasses() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getStorageClasses();}, m_maxTriesToConnect);
}

common::dataStructures::StorageClass CatalogueRetryWrapper::getStorageClass(const std::string &name) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getStorageClass(name);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbCopies) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassNbCopies(admin, name, nbCopies);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassComment(admin, name, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassName(admin, currentName, newName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassVo(admin, name, vo);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createMediaType(admin, mediaType);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteMediaType(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteMediaType(name);}, m_maxTriesToConnect);
}

std::list<MediaTypeWithLogs> CatalogueRetryWrapper::getMediaTypes() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getMediaTypes();}, m_maxTriesToConnect);
}

MediaType CatalogueRetryWrapper::getMediaTypeByVid(const std::string & vid) const {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->getMediaTypeByVid(vid);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeName(admin, currentName, newName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &cartridge) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeCartridge(admin, name, cartridge);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeCapacityInBytes(admin, name, capacityInBytes);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypePrimaryDensityCode(admin, name, primaryDensityCode);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeSecondaryDensityCode(admin, name, secondaryDensityCode);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint32_t> &nbWraps) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeNbWraps(admin, name, nbWraps);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &minLPos) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeMinLPos(admin, name, minLPos);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &maxLPos) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeMaxLPos(admin, name, maxLPos);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeComment(admin, name, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createTapePool(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue, const std::optional<std::string> &supply, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createTapePool(admin, name, vo, nbPartialTapes, encryptionValue, supply, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteTapePool(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteTapePool(name);}, m_maxTriesToConnect);
}

std::list<TapePool> CatalogueRetryWrapper::getTapePools(const TapePoolSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapePools(searchCriteria);}, m_maxTriesToConnect);
}

std::optional<TapePool> CatalogueRetryWrapper::getTapePool(const std::string &tapePoolName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapePool(tapePoolName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolVo(admin, name, vo);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbPartialTapes) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolNbPartialTapes(admin, name, nbPartialTapes);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolComment(admin, name, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool encryptionValue) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapePoolEncryption(admin, name, encryptionValue);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &supply) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolSupply(admin, name, supply);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolName(admin, currentName, newName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createArchiveRoute(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createArchiveRoute(admin, storageClassName, copyNb, tapePoolName, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteArchiveRoute(storageClassName, copyNb);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::ArchiveRoute> CatalogueRetryWrapper::getArchiveRoutes() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveRoutes();}, m_maxTriesToConnect);
}

std::list<common::dataStructures::ArchiveRoute> CatalogueRetryWrapper::getArchiveRoutes(const std::string &storageClassName, const std::string &tapePoolName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveRoutes(storageClassName, tapePoolName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyArchiveRouteTapePoolName(admin, storageClassName, copyNb, tapePoolName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyArchiveRouteComment(admin, storageClassName, copyNb, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool isDisabled, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createLogicalLibrary(admin, name, isDisabled, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteLogicalLibrary(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteLogicalLibrary(name);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::LogicalLibrary> CatalogueRetryWrapper::getLogicalLibraries() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getLogicalLibraries();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyLogicalLibraryName(admin, currentName, newName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyLogicalLibraryComment(admin, name, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &disabledReason) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyLogicalLibraryDisabledReason(admin, name, disabledReason);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->setLogicalLibraryDisabled(admin, name, disabledValue);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createTape( const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes & tape) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createTape(admin, tape);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteTape(const std::string &vid) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteTape(vid);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::Tape> CatalogueRetryWrapper::getTapes(const TapeSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapes(searchCriteria);}, m_maxTriesToConnect);
}

common::dataStructures::VidToTapeMap CatalogueRetryWrapper::getTapesByVid(const std::string& vid) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapesByVid(vid);}, m_maxTriesToConnect);
}

common::dataStructures::VidToTapeMap CatalogueRetryWrapper::getTapesByVid(const std::set<std::string> &vids) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapesByVid(vids);}, m_maxTriesToConnect);
}

std::map<std::string, std::string> CatalogueRetryWrapper::getVidToLogicalLibrary(const std::set<std::string> &vids) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getVidToLogicalLibrary(vids);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, cta::log::LogContext & lc) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->reclaimTape(admin, vid,lc);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::checkTapeForLabel(const std::string &vid) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->checkTapeForLabel(vid);}, m_maxTriesToConnect);
}

uint64_t CatalogueRetryWrapper::getNbFilesOnTape(const std::string &vid) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getNbFilesOnTape(vid);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &mediaType) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeMediaType(admin, vid, mediaType);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &vendor) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeVendor(admin, vid, vendor);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &logicalLibraryName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeLogicalLibraryName(admin, vid, logicalLibraryName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &tapePoolName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeTapePoolName(admin, vid, tapePoolName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &encryptionKeyName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeEncryptionKeyName(admin, vid, encryptionKeyName);}, m_maxTriesToConnect);
}
void CatalogueRetryWrapper::modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &verificationStatus) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeVerificationStatus(admin, vid, verificationStatus);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const std::optional<std::string> & stateReason) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeState(admin,vid, state, stateReason);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool fullValue) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeFull(admin, vid, fullValue);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool dirtyValue) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeDirty(admin, vid, dirtyValue);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setTapeIsFromCastorInUnitTests(const std::string &vid) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeIsFromCastorInUnitTests(vid);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setTapeDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string & reason) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeDisabled(admin, vid, reason);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setTapeDirty(const std::string & vid) {
  return retryOnLostConnection(m_log,[&]{ return m_catalogue->setTapeDirty(vid);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::optional<std::string> &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeComment(admin, vid, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &mountPolicy) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterActivityMountRulePolicy(admin, instanceName, requesterName, activityRegex, mountPolicy);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterActivityMountRuleComment(admin, instanceName, requesterName, activityRegex, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterMountRulePolicy(admin, instanceName, requesterName, mountPolicy);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesteMountRuleComment(admin, instanceName, requesterName, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterGroupMountRulePolicy(admin, instanceName, requesterGroupName, mountPolicy);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterGroupMountRuleComment(admin, instanceName, requesterGroupName, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createMountPolicy(const common::dataStructures::SecurityIdentity &admin, const CreateMountPolicyAttributes & mountPolicy) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createMountPolicy(admin, mountPolicy);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::MountPolicy> CatalogueRetryWrapper::getMountPolicies() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getMountPolicies();}, m_maxTriesToConnect);
}

std::optional<common::dataStructures::MountPolicy> CatalogueRetryWrapper::getMountPolicy(const std::string &mountPolicyName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getMountPolicy(mountPolicyName);}, m_maxTriesToConnect);
}


std::list<common::dataStructures::MountPolicy> CatalogueRetryWrapper::getCachedMountPolicies() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getCachedMountPolicies();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteMountPolicy(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteMountPolicy(name);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName, const std::string &activityRegex, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createRequesterActivityMountRule(admin, mountPolicyName, diskInstance, requesterName, activityRegex, comment);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::RequesterActivityMountRule> CatalogueRetryWrapper::getRequesterActivityMountRules() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getRequesterActivityMountRules();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteRequesterActivityMountRule(const std::string &diskInstanceName, const std::string &requesterName, const std::string &activityRegex) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteRequesterActivityMountRule(diskInstanceName, requesterName, activityRegex);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createRequesterMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createRequesterMountRule(admin, mountPolicyName, diskInstance, requesterName, comment);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::RequesterMountRule> CatalogueRetryWrapper::getRequesterMountRules() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getRequesterMountRules();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteRequesterMountRule(const std::string &diskInstanceName, const std::string &requesterName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteRequesterMountRule(diskInstanceName, requesterName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstanceName, const std::string &requesterGroupName, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createRequesterGroupMountRule(admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::RequesterGroupMountRule> CatalogueRetryWrapper::getRequesterGroupMountRules() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getRequesterGroupMountRules();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteRequesterGroupMountRule(const std::string &diskInstanceName, const std::string &requesterGroupName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t archivePriority) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyArchivePriority(admin, name, archivePriority);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minArchiveRequestAge) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyArchiveMinRequestAge(admin, name, minArchiveRequestAge);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t retrievePriority) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyRetrievePriority(admin, name, retrievePriority);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minRetrieveRequestAge) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyRetrieveMinRequestAge(admin, name, minRetrieveRequestAge);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyComment(admin, name, comment);}, m_maxTriesToConnect);
}

disk::DiskSystemList CatalogueRetryWrapper::getAllDiskSystems() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getAllDiskSystems();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment)   {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createDiskSystem(admin, name, diskInstanceName, diskInstanceSpaceName, fileRegexp, targetedFreeSpace, sleepTime, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteDiskSystem(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteDiskSystem(name);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &fileRegexp) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemFileRegexp(admin, name, fileRegexp);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t targetedFreeSpace) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemTargetedFreeSpace(admin, name, targetedFreeSpace);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemComment(admin, name, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemDiskInstanceName(admin, name, diskInstanceName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceSpaceName) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemDiskInstanceSpaceName(admin, name, diskInstanceSpaceName);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t sleepTime) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemSleepTime(admin, name, sleepTime);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createDiskInstance(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment)   {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createDiskInstance(admin, name, comment);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::DiskInstance> CatalogueRetryWrapper::getAllDiskInstances() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getAllDiskInstances();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteDiskInstance(const std::string &name) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteDiskInstance(name);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceComment(admin, name, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL, const uint64_t refreshInterval, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->createDiskInstanceSpace(admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);}, m_maxTriesToConnect);
}

std::list<common::dataStructures::DiskInstanceSpace> CatalogueRetryWrapper::getAllDiskInstanceSpaces() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getAllDiskInstanceSpaces();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteDiskInstanceSpace(name, diskInstance);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceComment(admin, name, diskInstance, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceRefreshInterval(admin, name, diskInstance, refreshInterval);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceQueryURL(admin, name, diskInstance, freeSpaceQueryURL);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyDiskInstanceSpaceFreeSpace(const std::string &name, const std::string &diskInstance, const uint64_t freeSpace) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceFreeSpace(name, diskInstance, freeSpace);}, m_maxTriesToConnect); 
}

using ArchiveFileItor = CatalogueItor<common::dataStructures::ArchiveFile>;
ArchiveFileItor CatalogueRetryWrapper::getArchiveFilesItor(const TapeFileSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFilesItor(searchCriteria);}, m_maxTriesToConnect);
}

using FileRecycleLogItor = CatalogueItor<common::dataStructures::FileRecycleLog>;
FileRecycleLogItor CatalogueRetryWrapper::getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getFileRecycleLogItor(searchCriteria);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->restoreFileInRecycleLog(searchCriteria, newFid);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc){
  return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteFileFromRecycleBin(archiveFileId,lc);},m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteFilesFromRecycleLog(const std::string & vid, log::LogContext & lc){
  return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteFilesFromRecycleLog(vid,lc);},m_maxTriesToConnect);
}

std::list<common::dataStructures::ArchiveFile> CatalogueRetryWrapper::getFilesForRepack(const std::string &vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getFilesForRepack(vid, startFSeq, maxNbFiles);}, m_maxTriesToConnect);
}

ArchiveFileItor CatalogueRetryWrapper::getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFilesForRepackItor(vid, startFSeq);}, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFileSummary CatalogueRetryWrapper::getTapeFileSummary(const TapeFileSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapeFileSummary(searchCriteria);}, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFile CatalogueRetryWrapper::getArchiveFileForDeletion(const TapeFileSearchCriteria &searchCriteria) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFileForDeletion(searchCriteria);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeFileAccessibility(const TapeFileSearchCriteria &criteria, const bool accessible) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeFileAccessibility(criteria, accessible);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteTapeFileCopy(file, reason);}, m_maxTriesToConnect);
}

common::dataStructures::ArchiveFile CatalogueRetryWrapper::getArchiveFileById(const uint64_t id) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFileById(id);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &instanceName, const uint64_t archiveFileId, log::LogContext &lc) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(instanceName, archiveFileId, lc);}, m_maxTriesToConnect);
}

bool CatalogueRetryWrapper::isAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->isAdmin(admin);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::ping() {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->ping();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::verifySchemaVersion() {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->verifySchemaVersion();}, m_maxTriesToConnect);
}

SchemaVersion CatalogueRetryWrapper::getSchemaVersion() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getSchemaVersion();}, m_maxTriesToConnect);
}

bool CatalogueRetryWrapper::tapePoolExists(const std::string &tapePoolName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->tapePoolExists(tapePoolName);}, m_maxTriesToConnect);
}

bool CatalogueRetryWrapper::tapeExists(const std::string &vid) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeExists(vid);}, m_maxTriesToConnect);
}

bool CatalogueRetryWrapper::diskSystemExists(const std::string &name) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->diskSystemExists(name);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance, const std::string &diskFileId) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->updateDiskFileId(archiveFileId, diskInstance, diskFileId);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
log::LogContext & lc) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->moveArchiveFileToRecycleLog(request,lc);},m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->createTapeDrive(tapeDrive);},m_maxTriesToConnect);
}

std::list<std::string> CatalogueRetryWrapper::getTapeDriveNames() const {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveNames();},m_maxTriesToConnect);
}

std::list<common::dataStructures::TapeDrive> CatalogueRetryWrapper::getTapeDrives() const {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDrives();},m_maxTriesToConnect);
}

std::optional<common::dataStructures::TapeDrive> CatalogueRetryWrapper::getTapeDrive(const std::string &tapeDriveName) const {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDrive(tapeDriveName);},m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setDesiredTapeDriveState(const std::string& tapeDriveName,
  const common::dataStructures::DesiredDriveState &desiredState) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->setDesiredTapeDriveState(tapeDriveName, desiredState);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
  const std::string &comment) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->setDesiredTapeDriveStateComment(tapeDriveName, comment);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::updateTapeDriveStatistics(const std::string& tapeDriveName,
  const std::string& host, const std::string& logicalLibrary,
  const common::dataStructures::TapeDriveStatistics& statistics) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->updateTapeDriveStatistics(tapeDriveName, host, logicalLibrary, statistics);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->updateTapeDriveStatus(tapeDrive);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteTapeDrive(const std::string &tapeDriveName) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteTapeDrive(tapeDriveName);},m_maxTriesToConnect);
}

void CatalogueRetryWrapper::createTapeDriveConfig(const std::string &driveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->createTapeDriveConfig(driveName, category, keyName, value, source);},m_maxTriesToConnect);
}

std::list<cta::catalogue::Catalogue::DriveConfig> CatalogueRetryWrapper::getTapeDriveConfigs() const {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveConfigs();},m_maxTriesToConnect);
}

std::list<std::pair<std::string, std::string>> CatalogueRetryWrapper::getTapeDriveConfigNamesAndKeys() const {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveConfigNamesAndKeys();},m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyTapeDriveConfig(const std::string &driveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->modifyTapeDriveConfig(driveName, category, keyName, value, source);},m_maxTriesToConnect);
}

std::optional<std::tuple<std::string, std::string, std::string>> CatalogueRetryWrapper::getTapeDriveConfig( const std::string &tapeDriveName,
  const std::string &keyName) const {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveConfig(tapeDriveName, keyName);},m_maxTriesToConnect);
}

void CatalogueRetryWrapper::deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {
  return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteTapeDriveConfig(tapeDriveName, keyName);},m_maxTriesToConnect);
}

std::map<std::string, uint64_t> CatalogueRetryWrapper::getDiskSpaceReservations() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->getDiskSpaceReservations();}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::reserveDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->reserveDiskSpace(driveName, mountId, diskSpaceReservation, lc);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::releaseDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->releaseDiskSpace(driveName, mountId, diskSpaceReservation, lc);}, m_maxTriesToConnect);
}

void CatalogueRetryWrapper::modifyArchiveFileStorageClassId(const uint64_t archiveFileId, const std::string& newStorageClassName) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyArchiveFileStorageClassId(archiveFileId, newStorageClassName);}, m_maxTriesToConnect);
}


}  // namespace catalogue
}  // namespace cta
