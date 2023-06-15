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

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "catalogue/retrywrappers/TapeCatalogueRetryWrapper.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"

namespace cta {
namespace catalogue {

TapeCatalogueRetryWrapper::TapeCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                                     log::Logger& log,
                                                     const uint32_t maxTriesToConnect) :
m_catalogue(catalogue),
m_log(log),
m_maxTriesToConnect(maxTriesToConnect) {}

void TapeCatalogueRetryWrapper::createTape(const common::dataStructures::SecurityIdentity& admin,
                                           const CreateTapeAttributes& tape) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->createTape(admin, tape); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::deleteTape(const std::string& vid) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->deleteTape(vid); }, m_maxTriesToConnect);
}

std::list<common::dataStructures::Tape>
  TapeCatalogueRetryWrapper::getTapes(const TapeSearchCriteria& searchCriteria) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getTapes(searchCriteria); }, m_maxTriesToConnect);
}

common::dataStructures::VidToTapeMap TapeCatalogueRetryWrapper::getTapesByVid(const std::string& vid) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getTapesByVid(vid); }, m_maxTriesToConnect);
}

common::dataStructures::VidToTapeMap TapeCatalogueRetryWrapper::getTapesByVid(const std::set<std::string>& vids) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getTapesByVid(vids); }, m_maxTriesToConnect);
}

common::dataStructures::VidToTapeMap TapeCatalogueRetryWrapper::getTapesByVid(const std::set<std::string>& vids,
                                                                              bool ignoreMissingVids) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getTapesByVid(vids, ignoreMissingVids); }, m_maxTriesToConnect);
}

std::map<std::string, std::string>
  TapeCatalogueRetryWrapper::getVidToLogicalLibrary(const std::set<std::string>& vids) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getVidToLogicalLibrary(vids); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::reclaimTape(const common::dataStructures::SecurityIdentity& admin,
                                            const std::string& vid,
                                            cta::log::LogContext& lc) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->reclaimTape(admin, vid, lc); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::checkTapeForLabel(const std::string& vid) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->checkTapeForLabel(vid); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::tapeLabelled(const std::string& vid, const std::string& drive) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->tapeLabelled(vid, drive); }, m_maxTriesToConnect);
}

uint64_t TapeCatalogueRetryWrapper::getNbFilesOnTape(const std::string& vid) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getNbFilesOnTape(vid); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeMediaType(const common::dataStructures::SecurityIdentity& admin,
                                                    const std::string& vid,
                                                    const std::string& mediaType) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeMediaType(admin, vid, mediaType); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeVendor(const common::dataStructures::SecurityIdentity& admin,
                                                 const std::string& vid,
                                                 const std::string& vendor) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeVendor(admin, vid, vendor); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
                                                             const std::string& vid,
                                                             const std::string& logicalLibraryName) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeLogicalLibraryName(admin, vid, logicalLibraryName); },
    m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                                                       const std::string& vid,
                                                       const std::string& tapePoolName) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeTapePoolName(admin, vid, tapePoolName); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& vid,
                                                            const std::string& encryptionKeyName) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeEncryptionKeyName(admin, vid, encryptionKeyName); },
    m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity& admin,
                                                             const std::string& vid,
                                                             const std::string& verificationStatus) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeVerificationStatus(admin, vid, verificationStatus); },
    m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeState(const common::dataStructures::SecurityIdentity& admin,
                                                const std::string& vid,
                                                const common::dataStructures::Tape::State& state,
                                                const std::optional<common::dataStructures::Tape::State>& prev_state,
                                                const std::optional<std::string>& stateReason) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeState(admin, vid, state, prev_state, stateReason); },
    m_maxTriesToConnect);
}

bool TapeCatalogueRetryWrapper::tapeExists(const std::string& vid) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->tapeExists(vid); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::setTapeFull(const common::dataStructures::SecurityIdentity& admin,
                                            const std::string& vid,
                                            const bool fullValue) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->setTapeFull(admin, vid, fullValue); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::setTapeDirty(const common::dataStructures::SecurityIdentity& admin,
                                             const std::string& vid,
                                             const bool dirtyValue) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->setTapeDirty(admin, vid, dirtyValue); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::setTapeIsFromCastorInUnitTests(const std::string& vid) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->setTapeIsFromCastorInUnitTests(vid); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::setTapeDirty(const std::string& vid) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->setTapeDirty(vid); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::modifyTapeComment(const common::dataStructures::SecurityIdentity& admin,
                                                  const std::string& vid,
                                                  const std::optional<std::string>& comment) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->modifyTapeComment(admin, vid, comment); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::tapeMountedForArchive(const std::string& vid, const std::string& drive) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->tapeMountedForArchive(vid, drive); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::tapeMountedForRetrieve(const std::string& vid, const std::string& drive) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->tapeMountedForRetrieve(vid, drive); }, m_maxTriesToConnect);
}

void TapeCatalogueRetryWrapper::noSpaceLeftOnTape(const std::string& vid) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->noSpaceLeftOnTape(vid); }, m_maxTriesToConnect);
}

std::list<TapeForWriting> TapeCatalogueRetryWrapper::getTapesForWriting(const std::string& logicalLibraryName) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getTapesForWriting(logicalLibraryName); }, m_maxTriesToConnect);
}

common::dataStructures::Label::Format TapeCatalogueRetryWrapper::getTapeLabelFormat(const std::string& vid) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->Tape()->getTapeLabelFormat(vid); }, m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta