/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyTapeCatalogue.hpp"

#include "catalogue/CatalogueItor.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <map>
#include <memory>
#include <set>
#include <string>

namespace cta::catalogue {

void DummyTapeCatalogue::createTape(const common::dataStructures::SecurityIdentity& admin,
                                    const CreateTapeAttributes& tape) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::deleteTape(const std::string& vid) {
  throw exception::NotImplementedException();
}

TapeItor DummyTapeCatalogue::getTapesItor(const TapeSearchCriteria& searchCriteria) const {
  throw exception::NotImplementedException();
}

std::vector<common::dataStructures::Tape> DummyTapeCatalogue::getTapes(const TapeSearchCriteria& searchCriteria) const {
  throw exception::NotImplementedException();
}

common::dataStructures::VidToTapeMap DummyTapeCatalogue::getTapesByVid(const std::string& vid) const {
  std::set<std::string, std::less<>> vids = {vid};
  return getTapesByVid(vids);
}

common::dataStructures::VidToTapeMap
DummyTapeCatalogue::getTapesByVid(const std::set<std::string, std::less<>>& vids) const {
  return getTapesByVid(vids, false);
}

common::dataStructures::VidToTapeMap DummyTapeCatalogue::getTapesByVid(const std::set<std::string, std::less<>>& vids,
                                                                       bool ignoreMissingVids) const {
  // Minimal implementation of VidToMap for retrieve request unit tests. We just support
  // disabled status for the tapes.
  // If the tape is not listed, it is listed as enabled in the return value.
  threading::MutexLocker lm(m_tapeEnablingMutex);
  common::dataStructures::VidToTapeMap ret;
  for (const auto& v : vids) {
    try {
      ret[v].state = m_tapeEnabling.at(v);
    } catch (std::out_of_range&) {
      ret[v].state = common::dataStructures::Tape::ACTIVE;
    }
  }
  return ret;
}

std::map<std::string, std::string, std::less<>>
DummyTapeCatalogue::getVidToLogicalLibrary(const std::set<std::string, std::less<>>& vids) const {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity& admin,
                                     const std::string& vid,
                                     cta::log::LogContext& lc) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::checkTapeForLabel(const std::string& vid) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::tapeLabelled(const std::string& vid, const std::string& drive) {
  throw exception::NotImplementedException();
}

uint64_t DummyTapeCatalogue::getNbFilesOnTape(const std::string& vid) const {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyTapeMediaType(const common::dataStructures::SecurityIdentity& admin,
                                             const std::string& vid,
                                             const std::string& mediaType) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyTapeVendor(const common::dataStructures::SecurityIdentity& admin,
                                          const std::string& vid,
                                          const std::string& vendor) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
                                                      const std::string& vid,
                                                      const std::string& logicalLibraryName) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                                                const std::string& vid,
                                                const std::string& tapePoolName) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& vid,
                                                     const std::string& encryptionKeyName) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyPurchaseOrder(const common::dataStructures::SecurityIdentity& admin,
                                             const std::string& vid,
                                             const std::string& purchaseOrder) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity& admin,
                                                      const std::string& vid,
                                                      const std::string& verificationStatus) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity& admin,
                                     const std::string& vid,
                                     const bool fullValue) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::setTapeDirty(const common::dataStructures::SecurityIdentity& admin,
                                      const std::string& vid,
                                      const bool dirtyValue) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::setTapeIsFromCastorInUnitTests(const std::string& vid) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::setTapeDirty(const std::string& vid) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity& admin,
                                           const std::string& vid,
                                           const std::optional<std::string>& comment) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::tapeMountedForArchive(const std::string& vid, const std::string& drive) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::tapeMountedForRetrieve(const std::string& vid, const std::string& drive) {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::noSpaceLeftOnTape(const std::string& vid) {
  throw exception::NotImplementedException();
}

std::vector<TapeForWriting> DummyTapeCatalogue::getTapesForWriting(const std::string& logicalLibraryName) const {
  throw exception::NotImplementedException();
}

common::dataStructures::Label::Format DummyTapeCatalogue::getTapeLabelFormat(const std::string& vid) const {
  throw exception::NotImplementedException();
}

void DummyTapeCatalogue::addRepackingTape(const std::string& vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid] = common::dataStructures::Tape::REPACKING;
}

void DummyTapeCatalogue::modifyTapeState(const common::dataStructures::SecurityIdentity& admin,
                                         const std::string& vid,
                                         const common::dataStructures::Tape::State& state,
                                         const std::optional<common::dataStructures::Tape::State>& prev_state,
                                         const std::optional<std::string>& stateReason) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  if (prev_state.has_value() && prev_state.value() != m_tapeEnabling[vid]) {
    throw exception::Exception("Previous state mismatch");
  }
  m_tapeEnabling[vid] = state;
}

bool DummyTapeCatalogue::tapeExists(const std::string& vid) const {
  return m_tapeEnabling.contains(vid);
}

common::dataStructures::Tape::State DummyTapeCatalogue::getTapeState(const std::string& vid) const {
  return m_tapeEnabling.at(vid);
}

// Special functions for unit tests.
void DummyTapeCatalogue::addEnabledTape(const std::string& vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid] = common::dataStructures::Tape::ACTIVE;
}

void DummyTapeCatalogue::addDisabledTape(const std::string& vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid] = common::dataStructures::Tape::DISABLED;
}

}  // namespace cta::catalogue
