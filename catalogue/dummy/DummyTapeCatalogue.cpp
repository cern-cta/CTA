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
#include <map>
#include <memory>
#include <set>
#include <string>

#include "catalogue/dummy/DummyTapeCatalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

void DummyTapeCatalogue::createTape(const common::dataStructures::SecurityIdentity &admin,
  const CreateTapeAttributes & tape) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::deleteTape(const std::string &vid) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::Tape> DummyTapeCatalogue::getTapes(
  const TapeSearchCriteria &searchCriteria) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

common::dataStructures::VidToTapeMap DummyTapeCatalogue::getTapesByVid(const std::string& vid) const {
  std::set<std::string> vids = {vid};
  return getTapesByVid(vids);
}

common::dataStructures::VidToTapeMap DummyTapeCatalogue::getTapesByVid(const std::set<std::string> &vids) const {
  return getTapesByVid(vids, false);
}

common::dataStructures::VidToTapeMap DummyTapeCatalogue::getTapesByVid(const std::set<std::string> &vids, bool ignoreMissingVids) const {
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

std::map<std::string, std::string> DummyTapeCatalogue::getVidToLogicalLibrary(
  const std::set<std::string> &vids) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, cta::log::LogContext & lc) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::checkTapeForLabel(const std::string &vid) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::tapeLabelled(const std::string &vid, const std::string &drive) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

uint64_t DummyTapeCatalogue::getNbFilesOnTape(const std::string &vid) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &mediaType) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &vendor) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &logicalLibraryName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &tapePoolName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &encryptionKeyName) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &verificationStatus) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const bool fullValue) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const bool dirtyValue) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::setTapeIsFromCastorInUnitTests(const std::string &vid) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::setTapeDirty(const std::string & vid) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::optional<std::string> &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::tapeMountedForArchive(const std::string &vid, const std::string &drive) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::tapeMountedForRetrieve(const std::string &vid, const std::string &drive) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::noSpaceLeftOnTape(const std::string &vid) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<TapeForWriting> DummyTapeCatalogue::getTapesForWriting(const std::string &logicalLibraryName) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

common::dataStructures::Label::Format DummyTapeCatalogue::getTapeLabelFormat(const std::string& vid) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyTapeCatalogue::addRepackingTape(const std::string & vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid]=common::dataStructures::Tape::REPACKING;
}

void DummyTapeCatalogue::modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid,
const common::dataStructures::Tape::State & state,
  const std::optional<common::dataStructures::Tape::State> & prev_state,
  const std::optional<std::string> & stateReason) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  if (prev_state.has_value() && prev_state.value() != m_tapeEnabling[vid]) {
    throw exception::Exception("Previous state mismatch");
  }
  m_tapeEnabling[vid]=state;
}

bool DummyTapeCatalogue::tapeExists(const std::string& vid) const {
  return m_tapeEnabling.find(vid) != m_tapeEnabling.end();
}

common::dataStructures::Tape::State DummyTapeCatalogue::getTapeState(const std::string & vid) const {
  return m_tapeEnabling.at(vid);
}

// Special functions for unit tests.
void DummyTapeCatalogue::addEnabledTape(const std::string & vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid]=common::dataStructures::Tape::ACTIVE;
}
void DummyTapeCatalogue::addDisabledTape(const std::string & vid) {
  threading::MutexLocker lm(m_tapeEnablingMutex);
  m_tapeEnabling[vid]=common::dataStructures::Tape::DISABLED;
}

} // namespace catalogue
} // namespace cta