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

#pragma once

#include "catalogue/interfaces/TapeCatalogue.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"

namespace cta {
namespace catalogue {

class DummyTapeCatalogue : public TapeCatalogue {
public:
  DummyTapeCatalogue() = default;
  ~DummyTapeCatalogue() override = default;

  void createTape(const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes & tape) override;

  void deleteTape(const std::string &vid) override;

  std::list<common::dataStructures::Tape> getTapes(const TapeSearchCriteria &searchCriteria) const override;

  common::dataStructures::Tape::State getTapeState(const std::string & vid) const;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::string& vid) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string> &vids) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string> &vids, bool ignoreMissingVids) const override;

  std::map<std::string, std::string> getVidToLogicalLibrary(const std::set<std::string> &vids) const override;

  void reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    cta::log::LogContext & lc) override;

  void checkTapeForLabel(const std::string &vid) override;

  void tapeLabelled(const std::string &vid, const std::string &drive) override;

  uint64_t getNbFilesOnTape(const std::string &vid) const override;

  void modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &mediaType) override;

  void modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &vendor) override;

  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &logicalLibraryName) override;

  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &tapePoolName) override;

  void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &encryptionKeyName) override;

  void modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &verificationStatus) override;

  void modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid,
    const common::dataStructures::Tape::State & state,
    const std::optional<common::dataStructures::Tape::State> & prev_state,
    const std::optional<std::string> & stateReason) override;

  bool tapeExists(const std::string &vid) const override;

  void setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const bool fullValue) override;

  void setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const bool dirtyValue) override;

  void setTapeIsFromCastorInUnitTests(const std::string &vid) override;

  void setTapeDirty(const std::string & vid) override;

  void modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::optional<std::string> &comment) override;

  void tapeMountedForArchive(const std::string &vid, const std::string &drive) override;

  void tapeMountedForRetrieve(const std::string &vid, const std::string &drive) override;

  void noSpaceLeftOnTape(const std::string &vid) override;

  std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const override;

  common::dataStructures::Label::Format getTapeLabelFormat(const std::string& vid) const override;

  // This two methods are for unit tests only in GarbageCollectorTests.cpp, they are not defined in the interface
  void addEnabledTape(const std::string & vid);
  void addDisabledTape(const std::string & vid);
  void addRepackingTape(const std::string & vid);

private:
  mutable threading::Mutex m_tapeEnablingMutex;
  std::map<std::string, common::dataStructures::Tape::State> m_tapeEnabling;
};

}  // namespace catalogue
}  // namespace cta