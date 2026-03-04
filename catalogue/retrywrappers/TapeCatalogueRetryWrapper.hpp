/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/TapeCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta::catalogue {

class Catalogue;
class SchemaVersion;

class TapeCatalogueRetryWrapper : public TapeCatalogue {
public:
  TapeCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~TapeCatalogueRetryWrapper() override = default;

  void createTape(const common::dataStructures::SecurityIdentity& admin, const CreateTapeAttributes& tape) override;

  void deleteTape(const std::string& vid) override;

  TapeItor getTapesItor(const TapeSearchCriteria& searchCriteria) const override;

  std::vector<common::dataStructures::Tape> getTapes(const TapeSearchCriteria& searchCriteria) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::string& vid) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string, std::less<>>& vids) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string, std::less<>>& vids,
                                                     bool ignoreMissingVids) const override;

  std::map<std::string, std::string, std::less<>>
  getVidToLogicalLibrary(const std::set<std::string, std::less<>>& vids) const override;

  void reclaimTape(const common::dataStructures::SecurityIdentity& admin,
                   const std::string& vid,
                   cta::log::LogContext& lc) override;

  void checkTapeForLabel(const std::string& vid) override;

  void tapeLabelled(const std::string& vid, const std::string& drive) override;

  uint64_t getNbFilesOnTape(const std::string& vid) const override;

  void modifyTapeMediaType(const common::dataStructures::SecurityIdentity& admin,
                           const std::string& vid,
                           const std::string& mediaType) override;

  void modifyTapeVendor(const common::dataStructures::SecurityIdentity& admin,
                        const std::string& vid,
                        const std::string& vendor) override;

  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
                                    const std::string& vid,
                                    const std::string& logicalLibraryName) override;

  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& vid,
                              const std::string& tapePoolName) override;

  void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity& admin,
                                   const std::string& vid,
                                   const std::string& encryptionKeyName) override;

  void modifyPurchaseOrder(const common::dataStructures::SecurityIdentity& admin,
                           const std::string& vid,
                           const std::string& purchaseOrder) override;

  void modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity& admin,
                                    const std::string& vid,
                                    const std::string& verificationStatus) override;

  void modifyTapeState(const common::dataStructures::SecurityIdentity& admin,
                       const std::string& vid,
                       const common::dataStructures::Tape::State& state,
                       const std::optional<common::dataStructures::Tape::State>& prev_state,
                       const std::optional<std::string>& stateReason) override;

  bool tapeExists(const std::string& vid) const override;

  void setTapeFull(const common::dataStructures::SecurityIdentity& admin,
                   const std::string& vid,
                   const bool fullValue) override;

  void setTapeDirty(const common::dataStructures::SecurityIdentity& admin,
                    const std::string& vid,
                    const bool dirtyValue) override;

  void setTapeIsFromCastorInUnitTests(const std::string& vid) override;

  void setTapeDirty(const std::string& vid) override;

  void modifyTapeComment(const common::dataStructures::SecurityIdentity& admin,
                         const std::string& vid,
                         const std::optional<std::string>& comment) override;

  void tapeMountedForArchive(const std::string& vid, const std::string& drive) override;

  void tapeMountedForRetrieve(const std::string& vid, const std::string& drive) override;

  void noSpaceLeftOnTape(const std::string& vid) override;

  std::vector<TapeForWriting> getTapesForWriting(const std::string& logicalLibraryName) const override;

  common::dataStructures::Label::Format getTapeLabelFormat(const std::string& vid) const override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class TapeCatalogueRetryWrapper

}  // namespace cta::catalogue
