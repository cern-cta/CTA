/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/MediaTypeCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class MediaTypeCatalogueRetryWrapper : public MediaTypeCatalogue {
public:
  MediaTypeCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~MediaTypeCatalogueRetryWrapper() override = default;

  void createMediaType(const common::dataStructures::SecurityIdentity& admin, const MediaType& mediaType) override;

  void deleteMediaType(const std::string& name) override;

  std::vector<MediaTypeWithLogs> getMediaTypes() const override;

  MediaType getMediaTypeByVid(const std::string& vid) const override;

  void modifyMediaTypeName(const common::dataStructures::SecurityIdentity& admin,
                           const std::string& currentName,
                           const std::string& newName) override;

  void modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity& admin,
                                const std::string& name,
                                const std::string& cartridge) override;

  void modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity& admin,
                                      const std::string& name,
                                      const uint64_t capacityInBytes) override;

  void modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity& admin,
                                         const std::string& name,
                                         const uint8_t primaryDensityCode) override;

  void modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity& admin,
                                           const std::string& name,
                                           const uint8_t secondaryDensityCode) override;

  void modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& name,
                              const std::optional<std::uint32_t>& nbWraps) override;

  void modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& name,
                              const std::optional<std::uint64_t>& minLPos) override;

  void modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& name,
                              const std::optional<std::uint64_t>& maxLPos) override;

  void modifyMediaTypeComment(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& name,
                              const std::string& comment) override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class MediaTypeCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
