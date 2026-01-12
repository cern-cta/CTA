/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/MediaTypeCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}  // namespace rdbms

namespace catalogue {

class RdbmsCatalogue;

class RdbmsMediaTypeCatalogue : public MediaTypeCatalogue {
public:
  ~RdbmsMediaTypeCatalogue() override = default;

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

protected:
  RdbmsMediaTypeCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue);

  virtual uint64_t getNextMediaTypeId(rdbms::Conn& conn) const = 0;

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

  bool mediaTypeIsUsedByTapes(rdbms::Conn& conn, const std::string& name) const;

  friend class RdbmsTapeCatalogue;
  std::optional<uint64_t> getMediaTypeId(rdbms::Conn& conn, const std::string& name) const;
};

}  // namespace catalogue
}  // namespace cta
