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

#include <memory>

#include "catalogue/interfaces/MediaTypeCatalogue.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}

namespace catalogue {

class RdbmsCatalogue;

class RdbmsMediaTypeCatalogue : public MediaTypeCatalogue {
public:
  ~RdbmsMediaTypeCatalogue() override = default;

  void createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) override;

  void deleteMediaType(const std::string &name) override;

  std::list<MediaTypeWithLogs> getMediaTypes() const override;

  MediaType getMediaTypeByVid(const std::string & vid) const override;

  void modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName,
    const std::string &newName) override;

  void modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &cartridge) override;

  void modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const uint64_t capacityInBytes) override;

  void modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
   const uint8_t primaryDensityCode) override;

  void modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint8_t secondaryDensityCode) override;

  void modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::optional<std::uint32_t> &nbWraps) override;

  void modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::optional<std::uint64_t> &minLPos) override;

  void modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::optional<std::uint64_t> &maxLPos) override;

  void modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &comment) override;

protected:
  RdbmsMediaTypeCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue);

  virtual uint64_t getNextMediaTypeId(rdbms::Conn &conn) const = 0;
private:
  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

  bool mediaTypeIsUsedByTapes(rdbms::Conn &conn, const std::string &name) const;

  friend class RdbmsTapeCatalogue;
  std::optional<uint64_t> getMediaTypeId(rdbms::Conn &conn, const std::string &name) const;
};

} // namespace catalogue
} // namespace cta