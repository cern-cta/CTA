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

#include "catalogue/interfaces/MediaTypeCatalogue.hpp"

namespace cta {
namespace catalogue {

class DummyMediaTypeCatalogue : public MediaTypeCatalogue {
public:
  DummyMediaTypeCatalogue() = default;
  ~DummyMediaTypeCatalogue() override = default;

  void createMediaType(const common::dataStructures::SecurityIdentity& admin, const MediaType& mediaType) override;

  void deleteMediaType(const std::string& name) override;

  std::list<MediaTypeWithLogs> getMediaTypes() const override;

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
};

}  // namespace catalogue
}  // namespace cta