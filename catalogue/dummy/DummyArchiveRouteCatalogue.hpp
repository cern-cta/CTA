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

#include "catalogue/interfaces/ArchiveRouteCatalogue.hpp"

namespace cta::catalogue {

class DummyArchiveRouteCatalogue : public ArchiveRouteCatalogue {
public:
  DummyArchiveRouteCatalogue() = default;
  ~DummyArchiveRouteCatalogue() override = default;

  void createArchiveRoute(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName,
    const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType, const std::string &tapePoolName, const std::string &comment) override;

  void deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType) override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName,
    const std::string &tapePoolName) const override;

  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType, const std::string &tapePoolName) override;

  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &comment) override;
};

} // namespace cta::catalogue
