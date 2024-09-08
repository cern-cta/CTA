/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <common/dataStructures/ArchiveRouteType.hpp>
#include "cta_admin.pb.h"

namespace cta::admin {

ArchiveRouteType ProtobufToArchiveRouteTypeFormat(ArchiveRouteLsItem::ArchiveRouteType archiveRouteType) {

  using namespace common::dataStructures;

  switch (archiveRouteType) {
    case ArchiveRouteLsItem::DEFAULT: return ArchiveRouteType::DEFAULT;
    case ArchiveRouteLsItem::REPACK:  return ArchiveRouteType::REPACK;
    default:
      throw std::runtime_error("In ProtobufToArchiveRouteTypeFormat(): unknown archive route type " + std::to_string(archiveRouteType));
  }
}

ArchiveRouteLsItem::ArchiveRouteType ArchiveRouteTypeFormatToProtobuf(ArchiveRouteType archiveRouteType) {
  using namespace common::dataStructures;

  switch (archiveRouteType) {
    case ArchiveRouteType::DEFAULT: return ArchiveRouteLsItem::DEFAULT;
    case ArchiveRouteType::REPACK:  return ArchiveRouteLsItem::REPACK;
    default:
      throw std::runtime_error("In ArchiveRouteTypeFormatToProtobuf(): unknown archive route type " + std::to_string(archiveRouteType));
  }
}

}  // namespace cta::admin
