/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <common/dataStructures/ArchiveRouteType.hpp>

#include "cta_admin.pb.h"

namespace cta::admin {

inline common::dataStructures::ArchiveRouteType
ProtobufToArchiveRouteTypeFormat(ArchiveRouteLsItem::ArchiveRouteType archiveRouteType) {
  using namespace common::dataStructures;

  switch (archiveRouteType) {
    case ArchiveRouteLsItem::DEFAULT:
      return ArchiveRouteType::DEFAULT;
    case ArchiveRouteLsItem::REPACK:
      return ArchiveRouteType::REPACK;
    default:
      throw std::runtime_error("In ProtobufToArchiveRouteTypeFormat(): unknown archive route type "
                               + std::to_string(archiveRouteType));
  }
}

inline ArchiveRouteLsItem::ArchiveRouteType
ArchiveRouteTypeFormatToProtobuf(common::dataStructures::ArchiveRouteType archiveRouteType) {
  using namespace common::dataStructures;

  switch (archiveRouteType) {
    case ArchiveRouteType::DEFAULT:
      return ArchiveRouteLsItem::DEFAULT;
    case ArchiveRouteType::REPACK:
      return ArchiveRouteLsItem::REPACK;
    default:
      throw std::runtime_error("In ArchiveRouteTypeFormatToProtobuf(): unknown archive route type "
                               + std::to_string(archiveRouteType));
  }
}

}  // namespace cta::admin
