/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/ArchiveRouteType.hpp"
#include "common/dataStructures/EntryLog.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * The archive route specifies which tape pool will be used as a destination of
 * a specific copy of a storage class
 */
struct ArchiveRoute {
  ArchiveRoute();

  bool operator==(const ArchiveRoute& rhs) const;

  bool operator!=(const ArchiveRoute& rhs) const;

  /**
   * The name of the storage class which is only guranateed to be unique within
   * its disk instance.
   */
  std::string storageClassName;

  /**
   * The copy number of the tape file.
   */
  uint8_t copyNb = 0;

  /**
   * The type of the archive route.
   */
  ArchiveRouteType type = ArchiveRouteType::DEFAULT;

  std::string tapePoolName;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

  using StorageClassMap = std::map<uint32_t, ArchiveRoute>;
  using FullMap = std::map<std::string /*storage class*/, StorageClassMap>;

};  // struct ArchiveRoute

std::ostream& operator<<(std::ostream& os, const ArchiveRoute& obj);

}  // namespace cta::common::dataStructures
