/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/ArchiveRouteType.hpp"
#include "common/dataStructures/EntryLog.hpp"

namespace cta::common::dataStructures {

/**
 * The archive route specifies which tape pool will be used as a destination of
 * a specific copy of a storage class
 */
struct ArchiveRoute {

  ArchiveRoute();

  bool operator==(const ArchiveRoute &rhs) const;

  bool operator!=(const ArchiveRoute &rhs) const;

  /**
   * The name of the storage class which is only guranateed to be unique within
   * its disk instance.
   */
  std::string storageClassName;

  /**
   * The copy number of the tape file.
   */
  uint8_t copyNb;

  /**
   * The type of the archive route.
   */
  ArchiveRouteType type;

  std::string tapePoolName;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

  typedef std::map<uint32_t, ArchiveRoute> StorageClassMap;
  typedef std::map<std::string /*storage class*/, StorageClassMap> FullMap;

}; // struct ArchiveRoute

std::ostream &operator<<(std::ostream &os, const ArchiveRoute &obj);

} // namespace cta::common::dataStructures
