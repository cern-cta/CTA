/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::common::dataStructures {

enum ArchiveRouteType {
  DEFAULT = 1,
  REPACK = 2,
};

/**
 * Convert enum to string for storage in DB and logging
 */
std::string toString(ArchiveRouteType type);

/**
 * Convert string to enum. Needed to get values from DB.
 */
ArchiveRouteType strToArchiveRouteType(const std::string& archiveRouteTypeStr);

} // namespace cta::common::dataStructures
