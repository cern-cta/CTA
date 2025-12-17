/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::common::dataStructures {

enum class MountType : uint32_t {
  ArchiveForUser = 1,
  ArchiveForRepack = 2,
  Retrieve = 3,
  Label = 4,
  NoMount = 0,
  /// A summary type used in scheduling.
  ArchiveAllTypes = 99
};

/**
 * A function summarizing subtypes (currently only Archive) to simplify scheduling.
 */
MountType getMountBasicType(MountType type);

/**
 * Convert enum to string for storage in DB and logging
 */
std::string toString(MountType type);

/**
 * Convert enum to string in camel case format to show in cta-admnin
 */
std::string toCamelCaseString(cta::common::dataStructures::MountType type);

/**
 * Convert string to enum. Needed to get values from DB.
 */
MountType strToMountType(const std::string& mountTypeStr);

std::ostream& operator<<(std::ostream& os, const MountType& obj);

}  // namespace cta::common::dataStructures
