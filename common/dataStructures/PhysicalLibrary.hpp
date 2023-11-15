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

#include <list>
#include <map>
#include <optional>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta::common::dataStructures {

/**
 * The attributes of a physical library
 */
struct PhysicalLibrary {
  std::string name;
  std::string manufacturer;
  std::string model;
  std::optional<std::string> type;
  std::optional<std::string> guiUrl;
  std::optional<std::string> webcamUrl;
  std::optional<std::string> location;
  uint64_t nbPhysicalCartridgeSlots;
  std::optional<uint64_t> nbAvailableCartridgeSlots;
  uint64_t nbPhysicalDriveSlots;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::optional<std::string> comment;

  PhysicalLibrary() = default;
}; // struct PhysicalLibrary

struct UpdatePhysicalLibrary {
  std::string name;
  std::optional<std::string> guiUrl;
  std::optional<std::string> webcamUrl;
  std::optional<std::string> location;
  std::optional<uint64_t> nbPhysicalCartridgeSlots;
  std::optional<uint64_t> nbAvailableCartridgeSlots;
  std::optional<uint64_t> nbPhysicalDriveSlots;
  std::optional<std::string> comment;
}; // struct PhysicalLibrary

} // namespace cta::common::dataStructures
