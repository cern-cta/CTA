/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/EntryLog.hpp"

#include <list>
#include <map>
#include <optional>
#include <stdint.h>
#include <string>

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
  bool isDisabled;
  std::optional<std::string> disabledReason;
};  // struct PhysicalLibrary

struct UpdatePhysicalLibrary {
  std::string name;
  std::optional<std::string> guiUrl;
  std::optional<std::string> webcamUrl;
  std::optional<std::string> location;
  std::optional<uint64_t> nbPhysicalCartridgeSlots;
  std::optional<uint64_t> nbAvailableCartridgeSlots;
  std::optional<uint64_t> nbPhysicalDriveSlots;
  std::optional<std::string> comment;
  std::optional<bool> isDisabled;
  std::optional<std::string> disabledReason;
};  // struct PhysicalLibrary

}  // namespace cta::common::dataStructures
