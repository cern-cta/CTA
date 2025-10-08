/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
 * The logical library is an attribute of both drives and tapes, it declares
 * which tapes can be mounted in which drives
 */
struct LogicalLibrary {

  /**
   * Constructor.
   *
   * Initialises isDisabled to false.
   */
  LogicalLibrary();

  bool operator==(const LogicalLibrary &rhs) const;

  bool operator!=(const LogicalLibrary &rhs) const;

  std::string name;
  bool isDisabled;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;
  std::optional<std::string> disabledReason;
  std::optional<std::string> physicalLibraryName;

}; // struct LogicalLibrary

std::ostream &operator<<(std::ostream &os, const LogicalLibrary &obj);

} // namespace cta::common::dataStructures
