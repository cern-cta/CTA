/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct is used to hold stats of a list of files (when listing them)
 */
struct ArchiveFileSummary {
  ArchiveFileSummary();

  bool operator==(const ArchiveFileSummary& rhs) const;

  bool operator!=(const ArchiveFileSummary& rhs) const;

  uint64_t totalBytes = 0;
  uint64_t totalFiles = 0;

};  // struct ArchiveFileSummary

std::ostream& operator<<(std::ostream& os, const ArchiveFileSummary& obj);

}  // namespace cta::common::dataStructures
