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
 * This is the result of a read test operation
 */
struct ReadTestResult {
  ReadTestResult();

  bool operator==(const ReadTestResult& rhs) const;

  bool operator!=(const ReadTestResult& rhs) const;

  std::string driveName;
  std::string vid;
  uint64_t noOfFilesRead = 0;
  std::map<uint64_t, std::string> errors;
  std::map<uint64_t, std::pair<std::string, std::string>> checksums;
  uint64_t totalBytesRead = 0;
  uint64_t totalFilesRead = 0;
  uint64_t totalTimeInSeconds = 0;

};  // struct ReadTestResult

std::ostream& operator<<(std::ostream& os, const ReadTestResult& obj);

}  // namespace cta::common::dataStructures
