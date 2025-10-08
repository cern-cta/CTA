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
 * This is the result of a write test operation
 */
struct WriteTestResult {

  WriteTestResult();

  bool operator==(const WriteTestResult &rhs) const;

  bool operator!=(const WriteTestResult &rhs) const;

  std::string driveName;
  std::string vid;
  uint64_t noOfFilesWritten;
  std::map<uint64_t,std::string> errors;
  std::map<uint64_t,std::pair<std::string,std::string>> checksums;
  uint64_t totalBytesWritten;
  uint64_t totalFilesWritten;
  uint64_t totalTimeInSeconds;

}; // struct WriteTestResult

std::ostream &operator<<(std::ostream &os, const WriteTestResult &obj);

} // namespace cta::common::dataStructures
