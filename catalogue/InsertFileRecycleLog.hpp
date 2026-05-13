/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeFileWritten.hpp"
#include "TapeItemWritten.hpp"

#include <cstdint>
#include <string>

namespace cta::catalogue {

class InsertFileRecycleLog {
public:
  std::string vid;
  uint64_t fSeq = 0;
  uint64_t blockId = 0;
  uint8_t copyNb = 0;
  time_t tapeFileCreationTime = 0;
  uint64_t archiveFileId = 0;
  std::optional<std::string> diskFilePath;
  std::string reasonLog;
  time_t recycleLogTime = 0;

  static std::string getRepackReasonLog() { return "REPACK"; }

  static std::string getDeletionReasonLog(const std::string& deleterName, const std::string& diskInstanceName) {
    return "File deleted by " + deleterName + " from the " + diskInstanceName + " instance";
  }
};

}  // namespace cta::catalogue
