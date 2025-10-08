/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "TapeItemWritten.hpp"
#include "TapeFileWritten.hpp"

namespace cta::catalogue {

  class InsertFileRecycleLog {
  public:
    std::string vid;
    uint64_t fSeq;
    uint64_t blockId;
    uint8_t copyNb;
    time_t tapeFileCreationTime;
    uint64_t archiveFileId;
    std::optional<std::string> diskFilePath;
    std::string reasonLog;
    time_t recycleLogTime;

    static std::string getRepackReasonLog(){
      return "REPACK";
    }

    static std::string getDeletionReasonLog(const std::string & deleterName, const std::string & diskInstanceName){
      return "File deleted by " + deleterName + " from the " + diskInstanceName + " instance";
    }
  };

} // namespace cta::catalogue

