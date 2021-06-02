/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>

#include "TapeItemWritten.hpp"
#include "TapeFileWritten.hpp"

namespace cta { namespace catalogue {

  class InsertFileRecycleLog {
  public:
    std::string vid;
    uint64_t fSeq;
    uint64_t blockId;
    uint8_t copyNb;
    time_t tapeFileCreationTime;
    uint64_t archiveFileId;
    cta::optional<std::string> diskFilePath;
    std::string reasonLog;
    time_t recycleLogTime;
    
    static std::string getRepackReasonLog(){
      return "REPACK";
    }
    
    static std::string getDeletionReasonLog(const std::string & deleterName, const std::string & diskInstanceName){
      return "File deleted by " + deleterName + " from the " + diskInstanceName + " instance";
    }
  };
  
}}

