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

#include <string>

#include "TapeItemWritten.hpp"
#include "TapeFileWritten.hpp"

namespace cta {
namespace catalogue {

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

  static std::string getRepackReasonLog() { return "REPACK"; }

  static std::string getDeletionReasonLog(const std::string& deleterName, const std::string& diskInstanceName) {
    return "File deleted by " + deleterName + " from the " + diskInstanceName + " instance";
  }
};

}  // namespace catalogue
}  // namespace cta
