/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds the information of a verify operation on a given tape 
 */
struct VerifyInfo {

  VerifyInfo();

  bool operator==(const VerifyInfo &rhs) const;

  bool operator!=(const VerifyInfo &rhs) const;

  std::string vid;
  std::string tag;
  uint64_t totalFiles;
  uint64_t totalSize;
  uint64_t filesToVerify;
  uint64_t filesFailed;
  uint64_t filesVerified;
  std::string verifyStatus;
  cta::common::dataStructures::EntryLog creationLog;
  std::map<uint64_t,std::string> errors;

}; // struct VerifyInfo

} // namespace dataStructures
} // namespace common
} // namespace cta

std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::VerifyInfo &obj);
