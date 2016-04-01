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
#include "common/dataStructures/TapeLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

struct Tape {

  Tape();

  bool operator==(const Tape &rhs) const;

  bool operator!=(const Tape &rhs) const;

  bool busy;
  uint64_t capacityInBytes;
  std::string comment;
  cta::common::dataStructures::EntryLog creationLog;
  uint64_t dataOnTapeInBytes;
  bool disabled;
  std::string encryptionKey;
  bool full;
  cta::common::dataStructures::TapeLog labelLog;
  uint64_t lastFSeq;
  cta::common::dataStructures::EntryLog lastModificationLog;
  cta::common::dataStructures::TapeLog lastReadLog;
  cta::common::dataStructures::TapeLog lastWriteLog;
  bool lbp;
  std::string logicalLibraryName;
  std::string tapePoolName;
  std::string vid;

}; // struct Tape

} // namespace dataStructures
} // namespace common
} // namespace cta

std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::Tape &obj);
