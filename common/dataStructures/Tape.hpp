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

#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/TapeLog.hpp"
#include "common/optional.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds all tape metadata information 
 */
struct Tape {

  Tape();

  bool operator==(const Tape &rhs) const;

  bool operator!=(const Tape &rhs) const;

  std::string vid;
  std::string mediaType;
  std::string vendor;
  uint64_t lastFSeq;
  std::string logicalLibraryName;
  std::string tapePoolName;
  std::string vo;
  uint64_t capacityInBytes;
  uint64_t dataOnTapeInBytes;

  /**
   * The optional identifier of the encrption key.
   *
   * This optional should either have a non-empty string value or no value at
   * all.  Empty strings are prohibited.
   */
  optional<std::string> encryptionKey;

  /**
   * Specifies whether or not the tape has Logical Block Protection (LBP)
   * enabled.  This value is not set until the tape is either labelled or
   * imported as a tape containing pre-existing files.
   */
  optional<bool> lbp;

  bool full;
  bool disabled;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;
  optional<TapeLog> labelLog;
  optional<TapeLog> lastWriteLog;
  optional<TapeLog> lastReadLog;

}; // struct Tape

std::ostream &operator<<(std::ostream &os, const Tape &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
