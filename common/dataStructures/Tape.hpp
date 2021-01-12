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
#include <vector>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds all tape metadata information 
 */
struct Tape {

  enum State {
    ACTIVE = 1,
    BROKEN = 2,
    DISABLED = 3
  };
  
  static const std::map<State,std::string> STATE_TO_STRING_MAP;
  static const std::map<std::string,State> STRING_TO_STATE_MAP;
  
  static std::string getAllPossibleStates();
  
  Tape();

  bool operator==(const Tape &rhs) const;

  bool operator!=(const Tape &rhs) const;
  
  void setState(const std::string & state);
  
  std::string getStateStr() const;
  
  /**
   * Return the string representation of the tape State passed in parameter
   * @param state the state to return the string representation
   * @return the string representation of the state passed in parameter
   * @throws cta::exception::Exception if the state passed in parameter does not exist
   */
  static std::string stateToString(const State &state);
  
  /**
   * Return the state value according to the state passed in parameter (not case sensitive)
   * @param state the string that identifies the state enum value to return
   * @return the state corresponding to the State enum value
   * @throws cta::exception::Exception if the state passed in parameter does not match any existing State enum value
   */
  static State stringToState(const std::string & state);

  std::string vid;
  std::string mediaType;
  std::string vendor;
  uint64_t lastFSeq;
  std::string logicalLibraryName;
  std::string tapePoolName;
  std::string vo;
  uint64_t capacityInBytes;
  uint64_t dataOnTapeInBytes;
  uint64_t nbMasterFiles;
  uint64_t masterDataInBytes;

  /**
   * The optional name of the encryption key.
   *
   * This optional should either have a non-empty string value or no value at
   * all.  Empty strings are prohibited.
   */
  optional<std::string> encryptionKeyName;

  bool full;
  bool disabled;
  bool readOnly;
  bool isFromCastor;
  bool dirty;
  uint64_t readMountCount;
  uint64_t writeMountCount;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;
  optional<TapeLog> labelLog;
  optional<TapeLog> lastWriteLog;
  optional<TapeLog> lastReadLog;
  
  State state;
  optional<std::string> stateReason;
  std::string stateModifiedBy;
  time_t stateUpdateTime;
  
  bool isDisabled() const;
  
}; // struct Tape

std::ostream &operator<<(std::ostream &os, const Tape &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
