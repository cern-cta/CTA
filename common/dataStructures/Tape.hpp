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

#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/TapeLog.hpp"
#include "common/dataStructures/LabelFormat.hpp"

#include <list>
#include <map>
#include <set>
#include <optional>
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
    DISABLED = 3,
    REPACKING = 4,
    EXPORTED = 5,
    REPACKING_DISABLED = 6,
    BROKEN_PENDING = 101,
    REPACKING_PENDING = 102,
    EXPORTED_PENDING = 103,
  };

  static const std::map<State,std::string> STATE_TO_STRING_MAP;
  static const std::map<std::string,State> STRING_TO_STATE_MAP;
  static const std::set<State> PENDING_STATES_SET;

  static std::string getAllPossibleStates(bool hidePendingStates = false);

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
  static State stringToState(const std::string & state, bool hidePendingStates = false);

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

  cta::common::dataStructures::Label::Format  labelFormat;

  /**
   * The optional name of the encryption key.
   *
   * This optional should either have a non-empty string value or no value at
   * all.  Empty strings are prohibited.
   */
  std::optional<std::string> encryptionKeyName;

  bool full;
  bool disabled;
  bool isFromCastor;
  bool dirty;
  uint64_t readMountCount;
  uint64_t writeMountCount;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;
  std::optional<TapeLog> labelLog;
  std::optional<TapeLog> lastWriteLog;
  std::optional<TapeLog> lastReadLog;

  State state;
  std::optional<std::string> stateReason;
  std::string stateModifiedBy;
  time_t stateUpdateTime;
  std::optional<std::string> verificationStatus;

  bool isActive() const;
  bool isDisabled() const;
  bool isRepacking() const;
  bool isBroken() const;
  bool isExported() const;
}; // struct Tape

std::ostream &operator<<(std::ostream &os, const Tape &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
