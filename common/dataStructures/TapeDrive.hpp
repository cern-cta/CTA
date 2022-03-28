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

#include <map>
#include <string>

#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/optional.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds status of a Tape Drive
 */
struct TapeDrive {
  static const std::map<DriveStatus, std::string> STATE_TO_STRING_MAP;
  static const std::map<std::string, DriveStatus> STRING_TO_STATE_MAP;

  static std::string getAllPossibleStates();

  TapeDrive();

  bool operator==(const TapeDrive &rhs) const;
  bool operator!=(const TapeDrive &rhs) const;

  void setState(const std::string & state);

  std::string getStateStr() const;

  /**
   * Return the string representation of the tape drive State passed in parameter
   * @param state the state to return the string representation
   * @return the string representation of the state passed in parameter
   * @throws cta::exception::Exception if the state passed in parameter does not exist
   */
  static std::string stateToString(const DriveStatus &state);

  /**
   * Return the state value according to the state passed in parameter (not case sensitive)
   * @param state the string that identifies the state enum value to return
   * @return the state corresponding to the State enum value
   * @throws cta::exception::Exception if the state passed in parameter does not match any existing State enum value
   */
  static DriveStatus stringToState(const std::string & state);

  std::string driveName;
  std::string host;
  std::string logicalLibrary;
  optional<uint64_t> sessionId;

  optional<uint64_t> bytesTransferedInSession;
  optional<uint64_t> filesTransferedInSession;

  optional<time_t> sessionStartTime;
  optional<time_t> sessionElapsedTime;
  optional<time_t> mountStartTime;
  optional<time_t> transferStartTime;
  optional<time_t> unloadStartTime;
  optional<time_t> unmountStartTime;
  optional<time_t> drainingStartTime;
  optional<time_t> downOrUpStartTime;
  optional<time_t> probeStartTime;
  optional<time_t> cleanupStartTime;
  optional<time_t> startStartTime;
  optional<time_t> shutdownTime;

  MountType mountType;
  DriveStatus driveStatus;
  bool desiredUp;
  bool desiredForceDown;
  optional<std::string> reasonUpDown;

  optional<std::string> currentVid;
  optional<std::string> ctaVersion;
  optional<uint64_t> currentPriority;
  optional<std::string> currentActivity;
  optional<std::string> currentTapePool;
  MountType nextMountType; // defaults to NO_MOUNT. This can't be optional, as we have a NOT NULL constraint in the DB.
  optional<std::string> nextVid;
  optional<std::string> nextTapePool;
  optional<uint64_t> nextPriority;
  optional<std::string> nextActivity;

  optional<std::string> devFileName;
  optional<std::string> rawLibrarySlot;

  optional<std::string> currentVo;
  optional<std::string> nextVo;

  optional<std::string> diskSystemName;
  optional<uint64_t> reservedBytes;
  optional<uint64_t> reservationSessionId;

  optional<std::string> userComment;
  optional<EntryLog> creationLog;
  optional<EntryLog> lastModificationLog;
};  // struct TapeDrive

std::ostream &operator<<(std::ostream &os, const TapeDrive &obj);

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
