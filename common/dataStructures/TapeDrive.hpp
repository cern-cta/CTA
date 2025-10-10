/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <map>
#include <optional>
#include <string>

#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/log/LogContext.hpp"

namespace cta::common::dataStructures {

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

  /**
   * Fills a ScopedParamContainer with all the DriveStatus field values, including the optional ones.
   * @param params the ScopedParamContainer, passed by reference
   * @param prefix an optional string to be used as a prefix for the log parameters
   */
  void convertToLogParams(log::ScopedParamContainer& params, const std::string& prefix = "") const;

  std::string driveName;
  std::string host;
  std::string logicalLibrary;
  std::optional<bool> logicalLibraryDisabled;
  std::optional<uint64_t> sessionId;

  std::optional<uint64_t> bytesTransferedInSession;
  std::optional<uint64_t> filesTransferedInSession;

  std::optional<time_t> sessionStartTime;
  std::optional<time_t> sessionElapsedTime;
  std::optional<time_t> mountStartTime;
  std::optional<time_t> transferStartTime;
  std::optional<time_t> unloadStartTime;
  std::optional<time_t> unmountStartTime;
  std::optional<time_t> drainingStartTime;
  std::optional<time_t> downOrUpStartTime;
  std::optional<time_t> probeStartTime;
  std::optional<time_t> cleanupStartTime;
  std::optional<time_t> startStartTime;
  std::optional<time_t> shutdownTime;

  MountType mountType;
  DriveStatus driveStatus;
  bool desiredUp;
  bool desiredForceDown;
  std::optional<std::string> reasonUpDown;

  std::optional<std::string> currentVid;
  std::optional<std::string> ctaVersion;
  std::optional<uint64_t> currentPriority;
  std::optional<std::string> currentActivity;
  std::optional<std::string> currentTapePool;
  MountType nextMountType; // defaults to NO_MOUNT. This can't be optional, as we have a NOT nullptr constraint in the DB.
  std::optional<std::string> nextVid;
  std::optional<std::string> nextTapePool;
  std::optional<uint64_t> nextPriority;
  std::optional<std::string> nextActivity;

  std::optional<std::string> devFileName;
  std::optional<std::string> rawLibrarySlot;

  std::optional<std::string> currentVo;
  std::optional<std::string> nextVo;

  std::optional<std::string> diskSystemName;
  std::optional<uint64_t> reservedBytes;
  std::optional<uint64_t> reservationSessionId;

  std::optional<std::string> physicalLibraryName;

  std::optional<std::string> userComment;
  std::optional<EntryLog> creationLog;
  std::optional<EntryLog> lastModificationLog;
  std::optional<bool> physicalLibraryDisabled;
};  // struct TapeDrive

} // namespace cta::common::dataStructures
