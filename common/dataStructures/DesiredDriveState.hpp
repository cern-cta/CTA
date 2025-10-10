/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <ostream>

namespace cta::common::dataStructures {

/**
 * Structure describing the instructions to the drive from operators.
 * The values are reset to all false when the drive goes down (including
 * at startup).
 */
class DesiredDriveState {
public:
  DesiredDriveState() = default;

  bool operator==(const DesiredDriveState& rhs) const {
    return up == rhs.up && forceDown == rhs.forceDown;
  }
  void setReasonFromLogMsg(int logLevel, std::string_view msg);

  static constexpr const char* const c_tpsrvPrefixComment = "[cta-taped]";
  static std::string generateReasonFromLogMsg(int logLevel, std::string_view msg);

  bool up = false;                    //!< Should the drive be up?
  bool forceDown = false;             //!< Should going down preempt an existig mount?
  std::optional<std::string> reason;  //!< The reason why operators put the drive down or up
  std::optional<std::string> comment; //!< General informations about the drive given by the operators
};

std::ostream& operator<<(std::ostream& os, const DesiredDriveState& obj);

} // namespace cta::common::dataStructures
