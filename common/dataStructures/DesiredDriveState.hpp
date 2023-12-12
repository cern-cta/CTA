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
