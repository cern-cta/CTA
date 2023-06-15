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

#include "SessionState.hpp"
#include <sstream>

namespace cta {
namespace tape {
namespace session {

std::string toString(SessionState state) {
  switch (state) {
    case SessionState::PendingFork:
      return "PendingFork";
    case SessionState::StartingUp:
      return "StartingUp";
    case SessionState::Checking:
      return "Checking";
    case SessionState::Scheduling:
      return "Scheduling";
    case SessionState::Mounting:
      return "Mounting";
    case SessionState::Running:
      return "Running";
    case SessionState::Unmounting:
      return "Unmounting";
    case SessionState::DrainingToDisk:
      return "DrainingToDisk";
    case SessionState::ShuttingDown:
      return "ShuttingDown";
    case SessionState::Shutdown:
      return "Shutdown";
    case SessionState::Killed:
      return "Killed";
    case SessionState::Fatal:
      return "Fatal";
    default: {
      std::stringstream st;
      st << "UnknownState (" << ((uint32_t) state) << ")";
      return st.str();
    }
  }
}

}  // namespace session
}  // namespace tape
}  // namespace cta
