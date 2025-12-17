/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SessionState.hpp"

#include <sstream>

namespace cta::tape::session {

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

}  // namespace cta::tape::session
