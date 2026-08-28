/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionState.hpp"

#include <sstream>

namespace cta::tape::session {

std::string toString(TapeSessionState state) {
  switch (state) {
    case TapeSessionState::Preparing:
      return "Preparing";
    case TapeSessionState::Mounting:
      return "Mounting";
    case TapeSessionState::Loading:
      return "Loading";
    case TapeSessionState::Transferring:
      return "Transferring";
    case TapeSessionState::Finalizing:
      return "Finalizing";
    case TapeSessionState::Unloading:
      return "Unloading";
    case TapeSessionState::Unmounting:
      return "Unmounting";
    case TapeSessionState::DrainingToDisk:
      return "DrainingToDisk";
    case TapeSessionState::Finished:
      return "Finished";
    default: {
      std::stringstream st;
      st << "UnknownState (" << ((uint32_t) state) << ")";
      return st.str();
    }
  }
}

}  // namespace cta::tape::session
