/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TransferState.hpp"

#include <sstream>

namespace cta::tape::session {

std::string toString(TransferState state) {
  switch (state) {
    case TransferState::Preparing:
      return "Preparing";
    case TransferState::Transferring:
      return "Transferring";
    case TransferState::Finalizing:
      return "Finalizing";
    case TransferState::DrainingToDisk:
      return "DrainingToDisk";
    case TransferState::Finished:
      return "Finished";
    default: {
      std::stringstream st;
      st << "UnknownState (" << ((uint32_t) state) << ")";
      return st.str();
    }
  }
}

}  // namespace cta::tape::session
