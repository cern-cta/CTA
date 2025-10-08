/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SessionType.hpp"
#include <sstream>

namespace cta::tape::session {

std::string toString(SessionType type) {
  switch(type) {
  case SessionType::Archive:
    return "Archive";
  case SessionType::Retrieve:
    return "Retrieve";
  case SessionType::Label:
    return "Label";
  case SessionType::Undetermined:
    return "Undetermined";
  case SessionType::Cleanup:
    return "Cleanup";
  default:
    {
      std::stringstream st;
      st << "UnknownType (" << ((uint32_t) type) << ")";
      return st.str();
    }
  }
}

} // namespace cta::tape::session

