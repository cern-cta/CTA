/**
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "OperationModes.hpp"

#include <stdexcept>

namespace cta::frontend {

std::string toString(OperationMode mode) {
  using enum OperationMode;
  switch (mode) {
    case WFE:
      return "wfe";
    case ADMIN_ALL:
      return "admin_all";
    case ADMIN_REPACK:
      return "admin_repack";
    case ADMIN_NO_REPACK:
      return "admin_no_repack";
  }
  throw std::invalid_argument("Invalid AppRunMode value");
}

AdminCmdMode toAdminCmdMode(OperationMode mode) {
  using enum OperationMode;
  switch (mode) {
    case ADMIN_ALL:
      return AdminCmdMode::ALL;
    case ADMIN_REPACK:
      return AdminCmdMode::REPACK;
    case ADMIN_NO_REPACK:
      return AdminCmdMode::NO_REPACK;
    case WFE:
      throw std::invalid_argument("OperationMode::WFE can't be converted to an AdminCmdMode");
  }
  throw std::invalid_argument("Invalid OperationMode");
}

}  // namespace cta::frontend
