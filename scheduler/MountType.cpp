/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/MountType.hpp"

//------------------------------------------------------------------------------
// toString
//------------------------------------------------------------------------------
const char* cta::MountTypeToDecommission::toString(const MountTypeToDecommission::Enum enumValue) noexcept {
  switch (enumValue) {
    case Enum::NONE:
      return "NONE";
    case Enum::ARCHIVE:
      return "ARCHIVE";
    case Enum::RETRIEVE:
      return "RETRIEVE";
    default:
      return "UNKNOWN";
  }
}
