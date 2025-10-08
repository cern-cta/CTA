/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/PositioningMethod.hpp"

//------------------------------------------------------------------------------
// positioningMethodtoString(PositioningMethod)
//------------------------------------------------------------------------------
const char *cta::positioningMethodToStr(const PositioningMethod enumValue) noexcept {
  switch(enumValue) {
    case PositioningMethod::ByBlock:
      return "ByBlock";
    case PositioningMethod::ByFSeq:
      return "ByFSeq";
    default:
      return "Unknown Positioning Method";
  }
}
