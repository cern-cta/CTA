/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <stdint.h>

namespace cta {

  /**
   * Positioning methods
   */
  enum class PositioningMethod: uint8_t {
    ByBlock = 0,
    ByFSeq = 1
  };

  /**
   * Thread safe method that returns the string representation of the
   * specified enumeration vale.
   */
  const char *positioningMethodToStr(const PositioningMethod enumValue) noexcept;

} // namespace cta
