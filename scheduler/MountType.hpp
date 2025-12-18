/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta {
/**
   * Enumeration class of the possible types of tape mount.
   */
struct MountTypeToDecommission {
  /**
     * Enumeration of the possible types of tape mount.
     */
  enum Enum { NONE, ARCHIVE, RETRIEVE, LABEL };

  /**
     * Thread safe method that returns the string representation of the
     * specified enumeration value.
     *
     * @param enumValue The enumeration value.
     * @return The string representation.
     */
  static const char* toString(const MountTypeToDecommission::Enum enumValue) noexcept;
};
}  // namespace cta
