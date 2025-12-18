/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>

namespace cta {

/**
 * Class representing a tape-drive quota.
 */
class DriveQuota {
public:
  /**
   * Constructor.
   */
  DriveQuota();

  /**
   * Constructor.
   *
   * @param minDrives The minimum number of drives that should be provided.
   * @param maxDrives The maximum number of drives that should be provided.
   */
  DriveQuota(const uint32_t minDrives, const uint32_t maxDrives);

  /**
   * Returns the minimum number of drives that should be provided.
   *
   * @return The minimum number of drives that should be provided.
   */
  uint32_t getMinDrives() const noexcept;

  /**
   * Returns the maximum number of drives that should be provided.
   *
   * @return The maximum number of drives that should be provided.
   */
  uint32_t getMaxDrives() const noexcept;

private:
  /**
   * The minimum number of drives that should be provided.
   */
  uint32_t m_minDrives;

  /**
   * The maximum number of drives that should be provided.
   */
  uint32_t m_maxDrives;

};  // class DriveQuota

}  // namespace cta
