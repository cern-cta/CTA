/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>

namespace cta {

/**
 * Class representing the criteria be met in order to justify mounting a tape.
 */
class MountCriteria {
public:

  /**
   * Constructor.
   */
  MountCriteria();

  /**
   * Constructor.
   *
   * @param nbBytes The minimum number of queued bytes required to justify a
   * mount.
   * @param nbFiles The minimum number of queued files required to justify a
   * mount.
   * @param ageInSecs The minimum age in seconds of queued data required to
   * justify a mount.
   */
  MountCriteria(const uint64_t nbBytes, const uint64_t nbFiles,
    const uint64_t ageInSecs);

  /**
   * Returns the minimum number of queued bytes required to justify a mount.
   *
   * @return The minimum number of queued bytes required to justify a mount.
   */
  uint64_t getNbBytes() const noexcept;

  /**
   * Returns the minimum number of queued files required to justify a mount.
   *
   * @return The minimum number of queued files required to justify a mount.
   */
  uint64_t getNbFiles() const noexcept;

  /**
   * Returns the minimum age in seconds of queued data required to justify a
   * mount.
   *
   * @return The minimum age in seconds of queued data required to justify a
   * mount.
   */
  uint64_t getAgeInSecs() const noexcept;

private:

  /**
   * The minimum number of queued bytes required to justify a mount.
   */
  uint64_t m_nbBytes;

  /**
   * The minimum number of queued files required to justify a mount.
   */
  uint64_t m_nbFiles;

  /**
   * The minimum age in seconds of queued data required to justify a mount.
   */
  uint64_t m_ageInSecs;

}; // class MountCriteria

} // namespace cta
