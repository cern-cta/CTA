/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>

namespace cta {

/**
 * A request to mount a tape in a tape drive in order to start a data transfer
 * session.
 */
struct MountRequest {

  /**
   * Enumeration of the possible transfer types for a tape mount, either all
   * archives or all retrieves.
   */
  enum TapeJobType {
    TAPEJOBTYPE_NONE,
    TAPEJOBTYPE_ARCHIVE,
    TAPEJOBTYPE_RETRIEVE
  };

  /**
   * Thread safe method that returns the string representation of the specified
   * enumeration value.
   *
   * @param enumValue The integer value of the type.
   * @return The string representation.
   */
  static const char *TapeJobTypeToStr(const TapeJobType enumValue) throw();

  /**
   * Constructor.
   */
  MountRequest();

  /**
   * Constructor.
   *
   * @param mountId The identifier of the mount.
   * @param vid The volume identifier of the tape to be mounted.
   * @param transferType The type of transfers to be carried out, either
   * all archives or all retrieves.
   */
  MountRequest(
    const std::string &mountId,
    const std::string &vid,
    const TapeJobType transferType);

  /**
   * The identifier of the mount.
   */
  std::string mountId;

  /**
   * The volume identifier of the tape to be mounted.
   */
  std::string vid;

  /**
   * The type of transfers to be carried out, either archives or retrieves.
   */
  TapeJobType transferType;

}; // class MountRequest

} // namespace cta
