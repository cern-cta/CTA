/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

namespace cta {
  /**
   * Enumeration class of the possible types of tape mount.
   */
  struct MountTypeToDecommission {
  public:

    /**
     * Enumeration of the possible types of tape mount.
     */
    enum Enum {
      NONE,
      ARCHIVE,
      RETRIEVE,
      LABEL};

    /**
     * Thread safe method that returns the string representation of the
     * specified enumeration value.
     *
     * @param enumValue The enumeration value.
     * @return The string representation.
     */
    static const char *toString(const MountTypeToDecommission::Enum enumValue) throw();

  }; // struct MountType
  
}
