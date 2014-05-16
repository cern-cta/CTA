/******************************************************************************
 *         castor/tape/utils/TpconfigLine.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include <string>

namespace castor {
namespace tape   {
namespace utils  {

/**
 * The data stored in a data-line (as opposed to a comment-line) from a
 * TPCONFIG file (/etc/castor/TPCONFIG).
 */
struct TpconfigLine {
  /**
   * Enumeration of the possible initial states of a tape drive.
   */
  enum InitialState {
    TPCONFIG_DRIVE_NONE,
    TPCONFIG_DRIVE_UP,
    TPCONFIG_DRIVE_DOWN};

  /**
   * Returns the string representation of the specified initial state of a
   * tape drive.
   *
   * This method is thread safe.
   *
   * This method does not throw an exception.  If the specified state is
   * unknown then an appropriately worded string is returned.
   *
   * @param state The initial state of a tape drive.
   * @return The string representation.
   */
  static const char *initialState2Str(const InitialState value) throw();

  /**
   * Returns the initial tape-drive state represented by the specified string.
   *
   * This method throws an exception if the specified string representation is
   * unknown.
   *
   * This method will not interpret a string a representing TPCONFIG_DRIVE_NONE.
   *
   * @param str The string representation of the initial tape-drive state.
   * @return The initial state of the tape drive.
   */
  static InitialState str2InitialState(std::string str);

  std::string unitName;
  std::string dgn;
  std::string devFilename;
  std::string density;
  InitialState initialState;
  std::string librarySlot;
  std::string devType;

  /**
   * Constructor.
   */
  TpconfigLine(
    const std::string &unitName,
    const std::string &dgn,
    const std::string &devFilename,
    const std::string &density,
    const std::string &initialState,
    const std::string &librarySlot,
    const std::string &devType) throw();
}; // struct TpconfigLine

} // namespace utils
} // namespace tape
} // namespace castor

