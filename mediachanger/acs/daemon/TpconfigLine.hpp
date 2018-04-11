/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>
#include "mediachanger/LibrarySlot.hpp"
#include <memory>

namespace cta {
namespace mediachanger {
namespace acs {
namespace daemon {

/**
 * The data stored in a data-line (as opposed to a comment-line) from a
 * TPCONFIG file (/etc/cta/TPCONFIG).
 */
class TpconfigLine {
public:
  /**
   * The unit name of the tape drive.
   */
  std::string unitName;

  /**
   * The logical library of the tape drive.
   */
  std::string logicalLibrary;

  /**
   * The filename of the device file of the tape drive.
   */
  std::string devFilename;

  /**
   * The slot in the tape library that contains the tape drive (string encoded).
   */
  std::string rawLibrarySlot;
  
  /**
   * Accessor method to the library slot strcuture.
   * @return reference to the library slot.
   */
  const cta::mediachanger::LibrarySlot & librarySlot() const;
  
private:
  /**
   * The library slot structure.
   */
  std::unique_ptr <cta::mediachanger::LibrarySlot> m_librarySlot;

public:
  /**
   * Trivial constructor (used in unit tests).
   */
  TpconfigLine();
  
  /**
   * Constructor.
   *
   * @param unitName The unit name of the tape drive.
   * @param dgn The Device Group Name (DGN) of the tape drive.
   * @param devFilename The filename of the device file of the tape drive.
   * @param librarySlot The slot in the tape library that contains the tape
   * drive.
   */
  TpconfigLine(
    const std::string &unitName,
    const std::string &logicalLibrary,
    const std::string &devFilename,
    const std::string &librarySlot);

  /**
   * Copy constructor
   * @param o the other TpConfigLine to be copied.
   */
  TpconfigLine(const TpconfigLine& o);
  
  /**
   * Copy operator
   * @param o the other TpConfigLine to copy.
   * @return a reference to the object
   */
  TpconfigLine& operator=(const TpconfigLine& o);
  static const size_t maxNameLen = 100;
}; // struct TpconfigLine

}}}} // namespace cta::mediachanger::acs::daemon
