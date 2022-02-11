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

#include "mediachanger/LibrarySlot.hpp"

namespace cta {
namespace mediachanger {

/**
 * Class representing a dummy slot for the tests.
 */
class DummyLibrarySlot: public LibrarySlot {
public:

  /**
   * Constructor.
   *
   * Sets all string member-variables to the empty string.
   */
  DummyLibrarySlot();

  /**
   * Constructor.
   *
   * Initialises the member variables based on the result of parsing the
   * specified string representation of the library slot.
   *
   * @param str The string representation of the library slot.
   */
  DummyLibrarySlot(const std::string &str);

  /**
   * Destructor.
   */
  ~DummyLibrarySlot();

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  LibrarySlot *clone();

}; // class DummyLibrarySlot

} // namespace mediachanger
} // namespace cta
