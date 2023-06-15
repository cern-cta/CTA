/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "mediachanger/LibrarySlot.hpp"

namespace cta {
namespace mediachanger {

/**
 * Class representing a dummy slot for the tests.
 */
class DummyLibrarySlot : public LibrarySlot {
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
  DummyLibrarySlot(const std::string& str);

  /**
   * Destructor.
   */
  ~DummyLibrarySlot();

  /**
   * Creates a clone of this object.
   *
   * @return The clone.
   */
  LibrarySlot* clone();

};  // class DummyLibrarySlot

}  // namespace mediachanger
}  // namespace cta
