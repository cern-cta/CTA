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

#include "common/exception/NotAnOwner.hpp"

#include <stdio.h>


namespace cta {

/**
 * A smart pointer that owns a FILE pointer.  When the smart pointer goes out
 * of scope, it will fclose the FILE pointer it owns.
 */
class SmartFILEPtr {

public:

  /**
   * Constructor
   */
  SmartFILEPtr() noexcept;

  /**
   * Constructor
   *
   * @param file The FILE pointer to be owned by the smart pointer.
   */
  explicit SmartFILEPtr(FILE* const file) noexcept;

  /**
   * Take ownership of the specified FILE pointer, closing the previously
   * owned FILE pointer if there is one and it is not the same as the one
   * specified.
   *
   * @param file The FILE pointer to be owned, defaults to nullptr if not
   *             specified, where nullptr means this smart pointer will not own a
   *             pointer after the reset() method returns.
   */
  void reset(FILE *const file = nullptr) noexcept;

  /**
   * SmartFILEPtr assignment operator.
   *
   * This function does the following:
   * <ul>
   * <li> Calls release on the previous owner (obj);
   * <li> Closes the FILE pointer of this object if it already owns one.
   * <li> Makes this object the owner of the FILE pointer released from the
   *      previous owner (obj).
   * </ul>
   */
  SmartFILEPtr &operator=(SmartFILEPtr& obj);

  /**
   * Destructor.
   *
   * Closes the owned FILE pointer if there is one.
   */
  ~SmartFILEPtr() noexcept;

  /**
   * Returns the owned pointer or nullptr if this smart pointer does not own one.
   *
   * @return The owned FILE pointer.
   */
  FILE *get() const noexcept;

  /**
   * Releases the owned FILE pointer.
   *
   * @return The released FILE pointer.
   */
  FILE *release() ;

private:

  /**
   * The owned pointer.  A value of nullptr means this smart pointer does not own
   * a pointer.
   */
  FILE *m_file;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   * Not implemented so that it cannot be called
   */
  SmartFILEPtr(const SmartFILEPtr &obj) noexcept;

}; // class SmartFILEPtr

} // namespace cta
