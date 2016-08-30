/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "common/exception/NotAnOwner.hpp"

#include <stdio.h>


namespace castor {
namespace utils {

/**
 * A smart pointer that owns a FILE pointer.  When the smart pointer goes out
 * of scope, it will fclose the FILE pointer it owns.
 */
class SmartFILEPtr {

public:

  /**
   * Constructor.
   */
  SmartFILEPtr() throw();

  /**
   * Constructor.
   *
   * @param file The FILE pointer to be owned by the smart pointer.
   */
  SmartFILEPtr(FILE *const file) throw();

  /**
   * Take ownership of the specified FILE pointer, closing the previously
   * owned FILE pointer if there is one and it is not the same as the one
   * specified.
   *
   * @param file The FILE pointer to be owned, defaults to NULL if not
   *             specified, where NULL means this smart pointer will not own a
   *             pointer after the reset() method returns.
   */
  void reset(FILE *const file = NULL) throw();

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
  ~SmartFILEPtr() throw();

  /**
   * Returns the owned pointer or NULL if this smart pointer does not own one.
   *
   * @return The owned FILE pointer.
   */
  FILE *get() const throw();

  /**
   * Releases the owned FILE pointer.
   *
   * @return The released FILE pointer.
   */
  FILE *release() ;

private:

  /**
   * The owned pointer.  A value of NULL means this smart pointer does not own
   * a pointer.
   */ 
  FILE *m_file;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   * Not implemented so that it cannot be called
   */
  SmartFILEPtr(const SmartFILEPtr &obj) throw();

}; // class SmartFILEPtr

} // namespace utils
} // namespace castor

