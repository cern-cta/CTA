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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include <stddef.h>
#include <sys/capability.h>

namespace castor {
namespace server {

/**
 * A smart pointer that owns a capability state.
 *
 * This smart pointer will call cap_free() if it needs to free the owned
 * resource.
 *
 * Please note that process capabilities are not supported on Mac OS X.
 */
class SmartCap {
public:

  /**
   * Constructor.
   */
  SmartCap() throw();

  /**
   * Constructor.
   *
   * @param cap The capability state to be owned.
   */
  SmartCap(cap_t cap) throw();

  /**
   * Takes ownership of the specified capability state.  If this smart pointer
   * already owns a capbility state that is not the same as the one specified
   * then it will be freed using cap_free().
   *
   * @param cap The capability state to be owned.  If a capabibility state is
   * not specified then the default value of NULL will be used.  In this default
   * case the smart pointer will not own a capbility state after the reset()
   * method returns.
   */
  void reset(cap_t cap = NULL) throw();

  /**
   * SmartCap assignment operator.
   *
   * This function does the following:
   * <ul>
   * <li> Calls release on the previous owner (obj);
   * <li> Resets this smart pointer to the released pointer of the previous
   * owner (obj).
   * </ul>
   */
  SmartCap &operator=(SmartCap& obj);

  /**
   * Destructor.
   *
   * Resets this smart pointer with the default value of NULL.
   */
  ~SmartCap() throw();

  /**
   * Returns the owned capbility state or NULL if this smart pointer does not
   * own one.
   *
   * @return The owned capbility state or NULL if this smart pointer does not
   * own one.
   */
  cap_t get() const throw();

  /**
   * Releases the owned capability state.
   *
   * @return The released capability state.
   */
  cap_t release();

private:

  /**
   * The owned capbility state or NULL if this smart pointer does not own one.
   */ 
  cap_t m_cap;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   *
   * Not implemented so that it cannot be called.
   */
  SmartCap(const SmartCap &obj) throw();

}; // class SmartCap

} // namespace server
} // namespace castor

