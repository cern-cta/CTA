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


namespace cta {
namespace mediachanger {

/**
 * A smart pointer that owns a ZMQ context.  If the smart pointer goes out of
 * scope and it owns a ZMQ context, then it will terminate that context by
 * calling zmq_term().
 */
class SmartZmqContext {

public:

  /**
   * Constructor.
   */
  SmartZmqContext() throw();

  /**
   * Constructor.
   *
   * @param zmqContext The ZMQ context to be owned by the smart pointer.
   */
  SmartZmqContext(void *const zmqContext) throw();

  /**
   * Take ownership of the specified ZMQ context, terminating the previously
   * owned ZMQ context if there is one and it is not the same as the one
   * specified.
   *
   * @param zmqContext The ZMQ context to be owned, defaults to NULL if not
   * specified, where NULL means this smart pointer will not own a ZMQ context
   * after the reset() method returns.
   */
  void reset(void *const zmqContext = NULL) throw();

  /**
   * SmartZmqContext assignment operator.
   *
   * This function does the following:
   * <ul>
   * <li> Calls release on the previous owner (obj);
   * <li> Terminates the ZMQ context of this object if it already owns one.
   * <li> Makes this object the owner of the ZMQ context released from the
   *      previous owner (obj).
   * </ul>
   */
  SmartZmqContext &operator=(SmartZmqContext& obj);

  /**
   * Destructor.
   *
   * If the smart pointer owns a ZMQ context, then the destructor will
   * terminate it by calling zmq_term().
   */
  ~SmartZmqContext() throw();

  /**
   * Returns the owned ZMQ context or NULL if this smart pointer does not own
   * one.
   *
   * @return The owned ZMQ context or NULL.
   */
  void *get() const throw();

  /**
   * Releases the owned ZMQ context.
   *
   * @return The released ZMQ context.
   */
  void *release() ;

private:

  /**
   * The owned ZMQ context.  A value of NULL means this smart pointer does not
   * own a ZMQ context.
   */ 
  void *m_zmqContext;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   *
   * Not implemented so that it cannot be called.
   */
  SmartZmqContext(const SmartZmqContext &obj) throw();

}; // class SmartZmqContext

} // namespace mediachanger
} // namespace cta

