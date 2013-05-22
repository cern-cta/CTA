/******************************************************************************
 *                      castor/tape/tapebridge/SessionException.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_SESSIONEXCEPTION_HPP
#define CASTOR_TAPE_TAPEBRIDGE_SESSIONEXCEPTION_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapebridge/SessionError.hpp"

#include <string>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * An exception that wraps a copy of a SessionError object.
 *
 * This class should be thrown by methods that do not wish to modify themselves
 * the m_sessionErrors member of the BridgeProtocolEngine class.
 */
class SessionException: public castor::exception::Exception {
public:

  /**
   * Constructor.
   *
   * @param sessionError The SessionError object to be copied into this
   * exception class.
   */
  SessionException(const SessionError &sessionError) throw();

  /**
   * Returns a "read only" (const) reference to the wrapped SessionError
   * object.
   *
   * @return A "read only" (const) reference to the wrapped SessionError
   * object.
   */
  const SessionError &getSessionError() throw();

private:

  /**
   * The session error wrapped by this exception class.
   */
  SessionError m_sessionError;

}; // class SessionException

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_SESSIONEXCEPTION_HPP
