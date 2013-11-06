/******************************************************************************
 *                      castor/tape/tapebridge/SessionErrorList.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_SESSIONERRORLIST_HPP
#define CASTOR_TAPE_TAPEBRIDGE_SESSIONERRORLIST_HPP

#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapebridge/SessionError.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/Cuuid.h"

#include <list>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * The list of session errors detected by the tapebridged daemon during a
 * tape-session.
 *
 * Besides storing errors, this class also logs the very first error it
 * receives because this error will the one that causes the current
 * tape-session to start shutting down gracefully.
 */
class SessionErrorList: public std::list<SessionError> {
public:

  /**
   * Constructor.
   *
   * @param cuuid      The ccuid to be used for logging.
   * @param jobRequest The RTCOPY job request from the VDQM.
   * @param volume     The volume message received from the client.
   */
  SessionErrorList(
    const Cuuid_t                       &cuuid,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest,
    const tapegateway::Volume           &volume)
    throw();

  /**
   * Pushes the specified SessionError onto the back of the list.
   *
   * If the SessionError is the first to be pushed onto the back of the list,
   * then this method will log the contents the SessionError to DLF.
   *
   * @param error The session-error to be pushed on to the back of the list.
   */
  void push_back(const SessionError &error);

private:

  /**
   * Delegates to either the logFileScopeSessionError() method or the
   * logNonFileScopeSessionError() method the taks of logging the specified
   * session-error to DLF.
   *
   * @param error The session-error to be logged.
   */
  void logSessionError(const SessionError &error) throw();

  /**
   * Logs the specified session-error to DLF including the information about
   * the file associated with the session-error.
   *
   * @param error The session-error to be logged.
   */
  void logFileScopeSessionError(const SessionError &error) throw();

  /**
   * Logs the specified session-error to DLF excldung any file-specific
   * information.
   *
   * @param error The session-error to be logged.
   */
  void logNonFileScopeSessionError(const SessionError &error) throw();

  /**
   * The cuuid to be used for logging.
   */
  const Cuuid_t &m_cuuid;

  /**
   * The RTCOPY job request from the VDQM.
   */
  const legacymsg::RtcpJobRqstMsgBody &m_jobRequest;

  /**
   * The volume message received from the client.
   */
  const tapegateway::Volume &m_volume;

}; // class SessionErrorList

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_SESSIONERRORLIST_HPP
