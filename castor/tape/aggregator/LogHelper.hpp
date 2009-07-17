/******************************************************************************
 *                      castor/tape/aggregator/LogHelper.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_LOGHELPER
#define CASTOR_TAPE_AGGREGATOR_LOGHELPER

#include "castor/tape/aggregator/GiveOutpMsgBody.hpp"
#include "castor/tape/aggregator/RcpJobReplyMsgBody.hpp"
#include "castor/tape/aggregator/RcpJobRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpAbortMsgBody.hpp"
#include "castor/tape/aggregator/RtcpNoMoreRequestsMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"

#include "h/Cuuid.h"


namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Collection of static methods to facilitate logging.
 */
class LogHelper {

public:

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t           &cuuid,
    const int               severity,
    const int               message_no,
    const uint32_t          volReqId,
    const int               socketFd,
    const RcpJobRqstMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t            &cuuid,
    const int                severity,
    const int                message_no,
    const uint32_t           volReqId,
    const int                socketFd,
    const RcpJobReplyMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                &cuuid,
    const int                    severity,
    const int                    message_no,
    const uint32_t               volReqId,
    const int                    socketFd,
    const RtcpTapeRqstErrMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t             &cuuid,
    const int                 severity,
    const int                 message_no,
    const uint32_t            volReqId,
    const int                 socketFd,
    const RtcpTapeRqstMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                &cuuid,
    const int                    severity,
    const int                    message_no,
    const uint32_t               volReqId,
    const int                    socketFd,
    const RtcpFileRqstErrMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t             &cuuid,
    const int                 severity,
    const int                 message_no,
    const uint32_t            volReqId,
    const int                 socketFd,
    const RtcpFileRqstMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t         &cuuid,
    const int             severity,
    const int             message_no,
    const uint32_t        volReqId,
    const int             socketFd,
    const GiveOutpMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                   &cuuid,
    const int                       severity,
    const int                       message_no,
    const uint32_t                  volReqId,
    const int                       socketFd,
    const RtcpNoMoreRequestsMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param message_no The message number in the facility.
   * @param severity   The severity of the message.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t          &cuuid,
    const int              severity,
    const int              message_no,
    const uint32_t         volReqId,
    const int              socketFd,
    const RtcpAbortMsgBody &body) throw();
};

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_LOGHELPER
