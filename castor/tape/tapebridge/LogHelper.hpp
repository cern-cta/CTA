/******************************************************************************
 *                      castor/tape/tapebridge/LogHelper.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_LOGHELPER
#define CASTOR_TAPE_TAPEBRIDGE_LOGHELPER

#include "castor/tape/legacymsg/GiveOutpMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpJobReplyMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpAbortMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpDumpTapeRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpNoMoreRequestsMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpTapeRqstErrMsgBody.hpp"
#include "castor/tape/legacymsg/VmgrTapeInfoMsgBody.hpp"
#include "castor/tape/tapegateway/DumpParameters.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/Cuuid.h"
#include "h/tapeBridgeFlushedToTapeMsgBody.h"

#include <sys/time.h>
#include <time.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Collection of static methods to facilitate logging.
 */
class LogHelper {

public:

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                      &cuuid,
    const int                          severity,
    const int                          message_no,
    const uint32_t                     volReqId,
    const int                          socketFd,
    const legacymsg::RtcpJobRqstMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                       &cuuid,
    const int                           severity,
    const int                           message_no,
    const uint32_t                      volReqId,
    const int                           socketFd,
    const legacymsg::RtcpJobReplyMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                           &cuuid,
    const int                               severity,
    const int                               message_no,
    const uint32_t                          volReqId,
    const int                               socketFd,
    const legacymsg::RtcpTapeRqstErrMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                        &cuuid,
    const int                            severity,
    const int                            message_no,
    const uint32_t                       volReqId,
    const int                            socketFd,
    const legacymsg::RtcpTapeRqstMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                           &cuuid,
    const int                               severity,
    const int                               message_no,
    const uint32_t                          volReqId,
    const int                               socketFd,
    const legacymsg::RtcpFileRqstErrMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                        &cuuid,
    const int                            severity,
    const int                            message_no,
    const uint32_t                       volReqId,
    const int                            socketFd,
    const legacymsg::RtcpFileRqstMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                    &cuuid,
    const int                        severity,
    const int                        message_no,
    const uint32_t                   volReqId,
    const int                        socketFd,
    const legacymsg::GiveOutpMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                              &cuuid,
    const int                                  severity,
    const int                                  message_no,
    const uint32_t                             volReqId,
    const int                                  socketFd,
    const legacymsg::RtcpNoMoreRequestsMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                     &cuuid,
    const int                         severity,
    const int                         message_no,
    const uint32_t                    volReqId,
    const int                         socketFd,
    const legacymsg::RtcpAbortMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                            &cuuid,
    const int                                severity,
    const int                                message_no,
    const uint32_t                           volReqId,
    const int                                socketFd,
    const legacymsg::RtcpDumpTapeRqstMsgBody &body) throw();

  /**
   * Logs the specified RTCP message body.
   *
   * @param cuuid            The uuid of the component issuing the message.
   * @param severity         The severity of the message.
   * @param message_no       The message number in the facility.
   * @param volReqId         The volume request ID.
   * @param socketFd         The file descriptor of the associated TCP/IP
   *                         socket.
   * @param body             The message body.
   * @param connectDuration  The time it took to connect to the client.
   * @param sendRecvDuration The time it took to send the request and receive
   *                         the reply.
   */
  static void logMsgBody(
    const Cuuid_t                        &cuuid,
    const int                            severity,
    const int                            message_no,
    const uint32_t                       volReqId,
    const int                            socketFd,
    const legacymsg::VmgrTapeInfoMsgBody &body,
    const timeval                        &connectDuration,
    const timeval                        &sendRecvDuration) throw();

  /**
   * Logs the specified client message body.
   *
   * @param cuuid      The uuid of the component issuing the message.
   * @param severity   The severity of the message.
   * @param message_no The message number in the facility.
   * @param volReqId   The volume request ID.
   * @param socketFd   The file descriptor of the associated TCP/IP socket.
   * @param body       The message body.
   */
  static void logMsgBody(
    const Cuuid_t                          &cuuid,
    const int                              severity,
    const int                              message_no,
    const uint32_t                         volReqId,
    const int                              socketFd,
    const tapeBridgeFlushedToTapeMsgBody_t &body) throw();

  /**
   * Logs the specified Client message.
   *
   * @param cuuid            The uuid of the component issuing the message.
   * @param severity         The severity of the message.
   * @param message_no       The message number in the facility.
   * @param msg              The message.
   * @param connectDuration  The time it took to connect to the client.
   * @param sendRecvDuration The time it took to send the request and receive
   *                         the reply.
   */
  static void logMsg(
    const Cuuid_t             &cuuid,
    const int                 severity,
    const int                 message_no,
    const tapegateway::Volume &msg,
    const timeval             &connectDuration,
    const timeval             &sendRecvDuration) throw();

  /**
   * Logs the specified Client message.
   *
   * @param cuuid            The uuid of the component issuing the message.
   * @param severity         The severity of the message.
   * @param message_no       The message number in the facility.
   * @param msg              The message.
   * @param connectDuration  The time it took to connect to the client.
   * @param sendRecvDuration The time it took to send the request and receive
   *                         the reply.
   */
  static void logMsg(
    const Cuuid_t                     &cuuid,
    const int                         severity,
    const int                         message_no,
    const tapegateway::DumpParameters &msg,
    const timeval                     &connectDuration,
    const timeval                     &sendRecvDuration) throw();
};

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_LOGHELPER
