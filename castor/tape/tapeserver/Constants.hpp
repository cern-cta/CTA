/******************************************************************************
 *                      castor/tape/tapeserver/Constants.hpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPESERVER_CONSTANTS_HPP
#define CASTOR_TAPE_TAPESERVER_CONSTANTS_HPP 1

#include "h/Castor_limits.h"

#include <stdint.h>
#include <stdlib.h>


namespace castor     {
namespace tape       {
namespace tapeserver {
  	
  /**
   * The minumim timeout in seconds between pings to the tapeserver clients.
   * Please note that the tape gateway is not pinged.
   */
  const int CLIENTPINGTIMEOUT = 5;

  /**
   * The size of an RTCOPY message header.  The header contains 3 32-bit integer
   * fields: magic number, request type and length or status.
   */
  const size_t HDRBUFSIZ = 3 * sizeof(uint32_t);

  /**
   * The maximum number of pending transfers between the tapeserver and RTCPD.
   * The theoretical maximum number of outstanding pending transfers is equal
   * to the memory allocated to memory buffers of RTCPD divided by the average
   * file size.
   */
  const int MAXPENDINGTRANSFERS = 100000;

  /**
   * The maximum size of an RTCOPY message in bytes including both message
   * header and message body.
   */
  const size_t RTCPMSGBUFSIZE = 4096;

  /**
   * The program name of the tapeserver daemon.
   */
  const char *const TAPESERVERPROGRAMNAME = "tapeserverd";

  /**
   * The tape record format.
   */
  const char *const RECORDFORMAT = "F";

  /**
   * The timeout in seconds used when sending and receiving RTCPD network
   * messages.
   */
  const int RTCPDNETRWTIMEOUT = 55;

  /**
   * The timeout in seconds for which the tapeserver waits for RTCPD to call it
   * back.
   */
  const int RTCPDCALLBACKTIMEOUT = 55;

  /**
   * The minimum timeout in seconds between sending a ping message from the
   * tapeserver to RTCPD.
   */
  const int RTCPDPINGTIMEOUT = 30;

  /**
   * The timeout in seconds used when sending and receiving client network
   * messages, where client refers to either tape gateway, readtp, writetp or
   * dumptp.  A value less than 60 seconds should be used because the default
   * timeout used by rtcpd when sending and receiving messages to and from the
   * tapeserver is 60 seconds.  The file CASTOR2/h/rtcp_constants.h contains
   * the following line:
   * <code>
   * #define RTCP_NETTIMEOUT (60)
   * </code>
   */
  const int CLIENTNETRWTIMEOUT = 55;

  /**
   * The maximum size of a VMGR message in bytes including both message header
   * and message body.
   */
  const size_t VMGRMSGBUFSIZE = 4096;

  /**
   * The timeout in seconds used when sending and receiving VMGR network
   * messages.  A value less than 60 seconds should be used because the default
   * timeout used by rtcpd when sending and receiving messages to and from the
   * tapeserver is 60 seconds.  The file CASTOR2/h/rtcp_constants.h contains
   * the following line:
   * <code>
   * #define RTCP_NETTIMEOUT (60)
   * </code>
   */
  const int VMGRNETRWTIMEOUT = 55;

  /**
   * An empty volume serial number.
   */
  const char EMPTYVSN[CA_MAXVSNLEN+1] = "";

} // namespace tapeserver
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPESERVER_CONSTANTS_HPP
