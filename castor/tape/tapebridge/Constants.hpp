/******************************************************************************
 *                      castor/tape/tapebridge/Constants.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_CONSTANTS_HPP
#define CASTOR_TAPE_TAPEBRIDGE_CONSTANTS_HPP 1

#include "h/Castor_limits.h"

#include <stdint.h>
#include <stdlib.h>


namespace castor     {
namespace tape       {
namespace tapebridge {
  	
  /**
   * The minumim timeout in seconds between pings to the tapebridge clients.
   * Please note that the tape gateway is not pinged.
   */
  const int CLIENTPINGTIMEOUT = 5;

  /**
   * The size of an RTCOPY message header.  The header contains 3 32-bit integer
   * fields: magic number, request type and length or status.
   */
  const size_t HDRBUFSIZ = 3 * sizeof(uint32_t);

  /**
   * The maximum number of pending transfers between the tapebridge and RTCPD.
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
   * The program name of the tapebridge daemon.
   */
  const char *const TAPEBRIDGEPROGRAMNAME = "tapebridged";

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
   * The timeout in seconds for which the tapebridge waits for RTCPD to call it
   * back.
   */
  const int RTCPDCALLBACKTIMEOUT = 55;

  /**
   * The minimum timeout in seconds between sending a ping message from the
   * tapebridge to RTCPD.
   */
  const int RTCPDPINGTIMEOUT = 30;

  /**
   * The timeout in seconds used when sending and receiving client network
   * messages, where client refers to either tape gateway, readtp, writetp or
   * dumptp.  A value less than 60 seconds should be used because the default
   * timeout used by rtcpd when sending and receiving messages to and from the
   * tapebridge is 60 seconds.  The file CASTOR2/h/rtcp_constants.h contains
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
   * tapebridge is 60 seconds.  The file CASTOR2/h/rtcp_constants.h contains
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

  /**
   * The compile-time default value of the tapebridged configuration parameter
   * named TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES.
   *
   * The value of this parameter defines whether or not the tapebridged and
   * rtcpd daemons will use buffered tape-marks over multiple files when
   * migrating data to tape.
   */
  const bool TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES = false;

  /**
   * The compile-time default value of the tapebridged configuration parameter
   * named TAPEBRIDGE/MAXBYTESBEFOREFLUSH.
   *
   * The value of this parameter defines the maximum number of bytes to be
   * written to tape before a flush to tape (synchronised tape-mark).  Please
   * note that a flush occurs on a file boundary therefore more bytes will
   * normally be written to tape before the actual flush occurs.
   *
   * The value of this parameter is used when buffered tape-marks are being
   * used over multiple files as defined by the parameter named
   * TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES.
   */
  const uint64_t TAPEBRIDGE_MAXBYTESBEFOREFLUSH = 8589934592ULL; // 8 GiB

  /**
   * The compile-time default value of the tapebridged configuration parameter
   * named TAPEBRIDGE/MAXFILESBEFOREFLUSH.
   *
   * The value of this parameter defines the maximum number of files to be
   * written to tape before a flush to tape (synchronised tape-mark).
   *
   * The value of this parameter is used when buffered tape-marks are being
   * used over multiple files as defined by the parameter named
   * TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES.
   */
  const uint64_t TAPEBRIDGE_MAXFILESBEFOREFLUSH = 100;

} // namespace tapebridge
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPEBRIDGE_CONSTANTS_HPP
