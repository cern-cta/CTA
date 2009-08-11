/******************************************************************************
 *                      castor/tape/aggregator/Constants.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_CONSTANTS_HPP
#define CASTOR_TAPE_AGGREGATOR_CONSTANTS_HPP 1

#include <stdint.h>
#include <stdlib.h>


namespace castor     {
namespace tape       {
namespace aggregator {
  	
  /**
   * The minumim timeout in seconds between pings to the aggregator clients.
   * Please note that the tape gateway is not pinged.
   */
  const int CLIENTPINGTIMEOUT = 5;

  /**
   * The size of an RTCOPY message header.  The header contains 3 32-bit integer
   * fields: magic number, request type and length or status.
   */
  const size_t HDRBUFSIZ = 3 * sizeof(uint32_t);

  /**
   * The maximum number of tape drives a single aggregator can cope with.
   */
  const int MAXDRIVES = 4;

  /**
   * The maximum number of pending transfers between the aggregator and RTCPD.
   * The actual number of outstanding pending transfers is equal to the number
   * of disk IO when all of the disk IO threads are busy transfering files.
   */
  const int MAXPENDINGTRANSFERS = 100;

  /**
   * The maximum size of an RTCOPY message in bytes including both message
   * header and message body.
   */
  const size_t RTCPMSGBUFSIZE = 4096;

  /**
   * The program name of the aggregator daemon.
   */
  const char *const AGGREGATORPROGRAMNAME = "aggregatord";

  /**
   * The tape record format.
   */
  const char *const RECORDFORMAT = "F";

  /**
   * The timeout in seconds to be used when the network reads and writes of the
   * RTCOPY protocol.
   */
  const int RTCPDNETRWTIMEOUT     = 5;

  /**
   * The timeout in seconds for which the aggregator waits for RTCPD to call it
   * back.
   */
  const int RTCPDCALLBACKTIMEOUT  = 5;

  /**
   * The minimum timeout in seconds between sending a ping message from the
   * aggregator to RTCPD.
   */
  const int RTCPDPINGTIMEOUT = 30;

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_CONSTANTS_HPP
