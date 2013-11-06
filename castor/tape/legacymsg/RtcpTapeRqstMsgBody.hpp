/******************************************************************************
 *                      castor/tape/legacymsg/RtcpTapeRqstMsgBody.hpp
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

#ifndef CASTOR_TAPE_LEGACYMSG_RTCPTAPERQSTMSGBODY
#define CASTOR_TAPE_LEGACYMSG_RTCPTAPERQSTMSGBODY

#include "castor/tape/legacymsg/RtcpErrorAppendix.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace legacymsg {

  /**
   * An RTCP tape request without error appendix message.
   */
  struct RtcpTapeRqstMsgBody {
    char vid[CA_MAXVIDLEN+1];
    char vsn[CA_MAXVSNLEN+1];
    char label[CA_MAXLBLTYPLEN+1];
    char devtype[CA_MAXDVTLEN+1];
    char density[CA_MAXDENLEN+1];
    char unit[CA_MAXUNMLEN+1];
    uint32_t volReqId;      // VDQM volume request ID
    uint32_t jobId;         // Local RTCOPY server job ID
    uint32_t mode;          // WRITE_DISABLE or WRITE_ENABLE
    uint32_t start_file;    // Start file if mapped VID
    uint32_t end_file;      // End file if mapped VID
    uint32_t side;          // Disk side number
    uint32_t tprc;          // Return code from last Ctape
    uint32_t tStartRequest; // Start time of request (set by client)
    uint32_t tEndRequest;   // End time of request (set by client)
    uint32_t tStartRtcpd;   // Time when request is received by rtcpd server
    uint32_t tStartMount;   // Time when mount request is sent to Ctape
    uint32_t tEndMount;     // Time when mount request returns
    uint32_t tStartUnmount; // Time when unmount request is sent to Ctape
    uint32_t tEndUnmount;   // Time when unmount request returns
    Cuuid_t  rtcpReqId;     // Unique request id assigned by RTCOPY

  }; // struct RtcpTapeRqstMsgBody

} // namespace legacymsg
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_LEGACYMSG_RTCPTAPERQSTMSGBODY
