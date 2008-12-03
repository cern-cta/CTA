/******************************************************************************
 *                      castor/tape/aggregator/RtcpTapeRequest.hpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_RTCPTAPEREQUEST
#define CASTOR_TAPE_AGGREGATOR_RTCPTAPEREQUEST

#include "h/Castor_limits.h"


namespace castor     {
namespace tape       {
namespace aggregator {

  /**
   * An RTCP tape request message.
   */
  struct RtcpTapeRequest {
    char vid[CA_MAXVIDLEN+1];
    char vsn[CA_MAXVSNLEN+1];
    char label[CA_MAXLBLTYPLEN+1];
    char devtype[CA_MAXDVTLEN+1];
    char density[CA_MAXDENLEN+1];
    char unit[CA_MAXUNMLEN+1];
    int VolReqID;              // VDQM volume request ID
    int jobID;                 // Local RTCOPY server job ID
    int mode;                  // WRITE_DISABLE or WRITE_ENABLE
    int start_file;            // Start file if mapped VID
    int end_file;              // End file if mapped VID
    int side;                  // Disk side number
    int tprc;                  // Return code from last Ctape
    int TStartRequest;         // Start time of request (set by client)
    int TEndRequest;           // End time of request (set by client)
    int TStartRtcpd;           // Time when request is received by rtcpd server
    int TStartMount;           // Time when mount request is sent to Ctape
    int TEndMount;             // Time when mount request returns
    int TStartUnmount;         // Time when unmount request is sent to Ctape
    int TEndUnmount;           // Time when unmount request returns

    // Unique request id assigned by RTCOPY
    Cuuid_t rtcpReqId;

    // Error reporting
    rtcpErrMsg_t err;

  }; // struct RtcpTapeRequest


} // namespace aggregator
} // namespace tape
} // namespace castor

#endif
