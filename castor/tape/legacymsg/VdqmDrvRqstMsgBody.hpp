/******************************************************************************
 *         castor/tape/legacymsg/VdqmDrvRqstMsgBody.hpp
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

#pragma once

#include "h/Castor_limits.h"

#include <stdint.h>

namespace castor    {
namespace tape      {
namespace legacymsg {

/**
 * The body of a vdqm drive request message.
 */
struct VdqmDrvRqstMsgBody {
  int32_t status;
  int32_t drvReqId;
  int32_t volReqId;  // Volume request ID for running requests
  int32_t jobId;
  int32_t recvTime;
  int32_t resetTime; // Last time counters were reset
  int32_t useCount;  // Usage counter (total number of VolReqs so far)
  int32_t errCount;  // Drive error counter
  int32_t transfMB;  // MBytes transfered in last request
  int32_t mode;      // WRITE_ENABLE/WRITE_DISABLE from Ctape_constants.h
  uint64_t totalMB;  // Total MBytes transfered
  char volId[CA_MAXVIDLEN+1];
  char server[CA_MAXHOSTNAMELEN+1];
  char drive[CA_MAXUNMLEN+1];
  char dgn[CA_MAXDGNLEN+1];
  char dedicate[CA_MAXLINELEN+1];

  /**
   * Constructor.
   *
   * Sets all integers to 0 and all strings to the empty string.
   */
  VdqmDrvRqstMsgBody() throw();
}; // struct VdqmDrvRqstMsgBody

} // namespace legacymsg
} // namespace tape
} // namespace castor

