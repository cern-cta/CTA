/******************************************************************************
 *                      castor/tape/aggregator/RcpJobRequestMsgBody.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_RCPJOBREQUESTMSGBODY
#define CASTOR_TAPE_AGGREGATOR_RCPJOBREQUESTMSGBODY

#include "h/Castor_limits.h"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace aggregator {

  /**
   * An RCP job submission request message.
   */
  struct RcpJobRequestMsgBody {
    uint32_t tapeRequestId;
    uint32_t clientPort;
    uint32_t clientEuid;
    uint32_t clientEgid;
    char clientHost[CA_MAXHOSTNAMELEN+1];
    char deviceGroupName[CA_MAXDGNLEN+1];
    char driveName[CA_MAXUNMLEN+1];
    char clientUserName[CA_MAXUSRNAMELEN+1];
  };

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_RCPJOBREQUESTMSGBODY
