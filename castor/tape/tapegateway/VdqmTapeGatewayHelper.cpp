
/******************************************************************************
 *                      VdqmTapeGatewayHelper.cpp
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
 * @(#)$RCSfile: VdqmTapeGatewayHelper.cpp,v $ $Revision: 1.1 $ $Release$ 
 * $Date: 2009/01/19 17:20:34 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include "castor/tape/tapegateway/VdqmTapeGatewayHelper.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "net.h"
#include "vdqm_api.h"

int castor::tape::tapegateway::VdqmTapeGatewayHelper::submitTapeToVdqm( castor::stager::Tape* tape, std::string dgn, int port) {

  // send vol request

  int reqId=0; // to have it as output TODO
  char vidForC[CA_MAXVIDLEN+1];
  char dgnForC[CA_MAXDGNLEN+1];
  
  strcpy(vidForC, tape->vid().c_str());
  strcpy (dgnForC, dgn.c_str());

  int ret = vdqm_SendAggregatorVolReq (NULL,&reqId,vidForC,dgnForC,NULL,NULL,tape->tpmode(),port);

  if (ret<0) return -1;
  return reqId;

}

 int castor::tape::tapegateway::VdqmTapeGatewayHelper::checkVdqmForRequest( castor::tape::tapegateway::TapeRequestState* tapeRequest) {

   int rc = vdqm_PingServer(NULL,NULL,tapeRequest->vdqmVolReqId()); // I don't give the dgn

   if ( rc == -1 ) {
     int save_serrno = serrno;
     if ( (save_serrno != EVQHOLD) && (save_serrno != SECOMERR) ) {
       // for these errors we send it again.
       return -1;
     }
     // ignored other errors

   }
   return 0; 
}
