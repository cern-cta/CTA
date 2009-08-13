
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
 * @(#)$RCSfile: VdqmTapeGatewayHelper.cpp,v $ $Revision: 1.5 $ $Release$ 
 * $Date: 2009/08/13 08:40:53 $ $Author: gtaur $
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

int castor::tape::tapegateway::VdqmTapeGatewayHelper::submitTapeToVdqm( castor::stager::Tape* tape, std::string dgn, int port) throw (castor::exception::Exception){

  // send vol request

  int reqId=0; // to have it as output
  char vidForC[CA_MAXVIDLEN+1];
  char dgnForC[CA_MAXDGNLEN+1];
  
  strcpy(vidForC, tape->vid().c_str());
  strcpy (dgnForC, dgn.c_str());
  
  serrno=0;
  int ret = vdqm_SendAggregatorVolReq (NULL,&reqId,vidForC,dgnForC,NULL,NULL,tape->tpmode(),port);

  if (ret<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::submitTapeToVdqm"
      <<" vdqm_SendAggregatorVolReq failed";
    throw ex;
  }

  return reqId;

}

 void castor::tape::tapegateway::VdqmTapeGatewayHelper::checkVdqmForRequest( castor::tape::tapegateway::TapeGatewayRequest* tapeRequest) throw (castor::exception::Exception) {
   serrno=0;
   int rc = vdqm_PingServer(NULL,NULL,tapeRequest->vdqmVolReqId()); // I don't give the dgn

   if (rc<0) {
     castor::exception::Exception ex(serrno);
     ex.getMessage()
       << "castor::tape::tapegateway::VdqmTapeGatewayHelper::checkVdqmForReques"
       << " vdqm_PingServer failed";
     throw ex;
   }
 }
