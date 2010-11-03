
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


#include "castor/tape/tapegateway/daemon/VdqmTapeGatewayHelper.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <cstring>

#include "castor/Services.hpp"
#include "vdqm_api.h"


void castor::tape::tapegateway::VdqmTapeGatewayHelper::connectToVdqm() throw (castor::exception::Exception){
  serrno=0;
  m_connection=NULL;
  int rc = vdqm_Connect(&m_connection);
  if (rc<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::connectToVdqm"
      << " connect to vdqm failed";
    throw ex;
  }
  return;
}

int castor::tape::tapegateway::VdqmTapeGatewayHelper::createRequestForAggregator( const castor::stager::Tape& tape, const int& port) throw (castor::exception::Exception){

  // send vol request

  int reqId=0; // to have it as output
  char vidForC[CA_MAXVIDLEN+1];
  char dgnForC[CA_MAXDGNLEN+1];
  
  strcpy(vidForC, tape.vid().c_str());
  strcpy (dgnForC, tape.dgn().c_str());
  
  serrno=0;
  int ret = vdqm_CreateRequestForAggregator(m_connection,&reqId,vidForC,dgnForC,NULL,NULL,tape.tpmode(),port);

  if (ret<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::submitTapeToVdqm"
      <<" vdqm_CreateRequestForAggregator failed";
    throw ex;
  }

  return reqId;

}

void castor::tape::tapegateway::VdqmTapeGatewayHelper::confirmRequestToVdqm() throw (castor::exception::Exception){

  // after saving the transaction id in the db I send the confirmation to vdqm
  
  serrno=0;
  int ret = vdqm_QueueRequestForAggregator(m_connection);

  if (ret<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::submitTapeToVdqm"
      <<" vdqm_QueueRequestForAggregator failed";
    throw ex;
  }

}


 void castor::tape::tapegateway::VdqmTapeGatewayHelper::checkVdqmForRequest( const castor::tape::tapegateway::TapeGatewayRequest& tapeRequest) throw (castor::exception::Exception) {
   serrno=0;
   int rc = vdqm_PingServer(NULL,NULL,tapeRequest.vdqmVolReqId()); // I don't give the dgn

   if (rc<0) {
     castor::exception::Exception ex(serrno);
     ex.getMessage()
       << "castor::tape::tapegateway::VdqmTapeGatewayHelper::checkVdqmForRequest"
       << " vdqm_PingServer failed";
     throw ex;
   }
 }


void castor::tape::tapegateway::VdqmTapeGatewayHelper::disconnectFromVdqm()throw (castor::exception::Exception){
  serrno=0;
  int rc = vdqm_Disconnect(&m_connection);
  if (rc<0) {
    castor::exception::Exception ex(serrno);
    ex.getMessage()
      << "castor::tape::tapegateway::VdqmTapeGatewayHelper::disconnectFromVdqm"
      << " connect to vdqm failed";
    throw ex;
  }
  return;
}
