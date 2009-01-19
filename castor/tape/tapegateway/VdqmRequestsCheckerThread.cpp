/******************************************************************************
 *                     VdqmRequestsCheckerThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: VdqmRequestsCheckerThread.cpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/tape/tapegateway/VdqmRequestsCheckerThread.hpp"
#include "castor/tape/tapegateway/VdqmTapeGatewayHelper.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Exception.hpp"


  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::VdqmRequestsCheckerThread::VdqmRequestsCheckerThread(castor::tape::tapegateway::ITapeGatewaySvc* svc, u_signed64 timeOut){
  m_dbSvc=svc; 
  m_timeOut=timeOut;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsCheckerThread::run(void* par)
{

  std::vector<castor::tape::tapegateway::TapeRequestState*> tapeRequests;
  
  try {
     // get tapes to check from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 10, 0, NULL);

    tapeRequests = m_dbSvc->getTapesToCheck(m_timeOut); 
  
  } catch (castor::exception::Exception e) {
     // error in getting new tape to submit

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 11, 2, params); 
    return;
  }

  std::vector<castor::tape::tapegateway::TapeRequestState*> tapesToRetry;

  // check the tapes to vdqm
  std::vector<castor::tape::tapegateway::TapeRequestState*>::iterator tapeRequest = tapeRequests.begin();
  
  while (tapeRequest != tapeRequests.end()){

    castor::dlf::Param params[] =
      {castor::dlf::Param("vdqm request id", (*tapeRequest)->vdqmVolReqId())
      };
    
    castor::tape::tapegateway::VdqmTapeGatewayHelper vdqmHelper;

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 12, 1, params);

    int ret = vdqmHelper.checkVdqmForRequest(*tapeRequest);
    if (ret<0) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 13, 1, params);
      tapesToRetry.push_back(*tapeRequest);
    }
    tapeRequest++;
  }
  
  // update the db to eventually send again some requests

  try {
    if (!tapesToRetry.empty())
      m_dbSvc->updateCheckedTapes(tapesToRetry); 
  } catch (castor::exception::Exception e){
     
    // impossible to update the information of checked tape
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 2, params); 
 
  }

  // cleanUp
  
  tapeRequest=tapeRequests.begin();
  while (tapeRequest != tapeRequests.end()){
    if (*tapeRequest) delete *tapeRequest;
    *tapeRequest=NULL;
    tapeRequest++;
  }
  tapeRequests.clear();
  tapesToRetry.clear();
  

}




