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
 * @(#)$RCSfile: VdqmRequestsCheckerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/Stream.hpp"

#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/VdqmRequestsCheckerThread.hpp"
#include "castor/tape/tapegateway/daemon/VdqmTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"



//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::VdqmRequestsCheckerThread::VdqmRequestsCheckerThread(u_signed64 timeOut){
  m_timeOut=timeOut;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsCheckerThread::run(void* par)
{

  std::list<castor::tape::tapegateway::TapeGatewayRequest> tapeRequests;
  
  std::list<castor::stager::Tape> tapesToReset;
  std::list<castor::stager::Tape>::iterator tapeToReset;
  std::list<std::string> vids;

 // service to access the database

  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);



  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }

  timeval tvStart,tvEnd;
  gettimeofday(&tvStart, NULL);
  
  
  try {
     // get tapes to check from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,CHECKER_GETTING_TAPES, 0, NULL);

    oraSvc->getTapesWithDriveReqs(tapeRequests,vids,m_timeOut);

  } catch (castor::exception::Exception e) {
     // error in getting new tape to submit

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,CHECKER_NO_TAPE, params);
    return;
  }

  if (tapeRequests.empty()){
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,CHECKER_NO_TAPE, 0, NULL);
    return;
  }
  
  
  gettimeofday(&tvEnd, NULL);
  signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param params[] =
    {
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, CHECHER_TAPES_FOUND, params);

  std::list<castor::tape::tapegateway::TapeGatewayRequest> tapesToRetry;

  // check the tapes to vdqm
  
  std::list<std::string>::iterator vid=vids.begin();

  for (std::list<castor::tape::tapegateway::TapeGatewayRequest>::iterator tapeRequest = tapeRequests.begin();
       tapeRequest != tapeRequests.end();
       tapeRequest++,vid++){

    castor::dlf::Param params[] =
      {castor::dlf::Param("mountTransactionId", (*tapeRequest).vdqmVolReqId()),
       castor::dlf::Param("TPVID", *vid)
      };

    castor::tape::tapegateway::VdqmTapeGatewayHelper vdqmHelper;

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,CHECKER_QUERYING_VDQM, params);

    try {
      vdqmHelper.checkVdqmForRequest(*tapeRequest);

    } catch (castor::exception::Exception e) {

      castor::dlf::Param params[] =
	{castor::dlf::Param("mountTransactionId", (*tapeRequest).vdqmVolReqId()),
       castor::dlf::Param("errorCode",sstrerror(e.code())),
	 castor::dlf::Param("errorMessage",e.getMessage().str()),
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, CHECKER_LOST_VDQM_REQUEST, params);
      if ( e.code() == EPERM ) {
	tapesToRetry.push_back(*tapeRequest);
	if ((*tapeRequest).accessMode() == 1 ){
	  castor::stager::Tape tapeToReset;
	  tapeToReset.setTpmode(1);
	  tapeToReset.setVid(*vid);
	  tapesToReset.push_back(tapeToReset);
	}
      }
    }
  }



  try {

    //  I update vmgr releasing the busy tapes

    for ( tapeToReset = tapesToReset.begin();
	  tapeToReset != tapesToReset.end();
	  tapeToReset++){
      castor::tape::tapegateway::VmgrTapeGatewayHelper vmgrHelper;
      try {
	vmgrHelper.resetBusyTape(*tapeToReset);
	castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", (*tapeToReset).vid())};
	castor::dlf::dlf_writep(nullCuuid,DLF_LVL_SYSTEM, CHECKER_RELEASING_UNUSED_TAPE, params);
      } catch (castor::exception::Exception e){
 	castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", (*tapeToReset).vid())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, CHECKER_VMGR_ERROR, params);
      }
    }
    // update the db to eventually send again some requests

    gettimeofday(&tvStart, NULL);
    
    oraSvc->restartLostReqs(tapesToRetry);

    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    castor::dlf::Param paramsReset[] =
    {
     castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, CHECKER_TAPES_RESURRECTED, paramsReset);
    


  } catch (castor::exception::Exception e){

    // impossible to update the information of checked tape

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, CHECKER_CANNOT_UPDATE_DB, params);

  }

}




