/******************************************************************************
 *                     VdqmRequestsProducerThread.cpp
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
 * @(#)$RCSfile: VdqmRequestsProducerThread.cpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <u64subr.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/tape/tapegateway/DlfCodes.hpp"
#include "castor/tape/tapegateway/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/VdqmRequestsProducerThread.hpp"
#include "castor/tape/tapegateway/VdqmTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"

  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::VdqmRequestsProducerThread::VdqmRequestsProducerThread(int port){
  m_port=port;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsProducerThread::run(void* par)
{
  std::vector<castor::tape::tapegateway::TapeGatewayRequest*> tapesToSubmit;
  std::vector<int>  vdqmReqIds;

  // connect to the db
   // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  

  if (0 == oraSvc) {    
   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR , 0, NULL);
   return;
  }
 

  try {

  // get tapes to send from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,PRODUCER_GETTING_TAPES, 0, NULL);

    tapesToSubmit = oraSvc->getTapesWithoutDriveReqs();

  } catch (castor::exception::Exception e){
    // error in getting new tape to submit

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_NO_TAPE, 2, params); 
    return;
  }

  std::vector<castor::tape::tapegateway::TapeGatewayRequest*>::iterator tapeItem =tapesToSubmit.begin();
  std::vector<castor::tape::tapegateway::TapeGatewayRequest*> submittedTapes;

  // send the tape to vdqm

  while (tapeItem != tapesToSubmit.end()){
    // query vmgr to check the tape status and to retrieve the dgn
    VmgrTapeGatewayHelper vmgrHelper;
    castor::stager::Tape* tape=NULL;
 
    if  ((*tapeItem)->accessMode() == 0)
      tape=(*tapeItem)->tapeRecall();
    else tape=(*tapeItem)->streamMigration()->tape();
    std::string dgn;
    try {
      vmgrHelper.getDataFromVmgr(*tape);
 
    } catch (castor::exception::Exception e) {
      
      castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VMGR_ERROR, 2, params);

    }
    
    if (!tape->dgn().empty()) {
      
      // tape is fine

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, PRODUCER_QUERYING_VMGR, 0, NULL);
      
      VdqmTapeGatewayHelper vdqmHelper;
      int vdqmReqId=0;
      try {
	vdqmReqId= vdqmHelper.submitTapeToVdqm(tape, tape->dgn(), m_port );
      } catch ( castor::exception::Exception e){

	castor::dlf::Param params[] =
	{castor::dlf::Param("Standard Message", sstrerror(e.code())),
	 castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VDQM_ERROR, 2, params);
	

      }
      if ( vdqmReqId  > 0 ){
	// submition went fine	
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, PRODUCER_SUBMITTING_VDQM, 0, NULL);
	(*tapeItem)->setVdqmVolReqId(vdqmReqId);
	submittedTapes.push_back(*tapeItem);
      }
      
    }
    tapeItem++;
  }
  
  try {
      
    // save vdqm ids 
      oraSvc->attachDriveReqsToTapes(submittedTapes);
    
  } catch (castor::exception::Exception e) {
    // impossible to update the information of submitted tape
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_CANNOT_UPDATE_DB, 2, params); 
  }

  // cleanUp

  tapeItem= tapesToSubmit.begin();
  while (tapeItem != tapesToSubmit.end()){
    if (*tapeItem) delete *tapeItem;
    *tapeItem=NULL;
    tapeItem++;
  }
    
  tapesToSubmit.clear();
  submittedTapes.clear();
  vdqmReqIds.clear();

}
