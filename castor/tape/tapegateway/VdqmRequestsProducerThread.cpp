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

#include "castor/tape/tapegateway/VdqmRequestsProducerThread.hpp"
#include "castor/tape/tapegateway/VmgrTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/VdqmTapeGatewayHelper.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Exception.hpp"

#include <u64subr.h>

 
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::VdqmRequestsProducerThread::VdqmRequestsProducerThread(castor::tape::tapegateway::ITapeGatewaySvc* svc, int port){
  m_dbSvc=svc;
  m_port=port;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsProducerThread::run(void* par)
{
  std::vector<castor::tape::tapegateway::TapeRequestState*> tapesToSubmit;
  std::vector<int>  vdqmReqIds;

  try {

  // get tapes to send from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 5, 0, NULL);

    tapesToSubmit = m_dbSvc->getTapesToSubmit();

  } catch (castor::exception::Exception e){
    // error in getting new tape to submit

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6, 2, params); 
    return;
  }

  std::vector<castor::tape::tapegateway::TapeRequestState*>::iterator tapeItem =tapesToSubmit.begin();
  std::vector<castor::tape::tapegateway::TapeRequestState*> submittedTapes;

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
      vmgrHelper.getDataFromVmgr(tape);
 
    } catch (castor::exception::Exception e) {
      // log TODO
    }
    
    if (!tape->dgn().empty()) {
      
      // tape is fine

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 8, 0, NULL);
      
      VdqmTapeGatewayHelper vdqmHelper;
      int vdqmReqId=0;
      try {
	vdqmReqId= vdqmHelper.submitTapeToVdqm(tape, tape->dgn(), m_port );
      } catch ( castor::exception::Exception e){
	//log TODO
      }
      if ( vdqmReqId  > 0 ){
	// submition went fine	
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 9, 0, NULL);
	(*tapeItem)->setVdqmVolReqId(vdqmReqId);
	submittedTapes.push_back(*tapeItem);
      }
      
    }
    tapeItem++;
  }
  
  try {
      
    // save vdqm ids 
    if (!submittedTapes.empty())
      m_dbSvc->updateSubmittedTapes(submittedTapes);
    
  } catch (castor::exception::Exception e) {
    // impossible to update the information of submitted tape
    
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 2, params); 
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
