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
 * @(#)$RCSfile: VdqmRequestsProducerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <u64subr.h>
#include <memory>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/VdqmTapeGatewayRequest.hpp"

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/VdqmRequestsProducerThread.hpp"
#include "castor/tape/tapegateway/daemon/VdqmTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"

#include "castor/tape/tapegateway/ScopedTransaction.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::VdqmRequestsProducerThread::VdqmRequestsProducerThread(int port){
  m_port=port;
}



//------------------------------------------------------------------------------
// select 
//------------------------------------------------------------------------------


castor::IObject* castor::tape::tapegateway::VdqmRequestsProducerThread::select() throw(){
  // connect to the db
   // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);


  if (0 == oraSvc) {
   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR ,0, NULL);
   return NULL;
  }
  // This thread will call a locking sql procedure. The scoped transaction
  // will release the locks in case of failure
  ScopedTransaction scpTrans (oraSvc);

  castor::tape::tapegateway::VdqmTapeGatewayRequest* request= new  castor::tape::tapegateway::VdqmTapeGatewayRequest();

  timeval tvStart,tvEnd;
  gettimeofday(&tvStart, NULL);

  try {

  // get all the tapes to send from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,PRODUCER_GETTING_TAPE, 0,  NULL);
    // This PL/SQL takes locks
    oraSvc->getTapeWithoutDriveReq(*request);
  } catch (castor::exception::Exception& e){
    // error in getting new tape to submit

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_NO_TAPE, params);
    if (request) delete request;
    return NULL;
  }

  if (request->vid().empty()) {
    
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_NO_TAPE, 0, NULL);
    delete request;
    return NULL;
    
  }

  gettimeofday(&tvEnd, NULL);
  signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param params[] =
    {
      castor::dlf::Param("TPVID", request->vid()),
      castor::dlf::Param("mode", request->accessMode()),
      castor::dlf::Param("request id", request->taperequest()),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
    };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_TAPE_FOUND,params);

  // On success, disengage the safety mechanism to not rollback.
  scpTrans.release();
  return request;
}


//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsProducerThread::process(castor::IObject* par)throw()
    {

  // connect to the db
  // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);

  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }
  // This thread is called with an existing dangling lock. The scoped transaction
  // will release the locks in case of failure
  ScopedTransaction scpTrans (oraSvc);

  std::auto_ptr<castor::tape::tapegateway::VdqmTapeGatewayRequest> request (dynamic_cast<castor::tape::tapegateway::VdqmTapeGatewayRequest*>(par));

  // query vmgr to check the tape status and to retrieve the dgn
  VmgrTapeGatewayHelper vmgrHelper;

  // query vmgr to check the tape status and to retrieve the dgn
  castor::stager::Tape tape;
  tape.setVid(request->vid());
  tape.setTpmode(request->accessMode());
  tape.setSide(0); //hardcoded

  timeval tvStart,tvEnd;
  gettimeofday(&tvStart, NULL);

  try {

    vmgrHelper.getDataFromVmgr(tape, m_shuttingDown);
  } catch (castor::exception::Exception& e) {

    try {
      if ( e.code() == ENOENT ||  
          e.code() == EFAULT ||
          e.code() == EINVAL ||
          e.code() == ETHELD ||
          e.code() == ETABSENT ||
          e.code() == ETARCH ) {
        // cancell the operation in the db for this error
        // tape is in a bad status, no need to un-BUSY it
        // SQL's wrapper is straightforward,
        // SQL deletes but does not commit.
        // Commit postponed to before the return.
        oraSvc->deleteTapeRequest(request->taperequest());

      } else {
        // This is indeed a commit (TODO should probably be a rollback, as usual, to check)
        oraSvc->endTransaction(); // try again to submit this tape later
        scpTrans.release();
      }

      gettimeofday(&tvEnd, NULL);
      signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
          castor::dlf::Param("Precise Message", e.getMessage().str()),
          castor::dlf::Param("TPVID",tape.vid()),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VMGR_ERROR, params);

    } catch (castor::exception::Exception& e ){
      // impossible to update the information of submitted tape

      castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str()),
          castor::dlf::Param("TPVID", tape.vid())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_CANNOT_UPDATE_DB, params);	    
    }
    // Commit for non-commiting deleteTapeRequest
    scpTrans.commit();
    return;
  }

  gettimeofday(&tvEnd, NULL);
  signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param paramsVmgr[] =
  {
      castor::dlf::Param("TPVID",tape.vid()),
      castor::dlf::Param("dgn",tape.dgn()),
      castor::dlf::Param("label",tape.label()),
      castor::dlf::Param("density",tape.density()),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };

  // tape is fine
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_QUERYING_VMGR,paramsVmgr);

  VdqmTapeGatewayHelper vdqmHelper;
  int vdqmReqId=0;

  gettimeofday(&tvStart, NULL);

  try {
    // connect to vdqm
    vdqmHelper.connectToVdqm();

    try {
      // submit the request to vdqm
      vdqmReqId=vdqmHelper.createRequestForAggregator(tape, m_port );
      request->setMountTransactionId(vdqmReqId);

      gettimeofday(&tvEnd, NULL);
      signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsVdqm[] =
      {
          castor::dlf::Param("TPVID",tape.vid()),
          castor::dlf::Param("port",m_port),
          castor::dlf::Param("mountTransactionId",vdqmReqId),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };

      // submition went fine
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_SUBMITTING_VDQM, paramsVdqm);
      gettimeofday(&tvStart, NULL);

      try {
        // save it to the db (this one does commit)
        oraSvc->attachDriveReqToTape(*request,tape);
        scpTrans.release();

        gettimeofday(&tvEnd, NULL);
        procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

        castor::dlf::Param paramsDb[] =
        {
            castor::dlf::Param("TPVID",tape.vid()),
            castor::dlf::Param("mountTransactionId",request->mountTransactionId()),
            castor::dlf::Param("request id", request->taperequest()),
            castor::dlf::Param("ProcessingTime", procTime * 0.000001)
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, PRODUCER_REQUEST_SAVED, paramsDb);

        // confirm to vdqm
        try {
          vdqmHelper.confirmRequestToVdqm();
        } catch (castor::exception::Exception& e) {
          castor::dlf::Param params[] =
          {castor::dlf::Param("errorCode",sstrerror(e.code())),
              castor::dlf::Param("errorMessage",e.getMessage().str()),
              castor::dlf::Param("TPVID", tape.vid()),
              castor::dlf::Param("mountTransactionId",vdqmReqId)
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VDQM_ERROR, params);
        }
      } catch (castor::exception::Exception& e) {
        // impossible to update the information of submitted tape

        castor::dlf::Param params[] =
        {castor::dlf::Param("errorCode",sstrerror(e.code())),
            castor::dlf::Param("errorMessage",e.getMessage().str()),
            castor::dlf::Param("TPVID", tape.vid()),
            castor::dlf::Param("mountTransactionId",vdqmReqId)
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_CANNOT_UPDATE_DB, params);
      }
    } catch (castor::exception::Exception& e) {
      // impossible to submit the tape
      vdqmHelper.disconnectFromVdqm();
      throw e;
    }
    // in all cases we disconnect (otherwise we leak memory)
    vdqmHelper.disconnectFromVdqm();

  } catch (castor::exception::Exception& e) {
    // impossible to connect/disconnect to/from vdqm
    castor::dlf::Param params[] =
    {castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str()),
        castor::dlf::Param("TPVID", tape.vid()),
        castor::dlf::Param("mountTransactionId",vdqmReqId)
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VDQM_ERROR,params);

    try {
      oraSvc->endTransaction();
      scpTrans.release();
    } catch (castor::exception::Exception& e) {
      castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
          castor::dlf::Param("errorMessage",e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_CANNOT_UPDATE_DB, params);
    }
  }
}



