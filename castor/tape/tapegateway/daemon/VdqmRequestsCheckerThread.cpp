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
 * Regularly checks that ongoing recalls and migrations have an associated
 * request in VDQM. Cleans up when it's not the case
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#include "Ctape_constants.h"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"
#include "castor/stager/TapeTpModeCodes.hpp"
#include "castor/tape/utils/Timer.hpp"
#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/VdqmRequestsCheckerThread.hpp"
#include "castor/tape/tapegateway/daemon/VdqmTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"

#include "castor/tape/tapegateway/ScopedTransaction.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::VdqmRequestsCheckerThread::VdqmRequestsCheckerThread
(u_signed64 timeOut) : m_timeOut(timeOut) {}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsCheckerThread::run(void*)
{
  // service to access the database
  castor::IService* dbSvc =
    castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc =
    dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }

  // get list of ongoing recall and migration requests
  std::list<castor::tape::tapegateway::ITapeGatewaySvc::TapeRequest> requests;
  castor::tape::utils::Timer timer;
  try {
    // get tapes to check from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,CHECKER_GETTING_TAPES, 0, NULL);
    // This SQL take lock on a tape gateway requests and updates them without commit.
    oraSvc->getTapesWithDriveReqs(requests, m_timeOut);
  } catch (castor::exception::Exception& e) {
    // log "VdqmRequestsChecker: no tape to check"
    castor::dlf::Param params[] = {
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str()),
      castor::dlf::Param("context", "was getting VDQM requests to be checked from the stager DB")
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, CHECKER_NO_TAPE, params);
    return;
  }
  if (requests.empty()){
    // log "VdqmRequestsChecker: no tape to check"
    castor::dlf::Param params[] = { castor::dlf::Param("ProcessingTime", timer.secs()) };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, CHECKER_NO_TAPE, params);
    return;
  } else {
    // log "VdqmRequestsChecker: tapes to check found"
    castor::dlf::Param params[] = {
      castor::dlf::Param("ProcessingTime", timer.secs()),
      castor::dlf::Param("NbVDQMRequestsToCheck", requests.size()),
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, CHECKER_TAPES_FOUND, params);
  }

  // Loop through requests and call VDQM
  std::list<int> tapesToRetry;
  std::list<std::string> tapesToReset;
  for (std::list<castor::tape::tapegateway::ITapeGatewaySvc::TapeRequest>::const_iterator
         request = requests.begin();
       request != requests.end();
       request++){
    castor::dlf::Param params[] = {
      castor::dlf::Param("mode", request->mode==WRITE_DISABLE?"Recall":"Migration"),
      castor::dlf::Param("mountTransactionId", request->mountTransactionId),
      castor::dlf::Param("TPVID", request->vid) };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, CHECKER_QUERYING_VDQM, params);
    try {
      VdqmTapeGatewayHelper::checkVdqmForRequest(request->mountTransactionId);
    } catch (castor::exception::Exception& e) {
      // log "VdqmRequestsChecker: request was lost or out of date"
      castor::dlf::Param params[] = {
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str()),
        castor::dlf::Param("mode", request->mode==WRITE_DISABLE?"Recall":"Migration"),
        castor::dlf::Param("mountTransactionId", request->mountTransactionId),
        castor::dlf::Param("TPVID", request->vid) };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, CHECKER_LOST_VDQM_REQUEST, params);
      if (e.code() == EPERM) {
        tapesToRetry.push_back(request->mountTransactionId);
        if (request->mode == WRITE_ENABLE ){
          tapesToReset.push_back(request->vid);
        }
      }
    }
  }

  // Update VMGR, releasing the busy tapes
  for (std::list<std::string>::iterator tapeToReset = tapesToReset.begin();
       tapeToReset != tapesToReset.end();
       tapeToReset++){
    try {
      VmgrTapeGatewayHelper::resetBusyTape(*tapeToReset, m_shuttingDown);
      castor::dlf::Param params[] =
        {castor::dlf::Param("VID", *tapeToReset)};
      castor::dlf::dlf_writep(nullCuuid,DLF_LVL_SYSTEM, CHECKER_RELEASING_UNUSED_TAPE, params);
    } catch (castor::exception::Exception& e){
      castor::dlf::Param params[] = {
        castor::dlf::Param("TPVID", *tapeToReset),
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, CHECKER_CANNOT_RELEASE_TAPE, params);
    }
  }

  // update the db to eventually send again some requests
  try {
    timer.reset();
    oraSvc->restartLostReqs(tapesToRetry); // autocommited
    castor::dlf::Param paramsReset[] = { castor::dlf::Param("ProcessingTime", timer.secs()) };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, CHECKER_TAPES_RESURRECTED, paramsReset);
  } catch (castor::exception::Exception& e){
    // impossible to update the information of checked tape
    castor::dlf::Param params[] = {
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, CHECKER_CANNOT_UPDATE_DB, params);
  }
}




