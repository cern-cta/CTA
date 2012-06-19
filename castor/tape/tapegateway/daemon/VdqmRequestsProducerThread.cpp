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
#include <sys/types.h>
#include <unistd.h>
#include <u64subr.h>
#include <memory>

#include <Ctape_constants.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/tape/utils/Timer.hpp"
#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"

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
// run 
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsProducerThread::run(void* /*param*/) throw() {
  // connect to the db
  castor::IService* dbSvc = 
    castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc =
    dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR ,0, NULL);
    return;
  }

  // loop over tapes that need to be handled
  std::string vid;
  int vdqmPriority;
  int mode;
  while (getTapeToHandle(oraSvc, vid, vdqmPriority, mode)) {
    // check tape in VMGR (and fail migration if not ok)
    struct TapeInfo info = checkInVMGR(oraSvc, vid, mode);
    if (0 != strcmp(info.dgnBuffer,"")) {
      // Create a VDQM request and commit to stager DB
      sendToVDQM(oraSvc, vid, info.dgnBuffer, mode, info.vmgrTapeInfo.lbltype,
                 info.vmgrTapeInfo.density, vdqmPriority);
    }
  }
}

//------------------------------------------------------------------------------
// getTapeToHandle 
//------------------------------------------------------------------------------
bool castor::tape::tapegateway::VdqmRequestsProducerThread::getTapeToHandle
(castor::tape::tapegateway::ITapeGatewaySvc* oraSvc, 
 std::string &vid,
 int &vdqmPriority,
 int &mode)
  throw() {
  castor::tape::utils::Timer timer;
  try {
    // get a tape from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_GETTING_TAPE, 0,  NULL);
    // This PL/SQL takes locks
    oraSvc->getTapeWithoutDriveReq(vid, vdqmPriority, mode);
  } catch (castor::exception::Exception& e){
    // error in getting new tape to submit
    castor::dlf::Param params[] = {
      castor::dlf::Param("errorCode",sstrerror(e.code())),
      castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_NO_TAPE, params);
    return 0;
  }
  // check whether we have something to do
  if (vid.empty()) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_NO_TAPE, 0, NULL);
    return false;
  } else {
    // We have something to do, log "found tape to submit"
    castor::dlf::Param params[] = {
      castor::dlf::Param("VID", vid),
      castor::dlf::Param("vdqmPriority", vdqmPriority),
      castor::dlf::Param("mode", mode==WRITE_ENABLE?"WRITE_ENABLE":"WRITE_DISABLE"),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_TAPE_FOUND, params);
  }
  return true;
}

//------------------------------------------------------------------------------
// checkInVMGR 
//------------------------------------------------------------------------------
castor::tape::tapegateway::TapeInfo
castor::tape::tapegateway::VdqmRequestsProducerThread::checkInVMGR
(castor::tape::tapegateway::ITapeGatewaySvc* oraSvc, 
 const std::string &vid,
 const int mode)
  throw() {
  castor::tape::utils::Timer timer;
  try {
    TapeInfo info = VmgrTapeGatewayHelper::getTapeInfo(vid, m_shuttingDown);
    castor::dlf::Param paramsVmgr[] = {
      castor::dlf::Param("TPVID", vid),
      castor::dlf::Param("dgn", info.dgnBuffer),
      castor::dlf::Param("label", info.vmgrTapeInfo.lbltype),
      castor::dlf::Param("density", info.vmgrTapeInfo.density),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    // tape is fine
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_QUERYING_VMGR,paramsVmgr);
    return info;
  } catch (castor::exception::Exception& e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Standard Message", sstrerror(e.code())),
      castor::dlf::Param("Precise Message", e.getMessage().str()),
      castor::dlf::Param("TPVID", vid),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VMGR_ERROR, params);
    if (e.code() == ENOENT || e.code() == EFAULT || e.code() == EINVAL ||
        e.code() == ETHELD || e.code() == ETABSENT || e.code() == ETARCH ) {
      try {
        // cancel the operation in the db for these error. Note that logging is done from the DB
        oraSvc->cancelMigrationOrRecall(mode, vid, e.code(), e.getMessage().str());
      } catch (castor::exception::Exception& e ){
        // impossible to update the information of submitted tape
        castor::dlf::Param params[] = {
          castor::dlf::Param("errorCode", sstrerror(e.code())),
          castor::dlf::Param("errorMessage", e.getMessage().str()),
          castor::dlf::Param("context","tape was unavailable, wanted to cancel recall/migration"),
          castor::dlf::Param("TPVID", vid)
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_CANNOT_UPDATE_DB, params);
      }
    }
    // For other errors, we leave the request untouched so that we will try again later
  }
  // case of error only
  return TapeInfo();
}

//------------------------------------------------------------------------------
// sendToVDQM 
//------------------------------------------------------------------------------
void castor::tape::tapegateway::VdqmRequestsProducerThread::sendToVDQM
(castor::tape::tapegateway::ITapeGatewaySvc* oraSvc, 
 const std::string &vid,
 const char* dgn,
 const int mode,
 const char* label,
 const char* density,
 const int vdqmPriority)
  throw() {
  VdqmTapeGatewayHelper vdqmHelper;
  castor::tape::utils::Timer timer;
  int mountTransactionId = 0;
  try {
    // connect to vdqm and submit the request
    vdqmHelper.connectToVdqm();
  } catch (castor::exception::Exception& e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("errorCode", sstrerror(e.code())),
      castor::dlf::Param("errorMessage", e.getMessage().str()),
      castor::dlf::Param("TPVID", vid),
      castor::dlf::Param("mountTransactionId", mountTransactionId),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VDQM_ERROR, params);
    return;
  }
  // we are now connected to VDQM
  try {
    mountTransactionId = vdqmHelper.createRequestForAggregator
      (vid, dgn, mode, m_port, vdqmPriority);
    // set mountTransactionId in the RecallMount and commit
    oraSvc->attachDriveReq(vid, mountTransactionId, mode, label, density);
    // confirm to vdqm
    vdqmHelper.confirmRequestToVdqm();
    // log "VdqmRequestsProducer: request submitted to vdqm successfully"
    castor::dlf::Param paramsDb[] = {
      castor::dlf::Param("TPVID", vid),
      castor::dlf::Param("mountTransactionId", mountTransactionId),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, PRODUCER_REQUEST_SUBMITTED, paramsDb);
  } catch (castor::exception::Exception& e) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("errorCode", sstrerror(e.code())),
      castor::dlf::Param("errorMessage", e.getMessage().str()),
      castor::dlf::Param("TPVID", vid),
      castor::dlf::Param("mountTransactionId", mountTransactionId),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VDQM_ERROR, params);
  }
  // in all cases, we (try to) disconnect from VDQM
  try {
    vdqmHelper.disconnectFromVdqm();
  } catch (castor::exception::Exception& e) {
    // impossible to disconnect from vdqm
    castor::dlf::Param params[] = {
      castor::dlf::Param("errorCode", sstrerror(e.code())),
      castor::dlf::Param("errorMessage", e.getMessage().str()),
      castor::dlf::Param("TPVID", vid),
      castor::dlf::Param("mountTransactionId", mountTransactionId),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PRODUCER_VDQM_ERROR,params);
  }
}
