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

  // Get the lists of tapes to submit to VDQM for migration and recall
  // All returned Migration/RecallMounts will be locked
  std::vector<std::string> tapesForMigr;
  std::vector<std::pair<std::string, int> > tapesForRecall; 
  int nbTapes = getTapesToHandle(oraSvc, tapesForMigr, tapesForRecall);
  if (0 == nbTapes) return;

  // go through the migration candidates
  for (std::vector<std::string>::const_iterator vid = tapesForMigr.begin();
       vid != tapesForMigr.end();
       vid++) {
    // check tape in VMGR (and fail migration if not ok)
    struct TapeInfo info = checkInVMGR(oraSvc, *vid, WRITE_ENABLE);
    if (0 != strcmp(info.dgnBuffer,"")) {
      // Create a VDQM request
      sendToVDQM(oraSvc, *vid, info.dgnBuffer, WRITE_ENABLE, info.vmgrTapeInfo.lbltype,
                 info.vmgrTapeInfo.density, 0);  // hardcoded priority 0 for migrations
    }
  }

  // go through the recall candidates
  for (std::vector<std::pair<std::string, int> >::const_iterator tape = tapesForRecall.begin();
       tape != tapesForRecall.end();
       tape++) {
    // check tape in VMGR (and fail recall if not ok)
    struct TapeInfo info = checkInVMGR(oraSvc, tape->first, WRITE_DISABLE);
    if (0 != strcmp(info.dgnBuffer,"")) {
      // Create a VDQM request
      sendToVDQM(oraSvc, tape->first, info.dgnBuffer, WRITE_DISABLE, info.vmgrTapeInfo.lbltype,
                 info.vmgrTapeInfo.density, tape->second);
    }
  }

  // release the locks on the Migration/RecallMounts
  oraSvc->commit();
}

//------------------------------------------------------------------------------
// getTapesToHandle 
//------------------------------------------------------------------------------
int castor::tape::tapegateway::VdqmRequestsProducerThread::getTapesToHandle
(castor::tape::tapegateway::ITapeGatewaySvc* oraSvc, 
 std::vector<std::string> &vidsForMigr,
 std::vector<std::pair<std::string, int> > &tapesForRecall)
  throw() {
  castor::tape::utils::Timer timer;
  try {
    // get all the tapes to send from the db
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,PRODUCER_GETTING_TAPES, 0,  NULL);
    // This PL/SQL takes locks
    oraSvc->getTapeWithoutDriveReq(vidsForMigr, tapesForRecall);
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
  int nbTapes = vidsForMigr.size() + tapesForRecall.size();
  if (0 == nbTapes) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_NO_TAPE, 0, NULL);
  } else {
    // We have something to do, log "found tapes to submit"
    castor::dlf::Param params[] = {
      castor::dlf::Param("nbTapesForMigration", vidsForMigr.size()),
      castor::dlf::Param("nbTapesForRecall", tapesForRecall.size()),
      castor::dlf::Param("ProcessingTime", timer.secs())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRODUCER_TAPE_FOUND, params);
  }
  return nbTapes;
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
    mountTransactionId = vdqmHelper.createRequestForAggregator
      (vid, dgn, mode, m_port, vdqmPriority);
    // set mountTransactionId in the RecallMount
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
    try {
      // (try to) disconnect (otherwise we leak memory)
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
}
