/******************************************************************************
 *                      TapeMigrationMountLinkerThread.cpp
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
 * @(#)$RCSfile: TapeMigrationMountLinkerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/


#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <u64subr.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/TapeMigrationMountLinkerThread.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/ScopedTransaction.hpp"
#include "vdqm_api.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::TapeMigrationMountLinkerThread::TapeMigrationMountLinkerThread(){

}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::TapeMigrationMountLinkerThread::run(void*)
{
  std::list<castor::tape::tapegateway::ITapeGatewaySvc::migrationMountParameters> migrationsMountToResolve;

  // service to access the database
  castor::IService* dbSvc =
    castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc =
    dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);

  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }
  // This thread will call a locking sql procedure. The scoped transaction
  // will release the locks in case of failure
  ScopedTransaction scpTrans (oraSvc);

  timeval tvStart,tvEnd;
  gettimeofday(&tvStart, NULL);

  // get migration mounts to check from the db
  try {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_GETTING_MIGRATION_MOUNTS, 0, NULL);
    oraSvc->getMigrationMountsWithoutTapes(migrationsMountToResolve);

  } catch (castor::exception::Exception& e) {
    // error in getting new tape to submit
    castor::dlf::Param params[] =
    {castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_NO_MIGRATION_MOUNT, params);
    return;
  }

  if (migrationsMountToResolve.empty()){
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_NO_MIGRATION_MOUNT, 0, NULL);
    return;
  }

  gettimeofday(&tvEnd, NULL);
  signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param paramsTapes[] =
  {
      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_MOUNTS_FOUND, paramsTapes);

  std::list<u_signed64> MMIds;
  std::list<std::string> vids;
  std::list<int> fseqs;

  for (std::list<castor::tape::tapegateway::ITapeGatewaySvc::migrationMountParameters>::iterator item = migrationsMountToResolve.begin();
      item != migrationsMountToResolve.end();
      item++){

    // get tape from vmgr
    castor::dlf::Param paramsVmgr[] = {
        castor::dlf::Param("MigrationMountId", item->migrationMountId),

       castor::dlf::Param("TapePool", item->tapePoolName)
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,LINKER_QUERYING_VMGR, paramsVmgr);
    int lastFseq=-1;
    std::string vidToUse;

    try {
      // last Fseq is the value which should be used for the first file
      VmgrTapeGatewayHelper::getTapeForMigration(item->initialSizeToTransfer,
                                                 item->tapePoolName,
                                                 vidToUse,
                                                 lastFseq,
                                                 m_shuttingDown);

      // Validate the value received from the vmgr with the nameserver: for this given tape
      // fseq should be strictly greater than the highest referenced segment on the tape
      // to prevent overwrites.
      //
      // As a side effect, the tape will be left as BUSY (effect of getting tape from vmgr)
      // and the gateway will immediately forget about it. This will be removed and replaced
      // by a change to read only. This will maximise safety (no more attempts to write to this
      // badly tracked tape) without disrutions (no problems for reads)
      NsTapeGatewayHelper nsHelper;
      nsHelper.checkFseqForWrite (vidToUse, lastFseq);
    } catch(castor::exception::Exception& e) {
      castor::dlf::Param params[] = {
          castor::dlf::Param("MigrationMountId", item->migrationMountId),
          castor::dlf::Param("errorCode", sstrerror(e.code())),
          castor::dlf::Param("errorMessage", e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_NO_TAPE_AVAILABLE, params);

      // different errors from vmgr
      if (e.code()== ENOENT){
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_NOT_POOL, params);
        //tapepool doesn't exists anymore
        try {
          // This PL/SQL does not commit yet. Commit will happen only on global
          // completion after calling oraSvc->attachTapesToMigMounts
          // TODO: can probably be improved by adding autonomous transaction
          // in this SQL procedure.
          // Wrapper has no side-effect
          oraSvc->deleteMigrationMountWithBadTapePool(item->migrationMountId);
        } catch (castor::exception::Exception &e){
          castor::dlf::Param params[] = {
              castor::dlf::Param("MigrationMountId", item->migrationMountId),
              castor::dlf::Param("errorCode",sstrerror(e.code())),
              castor::dlf::Param("errorMessage",e.getMessage().str())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, params);
        }
      } else if (e.code() == ERTWRONGFSEQ) {
        try {
          VmgrTapeGatewayHelper::setTapeAsReadonlyAndUnbusy(vidToUse, m_shuttingDown);
        } catch (castor::exception::Exception &e) {
          castor::dlf::Param params[] = {
              castor::dlf::Param("MigrationMountId", item->migrationMountId),
              castor::dlf::Param("VID", vidToUse),
              castor::dlf::Param("TapePool", item->tapePoolName),
              castor::dlf::Param("errorCode", sstrerror(e.code())),
              castor::dlf::Param("errorMessage", e.getMessage().str())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_VMGRSETTOREADONLY_FAILED, params);
        }
        // Major problem, the vmgr told us to write on an fseq still referenced in the NS
        castor::dlf::Param params[] = {
            castor::dlf::Param("MigrationMountId", item->migrationMountId),
            castor::dlf::Param("TapePool", item->tapePoolName),
            castor::dlf::Param("errorCode", sstrerror(e.code())),
            castor::dlf::Param("errorMessage", e.getMessage().str()),
            castor::dlf::Param("VID", vidToUse)
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_CRIT, LINKER_VMGR_NS_DISCREPANCY, params);
        // Abort.
        throw e;
      }
      continue;
      // in case of errors we don't change the status from TO_BE_RESOLVED to TO_BE_SENT_TO_VDQM -- NO NEED OF WAITSPACE status
    }

    if ( !vidToUse.empty()){
      // got the tape
      castor::dlf::Param params[] = {
          castor::dlf::Param("MigrationMountId", item->migrationMountId),
          castor::dlf::Param("TPVID", vidToUse)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_LINKING_TAPE_MOUNT, params);

      MMIds.push_back(item->migrationMountId);
      vids.push_back(vidToUse);
      fseqs.push_back(lastFseq);
    }
  }
  gettimeofday(&tvStart, NULL);

  // update the db
  try {
    // To avoid a race condition with closing (the tape is unbusied before
    // the tape session is closed in the DB), we will here:
    // 1- Find the tape session using our targeted tapes
    // 2- Validate that they are gone from VDQM (to make sure we are
    //    simply cleaning up a finished session). Failing this test will
    //    an error.
    // 3- Issue an end tape session for the session standing in our way
    //   This will be logged as a warning, as the window for the race
    //   condition is thought to be impractically small in usual conditions.
    // 4- If all went well, we can now attach all the tapes to migration mounts.

    // 1- Find the blocking sessions
    std::list<castor::tape::tapegateway::ITapeGatewaySvc::blockingSessionInfo> blockingSessions;
    oraSvc->getMigrationMountReqsForVids(vids, blockingSessions);

    // 2- If any of those sessions is still active, we have a problem!
    for (std::list<castor::tape::tapegateway::ITapeGatewaySvc::blockingSessionInfo>::iterator bs = blockingSessions.begin();
        bs != blockingSessions.end(); bs++) {
      // If we successfully ping this session, we have a problem, as it is still active
      if (vdqm_PingServer(NULL, NULL, bs->vdqmReqId) >= 0) {
        castor::exception::Internal ex;
        ex.getMessage() << "Trying to link a tape already involved in an active session: vdqmrequestId="
            << bs->vdqmReqId << " TPVID=" << bs->vid;
        throw ex;
      }
    }

    // 3- Get all the sessions (now confirmed to be going away anyway)
    // out of our way. Not that this has to be an autonomous transaction
    // as we are still holding lock from getMigrationMountsWithoutTapes
    for (std::list<castor::tape::tapegateway::ITapeGatewaySvc::blockingSessionInfo>::iterator bs = blockingSessions.begin();
            bs != blockingSessions.end(); bs++) {
      oraSvc->endTapeSessionAutonomous(bs->vdqmReqId);
      castor::dlf::Param params[] = {
          castor::dlf::Param("TPVID", bs->vid),
          castor::dlf::Param("mountTransactionId", bs->vdqmReqId)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, LINKER_SESSION_IN_THE_WAY, params);
    }

    // 4-All checks went well, we have a set of tapes to attach.
    // This is where the commit happens (finally)
    oraSvc->attachTapesToMigMounts(MMIds, vids, fseqs);
  } catch (castor::exception::Exception &e){
    castor::dlf::Param params[] =  {
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, params);

    for (std::list<std::string>::iterator vidItem = vids.begin();
        vidItem != vids.end();
        vidItem++) {
      // release the tape
      castor::dlf::Param params[] = {castor::dlf::Param("TPVID", *vidItem)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_RELEASED_BUSY_TAPE, params);
      try {
        VmgrTapeGatewayHelper::resetBusyTape(*vidItem, m_shuttingDown);
      } catch (castor::exception::Exception& e){
        castor::dlf::Param params[] = {
            castor::dlf::Param("TPVID", *vidItem),
            castor::dlf::Param("errorCode",sstrerror(e.code())),
            castor::dlf::Param("errorMessage",e.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_RELEASE_TAPE, params);
      }
    }
  }

  gettimeofday(&tvEnd, NULL);
  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param paramsAttached[] =
    { castor::dlf::Param("ProcessingTime", procTime * 0.000001) };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_TAPES_ATTACHED, paramsAttached);
}

