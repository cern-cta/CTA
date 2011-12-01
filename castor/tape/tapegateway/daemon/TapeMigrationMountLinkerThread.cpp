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
  std::list<castor::tape::tapegateway::ITapeGatewaySvc::migrationMountParameters> migrationMountToResolve;

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
    oraSvc->getMigrationMountsWithoutTapes(migrationMountToResolve);

  } catch (castor::exception::Exception& e) {
    // error in getting new tape to submit
    castor::dlf::Param params[] =
    {castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_NO_MIGRATION_MOUNT, params);
    return;
  }

  if (migrationMountToResolve.empty()){
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

  // ask tapes to vmgr
  VmgrTapeGatewayHelper vmgrHelper;

  std::list<u_signed64> MMIds;
  std::list<std::string> vids;
  std::list<int> fseqs;
  std::list<castor::stager::Tape> tapesUsed;

  for (std::list<castor::tape::tapegateway::ITapeGatewaySvc::migrationMountParameters>::iterator item = migrationMountToResolve.begin();
      item != migrationMountToResolve.end();
      item++){

    // get tape from vmgr
    castor::dlf::Param paramsVmgr[] =
      {castor::dlf::Param("MigationMountId", item->migrationMountId),
       castor::dlf::Param("TapePool", item->tapePoolName)
      };

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,LINKER_QUERYING_VMGR, paramsVmgr);
    int lastFseq=-1;
    castor::stager::Tape tapeToUse;

    try {
      // last Fseq is the value which should be used for the first file
      vmgrHelper.getTapeForMigration(item->initialSizeToTransfer, item->tapePoolName, lastFseq, tapeToUse, m_shuttingDown);

      // Validate the value received from the vmgr with the nameserver: for this given tape
      // fseq should be strictly greater than the highest referenced segment on the tape
      // to prevent overwrites.
      //
      // As a side effect, the tape will be left as BUSY (effect of getting tape from vmgr)
      // and the gateway will immediately forget about it. This will be removed and replaced
      // by a change to read only. This will maximise safety (no more attempts to write to this
      // badly tracked tape) without disrutions (no problems for reads)
      NsTapeGatewayHelper nsHelper;
      nsHelper.checkFseqForWrite (tapeToUse.vid(), tapeToUse.side(), lastFseq);
    } catch(castor::exception::Exception& e) {
      castor::dlf::Param params[] = {
          castor::dlf::Param("MigationMountId", item->migrationMountId),
          castor::dlf::Param("errorCode", sstrerror(e.code())),
          castor::dlf::Param("errorMessage", e.getMessage().str())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, LINKER_NO_TAPE_AVAILABLE, params);

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
        } catch (castor::exception::Exception e){
          castor::dlf::Param params[] = {
              castor::dlf::Param("errorCode",sstrerror(e.code())),
              castor::dlf::Param("errorMessage",e.getMessage().str())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, params);
        }
      } else if (e.code() == ERTWRONGFSEQ) {
        try {
          vmgrHelper.setTapeAsReadonlyAndUnbusy(tapeToUse, m_shuttingDown);
        } catch (castor::exception::Exception e) {
          castor::dlf::Param params[] = {
              castor::dlf::Param("VID", tapeToUse.vid()),
              castor::dlf::Param("TapePool", item->tapePoolName),
              castor::dlf::Param("errorCode", sstrerror(e.code())),
              castor::dlf::Param("errorMessage", e.getMessage().str())
          };
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_VMGRSETTOREADONLY_FAILED, params);
        }
        // Major problem, the vmgr told us to write on an fseq still referenced in the NS
        castor::dlf::Param params[] = {
            castor::dlf::Param("MigationMountId", item->migrationMountId),
            castor::dlf::Param("TapePool", item->tapePoolName),
            castor::dlf::Param("errorCode", sstrerror(e.code())),
            castor::dlf::Param("errorMessage", e.getMessage().str()),
            castor::dlf::Param("VID", tapeToUse.vid())
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_CRIT, LINKER_VMGR_NS_DISCREPANCY, params);
        // Abort.
        throw e;
      }
      continue;
      // in case of errors we don't change the status from TO_BE_RESOLVED to TO_BE_SENT_TO_VDQM -- NO NEED OF WAITSPACE status
    }

    if ( !tapeToUse.vid().empty()){
      // got the tape
      castor::dlf::Param params[] = {
          castor::dlf::Param("MigationMountId", item->migrationMountId),
          castor::dlf::Param("TPVID", tapeToUse.vid())
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, LINKER_LINKING_TAPE_MOUNT, params);

      MMIds.push_back(item->migrationMountId);
      vids.push_back(tapeToUse.vid());
      fseqs.push_back(lastFseq);
      tapesUsed.push_back(tapeToUse);
    }
  }
  gettimeofday(&tvStart, NULL);

  // update the db
  try {
    // This is where the commit happens (finally)
    oraSvc->attachTapesToMigMounts(MMIds, vids, fseqs);
  } catch (castor::exception::Exception e){
    castor::dlf::Param params[] =  {
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, LINKER_CANNOT_UPDATE_DB, params);

    for (std::list<castor::stager::Tape>::iterator tapeItem=tapesUsed.begin();
        tapeItem!=tapesUsed.end();
        tapeItem++) {
      // release the tape
      castor::dlf::Param params[] = {castor::dlf::Param("TPVID",(*tapeItem).vid())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_RELEASED_BUSY_TAPE, params);
      try {
        vmgrHelper.resetBusyTape(*tapeItem, m_shuttingDown);
      } catch (castor::exception::Exception& e){
        castor::dlf::Param params[] = {
            castor::dlf::Param("TPVID",(*tapeItem).vid()),
            castor::dlf::Param("errorCode",sstrerror(e.code())),
            castor::dlf::Param("errorMessage",e.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, WORKER_CANNOT_RELEASE_TAPE, params);
      }
    }
  }

  gettimeofday(&tvEnd, NULL);
  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

  castor::dlf::Param paramsAttached[] =
    { castor::dlf::Param("ProcessingTime", procTime * 0.000001) };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, LINKER_TAPES_ATTACHED, paramsAttached);
}

