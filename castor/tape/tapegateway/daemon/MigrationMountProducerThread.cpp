/******************************************************************************
 *                      MigrationMountProducerThread.cpp
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
#include "castor/tape/tapegateway/daemon/MigrationMountProducerThread.hpp"

#include "castor/tape/utils/Timer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::MigrationMountProducerThread::MigrationMountProducerThread(){
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::MigrationMountProducerThread::run(void*)
{
  // Syntactic simplification
  typedef castor::tape::tapegateway::ITapeGatewaySvc::StartMigrationMountReport
      StartMigrationMountReport;
  typedef std::vector<StartMigrationMountReport> StartMigrationMountReport_vec;
  // service to access the database
  castor::IService* dbSvc =
    castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc =
    dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);

  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }
  castor::tape::utils::Timer timer;
  StartMigrationMountReport_vec report;
  // Execute the migration mount creation procedure.
  try {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, MOUNT_PRODUCER_START, 0, NULL);
    oraSvc->startMigrationMounts(report);
  } catch (castor::exception::Exception& e) {
    // error in DB
    castor::dlf::Param params[] =
    {castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, MOUNT_PRODUCER_DB_ERROR, params);
    return;
  }

  // Push the report in DLF
  StartMigrationMountReport_vec::iterator rep;
  for (rep=report.begin(); rep < report.end(); rep++) {
    if (rep->mountsCreated) {
      // Report the migration mount creation
      if (rep->requestId > 0) {
        // In the stadard case, we get a positive (valid) mountID reported.
        castor::dlf::Param params[] = {
            castor::dlf::Param("mountTransactionId", rep->requestId),
            castor::dlf::Param("TapePool",           rep->tapepool),
            castor::dlf::Param("sizeQueued",         rep->sizeQueued),
            castor::dlf::Param("filesQueued",        rep->filesQueued),
            castor::dlf::Param("mountsBefore",       rep->mountsBefore),
            castor::dlf::Param("mountsCreated",      rep->mountsCreated),
            castor::dlf::Param("mountsAfter",        rep->mountsAfter),
            castor::dlf::Param("ProcessingTime",     timer.getusecs() * 0.000001)
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, MOUNT_PRODUCER_REPORT, params);
      } else {
        // If mount id is negative or NULL, something pathologic hapenned
        // The standard case is that we found no file to migrate in the last minute check (on DB side)
        // In the stadard case, we get a positive (valid) mountID reported.
        castor::dlf::Param params[] = {
            castor::dlf::Param("mountTransactionId", rep->requestId),
            castor::dlf::Param("TapePool",           rep->tapepool),
            castor::dlf::Param("sizeQueued",         rep->sizeQueued),
            castor::dlf::Param("filesQueued",        rep->filesQueued),
            castor::dlf::Param("mountsBefore",       rep->mountsBefore),
            castor::dlf::Param("mountsCreated",      rep->mountsCreated),
            castor::dlf::Param("mountsAfter",        rep->mountsAfter),
            castor::dlf::Param("ProcessingTime",     timer.getusecs() * 0.000001)
        };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, MOUNT_PRODUCER_REPORT_NO_FILE, params);
      }
    } else {
      // Report (debug level) the non-creation of the migartion mount
      // Report the migration mount creation
      castor::dlf::Param params[] = {
          castor::dlf::Param("mountTransactionId", rep->requestId),
          castor::dlf::Param("TapePool",           rep->tapepool),
          castor::dlf::Param("sizeQueued",         rep->sizeQueued),
          castor::dlf::Param("filesQueued",        rep->filesQueued),
          castor::dlf::Param("mountsBefore",       rep->mountsBefore),
          castor::dlf::Param("mountsCreated",      rep->mountsCreated),
          castor::dlf::Param("mountsAfter",        rep->mountsAfter),
          castor::dlf::Param("ProcessingTime",     timer.getusecs() * 0.000001)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, MOUNT_PRODUCER_REPORT_NO_ACTION, params);
    }
  }
}

