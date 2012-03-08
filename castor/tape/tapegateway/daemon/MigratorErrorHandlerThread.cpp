/******************************************************************************
 *                      MigratorErrorHandlerThread.cpp
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
 * @(#)$RCSfile: MigratorErrorHandlerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include "castor/tape/python/python.hpp"

#include <list>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <u64subr.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Internal.hpp"

#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"

#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/MigratorErrorHandlerThread.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"
#include "castor/tape/utils/Timer.hpp"
#include "castor/tape/tapegateway/ScopedTransaction.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::MigratorErrorHandlerThread::MigratorErrorHandlerThread( PyObject* pyFunction ){
  m_pyFunction=pyFunction;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::MigratorErrorHandlerThread::run(void*)
{
  // get failed migration tapecopies
  // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);
  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0,  NULL);
    return;
  }
  // Open a scoped transaction that will rollback if any exception is thrown.
  // Explicit commits and rollbacks will be done through it also.
  // (To avoid unnecessary call to the DB)
  ScopedTransaction scpTrans (oraSvc);
  castor::tape::utils::Timer timer;

  std::list<RetryPolicyElement> mjList;
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,MIG_ERROR_GETTING_FILES, 0, NULL);
  // Find all the failed migrations (tapecopies) and lock them
  try {
    oraSvc->getFailedMigrations(mjList);
  } catch (castor::exception::Exception& e){
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,MIG_ERROR_DB_ERROR, params);
    return;
  }
  if (mjList.empty()){
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,MIG_ERROR_NO_JOB, 0, NULL);
    scpTrans.rollback();
    return;
  }
  castor::dlf::Param paramsDb[] =
    {
      castor::dlf::Param("ProcessingTime", timer.getusecs() * 0.000001)
    };
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, MIG_ERROR_JOBS_FOUND, paramsDb);

  // Sort the migrations in error into migrations to retry and to failed
  std::list<u_signed64> mjIdsToRetry;
  std::list<u_signed64> mjIdsToFail;
  std::list<RetryPolicyElement>::iterator migJob;
  for (migJob = mjList.begin(); migJob != mjList.end(); migJob++){
    //apply the policy
    castor::dlf::Param params[] =
      {
          castor::dlf::Param("migrationJobId",migJob->migrationOrRecallJobId),
          castor::dlf::Param("NSFILEID",      migJob->fileId),
          castor::dlf::Param("NSHOSTNAME",    migJob->nsHost),
          castor::dlf::Param("errorCode",     sstrerror(migJob->errorCode)),
          castor::dlf::Param("nbRetry",       migJob->nbRetry)
      };
    try {
      if ( applyRetryMigrationPolicy(*migJob)) {
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,MIG_ERROR_RETRY, params);
	mjIdsToRetry.push_back( (*migJob).migrationOrRecallJobId);
      } else {
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,MIG_ERROR_FAILED, params);
	mjIdsToFail.push_back( (*migJob).migrationOrRecallJobId);
      }
    } catch (castor::exception::Exception& e){
      castor::dlf::Param paramsEx[] = {
          castor::dlf::Param("migrationJobId",(*migJob).migrationOrRecallJobId),
          castor::dlf::Param("NSFILEID",      migJob->fileId),
          castor::dlf::Param("NSHOSTNAME",    migJob->nsHost),
          castor::dlf::Param("errorCode",     sstrerror(migJob->errorCode)),
          castor::dlf::Param("nbRetry",       migJob->nbRetry),
          castor::dlf::Param("policyErrorCode",sstrerror(e.code())),
          castor::dlf::Param("policyErrorMessage",e.getMessage().str())
      };
      // retry in case of error
      mjIdsToRetry.push_back( (*migJob).migrationOrRecallJobId);
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, MIG_ERROR_RETRY_BY_DEFAULT, paramsEx);
    }
  }

  timer.reset();
  // update the db
  try {
    // TODO This function does a commit in SQL for the moment but it should be
    // fixed (when having a single convention)
    oraSvc->setMigRetryResult(mjIdsToRetry,mjIdsToFail);
    scpTrans.commit();
    castor::dlf::Param paramsDbUpdate[] = {
      castor::dlf::Param("ProcessingTime", timer.getusecs() * 0.000001),
      castor::dlf::Param("migration jobs to retry",mjIdsToRetry.size()),
      castor::dlf::Param("migration jobs to fail",mjIdsToFail.size())
    };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, MIG_ERROR_RESULT_SAVED, paramsDbUpdate);
  } catch (castor::exception::Exception& e) {
    castor::dlf::Param params[] = {
        castor::dlf::Param("errorCode",sstrerror(e.code())),
        castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, MIG_ERROR_CANNOT_UPDATE_DB, params);
  }
}



bool castor::tape::tapegateway::MigratorErrorHandlerThread::applyRetryMigrationPolicy(const RetryPolicyElement& elem)
  throw (castor::exception::Exception ){

  // if the function is null, I allow the retry

  if (m_pyFunction == NULL)
    return true;
  
  castor::tape::python::ScopedPythonLock scopedPythonLock;

  // Create the input tuple for retry migration policy Python-function
  //
  // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
  // K must be used for unsigned (feature not documented at all but available)

  castor::tape::python::SmartPyObjectPtr inputObj(Py_BuildValue((char*)"(i,i)", (elem.errorCode), (elem.nbRetry)));
  
  // Call the retry_migration-policy Python-function
  castor::tape::python::SmartPyObjectPtr resultObj(PyObject_CallObject(m_pyFunction, inputObj.get()));

  // Throw an exception if the migration-policy Python-function call failed
  if(resultObj.get() == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() <<
      "Failed to execute migration-policy Python-function";
    throw ex;
  }

  // Return the result of the Python function
  const int resultInt = PyInt_AsLong(resultObj.get());
  return resultInt;


}
