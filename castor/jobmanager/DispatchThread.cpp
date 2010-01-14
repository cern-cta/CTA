/******************************************************************************
 *                      DispatchThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 * @(#)$RCSfile: DispatchThread.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2009/03/23 15:26:20 $ $Author: sponcec3 $
 *
 * A thread used to dispatch subrequest's in a SUBREQUEST_READYFORSCHED into
 * the scheduler using the forked process pool
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/jobmanager/DispatchThread.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/metrics/MetricsCollector.hpp"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::jobmanager::DispatchThread::DispatchThread
(castor::server::ForkedProcessPool *processPool)
  throw(castor::exception::Exception) :
  m_processPool(processPool) {}


//-----------------------------------------------------------------------------
// Init
//-----------------------------------------------------------------------------
void castor::jobmanager::DispatchThread::init() {
  // Initialize the oracle job manager service.
  castor::IService *orasvc =
    castor::BaseObject::services()->service
    ("OraJobManagerSvc", castor::SVC_ORAJOBMANAGERSVC);
  if (orasvc == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to get OraJobManagerSVC for DispatchThread";
    throw e;
  }
  m_jobManagerService =
    dynamic_cast<castor::jobmanager::IJobManagerSvc *>(orasvc);
  if (m_jobManagerService == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Could not convert newly retrieved service into "
                   << "IJobManagerSvc for DispatchThread" << std::endl;
    throw e;
  }
}


//-----------------------------------------------------------------------------
// Select
//-----------------------------------------------------------------------------
castor::IObject *castor::jobmanager::DispatchThread::select() throw() {
  try {
    castor::jobmanager::JobSubmissionRequest *result =
      m_jobManagerService->jobToSchedule();
    return result;
  } catch (castor::exception::Exception e) {

    // "Exception caught selecting a new job to schedule in
    // DispatchThread::select"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 61, 2, params);
  } catch (...) {

    // "Failed to execute jobToSchedule procedure in DispatchThread::select"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 62, 1, params);
  }
  return NULL;
}


//-----------------------------------------------------------------------------
// Process
//-----------------------------------------------------------------------------
void castor::jobmanager::DispatchThread::process(castor::IObject *param)
  throw() {

  // The only time dispatch fails is when the select() call of the process
  // pool fails or the process pool is in the process of being shut down.
  // Under these circumstances we need to restart the job otherwise it will
  // remain in a SUBREQUEST_BEINGSCHED status indefinitely!
  castor::jobmanager::JobSubmissionRequest *request =
    dynamic_cast<castor::jobmanager::JobSubmissionRequest *>(param);

  // Needed for logging purposes
  string2Cuuid(&m_requestUuid, (char *)request->reqId().c_str());
  string2Cuuid(&m_subRequestUuid, (char *)request->subReqId().c_str());
  m_fileId.fileid = request->fileId();
  strncpy(m_fileId.server, request->nsHost().c_str(), CA_MAXHOSTNAMELEN);
  m_fileId.server[CA_MAXHOSTNAMELEN] = '\0';

  // "Job received"
  castor::dlf::Param params[] =
    {castor::dlf::Param("ID", request->id()),
     castor::dlf::Param(m_subRequestUuid)};
  castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_SYSTEM, 60, 2, params, &m_fileId);

  // Update counters
  castor::metrics::MetricsCollector* mc =
    castor::metrics::MetricsCollector::getInstance();
  if(mc) {
    mc->updateHistograms(request);
  }

  try {
    m_processPool->dispatch(*param);
  } catch (castor::exception::Exception e) {

    // Attempt to restart the job for later selection and processing
    try {
      m_jobManagerService->updateSchedulerJob(request,
                                              castor::stager::SUBREQUEST_READYFORSCHED);
    } catch (castor::exception::Exception e) {

      // "Exception caught trying to restart a job in DispatchThread::process,
      // job will remain incorrectly in SUBREQUEST_BEINGSCHED"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", sstrerror(e.code())),
         castor::dlf::Param("Message", e.getMessage().str()),
         castor::dlf::Param("ID", request->id()),
         castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 63, 4, params, &m_fileId);
    } catch (...) {

      // "Failed to execute updateSchedulerJob in DispatchThread::process,
      // job will remain incorrectly in SUBREQUEST_BEINGSCHED"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", "General exception caught"),
         castor::dlf::Param("ID", request->id()),
         castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 64, 3, params, &m_fileId);
    }
  }
  delete param;
}

