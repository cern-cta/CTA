/******************************************************************************
 *                      CancellationThread.cpp
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
 * @(#)$RCSfile: CancellationThread.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2007/10/04 07:44:42 $ $Author: waldron $
 *
 * Cancellation thread used to cancel jobs in the LSF with have been in a 
 * PENDING status for too long 
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/jobmanager/CancellationThread.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "getconfent.h"
#include "u64subr.h"
#include <sstream>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::jobmanager::CancellationThread::CancellationThread(int timeout)
  throw(castor::exception::Exception) :
  m_cleanPeriod(3600),
  m_defaultQueue("default"), 
  m_timeout(timeout), 
  m_initialized(false), 
  m_resReqKill(false) {

  // Initialize the oracle job manager service.
  castor::IService *orasvc =
    castor::BaseObject::services()->service("OraJobManagerSvc", castor::SVC_ORAJOBMANAGERSVC);
  if (orasvc == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to get OraJobManagerSVC for CancellationThread";
    throw e;
  }
  m_jobManagerService = 
    dynamic_cast<castor::jobmanager::IJobManagerSvc *>(orasvc);
  if (m_jobManagerService == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Could not convert newly retrieved service into "
		   << "IJobManagerSvc for CancellationThread" << std::endl;
    throw e;
  }
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::jobmanager::CancellationThread::run(void *param) {  

  // Initialization is required ?
  //   - Ideally it would be nice to initialise the LSF API in the constructor.
  //     However the instant that a fork() is called for daemonization the API
  //     becomes invalid.
  if (m_initialized == false) {

    // Initialize the LSF API
    //   - This doesn't actually communicate with the LSF master in any way, 
    //     it just sets up the environment for later calls
    if (lsb_init("JobManager") < 0) {
      
      // "Failed to initialize the LSF batch library (LSBLIB)"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Function", "lsb_init"),
	 castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 2, params);
    }
    
    // Determine the value of CLEAN_PERIOD as defined in lsb.params. This will
    // be used later to cleanup the notification cache.
    parameterInfo *paramInfo = lsb_parameterinfo(NULL, NULL, 0);
    if (paramInfo == NULL) {
      
      // "Failed to extract CLEAN_PERIOD and default queue values from 
      // lsb.params, using defaults"
      castor::dlf::Param params[] = 
	{castor::dlf::Param("Default", m_cleanPeriod)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 23, 1, params);
    } else {
      m_cleanPeriod  = paramInfo->cleanPeriod + (m_timeout * 2);
      m_defaultQueue = paramInfo->defaultQueues;
    }
    
    // If the frequency of the thread, its 'interval' is greater then the
    // CLEAN_PERIOD we will never catch jobs which have exited abnormally as 
    // those jobs will never appear in the LSF queues output.
    if (m_timeout > m_cleanPeriod) {
      
      // "Cancellation thread interval is greater then CLEAN_PERIOD in 
      // lsb.params, detection of abnormally EXITING jobs disabled"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Thread Interval", m_timeout),
	 castor::dlf::Param("LSF Clean Period", m_cleanPeriod)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 27, 2, params);
    }
    m_initialized = true;
  }

  // Reset the pending timeouts map containing the service class to timeout
  // value mappings.
  m_pendingTimeouts.clear();

  // Remove all entries in the processed cache whose timestamp has exceeded 
  // the CLEAN_PERIOD in lsb.params.
  for (std::map<std::string, u_signed64>::iterator it = 
	 m_processedCache.begin(); 
       it != m_processedCache.end(); ) {
    if ((m_cleanPeriod > 0) && 
	((time(NULL) - (*it).second) > (u_signed64)m_cleanPeriod)) {
      m_processedCache.erase(it++);
    } else {
      it++;
    }
  }

  // LSF doesn't have a built in mechanism to automatically remove jobs from
  // the queue if they spend too much time in a PENDING status. So we must 
  // issue an external kill command to achieve this.
  char **results;
  int  count, i;
  if (!getconfent_multi("JobManager", "PendingTimeouts", 1, &results, &count)) {
    for (i = 0; i < count; i++) {
      std::istringstream buf(results[i]);
      std::string svcclass;
      u_signed64 timeout;

      // Parse the configuration option. The supported syntax is:
      //     <svcclass>:<timeout> [svcclass:timeout2[...]]
      std::getline(buf, svcclass, ':');
      buf >> timeout;

      // If the svcclass is 'default' map it to the default queue in LSF
      if (!strcasecmp(svcclass.c_str(), "default")) {
	svcclass = m_defaultQueue;
      }

      if (buf.fail()) {
	// "Invalid JobManager/PendingTimeouts option, ignoring entry"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Value", results[i]),
	   castor::dlf::Param("Index", i + 1)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 20, 2, params);
      } else {
	m_pendingTimeouts[svcclass] = timeout;
      }
      free(results[i]);
    }
  }

  // The job manager also has the ability to terminate any job in LSF if the
  // all of the requested filesystems are no longer available. This prevents 
  // the queues from accumulating jobs that will never run. For example, when a 
  // diskserver is removed from production and never returns.
  m_resReqKill = false;
  char *value  = getconfent("JobManager", "ResReqKill", 0);
  if (value != NULL) {
    if (!strcasecmp(value, "yes")) {
      m_resReqKill = true;
    }
  }

  // First determine the status of all filesystems known by the stager. This
  // will be used to cross reference jobs requested filesystems with those
  // actually available
  try {
    m_fileSystemStates.clear();
    if (m_resReqKill)
      m_jobManagerService->fileSystemStates(m_fileSystemStates);
  } catch (castor::exception::Exception e) {
    
    // "Exception caught in trying to get filesystem states, continuing anyway"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Code", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 30, 2, params);
  } catch (...) {
    
    // "Failed to execute fileSystemStates procedure, continuing anyway"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 31, 1, params);
  }

  // Retrieve a list of all LSF jobs recently finished. This will allow us to
  // determine those jobs which have exited abnormally. As this is a call to 
  // mbatch (LSF batch master) a response cannot be guaranteed. Should a 
  // failure occur there is nothing that can be done apart from trying again 
  // later.
  jobInfoEnt *job;
  if (lsb_openjobinfo(0, NULL, "all", NULL, NULL, ALL_JOB) < 0) {
    if (lsberrno == LSBE_NO_JOB) {
      lsb_closejobinfo(); // No matching job found
    } else {
      
      // "Failed to retrieve historical LSF job information from batch master"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Function", "lsb_openjobinfo"),
	 castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 21, 2, params);
    }
  } 
  else {
    // Loop over all jobs
    while ((job = lsb_readjobinfo(NULL)) != NULL) {
      processJob(job);
    }
    lsb_closejobinfo();
  }

  // Now retrieve a list of all unfinished jobs. This will allow use to
  // determine those jobs which have timed out. Why a second call? because
  // lsb_openjobinfo() does not allow the bitwise operation ALL_JOB | CUR_JOB
  // as an option despite saying it does in the documentation, try `bjobs -ap`
  if (lsb_openjobinfo(0, NULL, "all", NULL, NULL, CUR_JOB) < 0) {
   if (lsberrno == LSBE_NO_JOB) {
      lsb_closejobinfo(); // No matching job found
    } else {
      
      // "Failed to retrieve current LSF job information from batch master"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Function", "lsb_openjobinfo"),
	 castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 22, 2, params);
    }
  } 
  else {
    // Loop over all jobs
    while ((job = lsb_readjobinfo(NULL)) != NULL) {
      processJob(job);
    }
    lsb_closejobinfo();
  }
}


//----------------------------------------------------------------------------
// processJob
//----------------------------------------------------------------------------
void castor::jobmanager::CancellationThread::processJob(jobInfoEnt *job) {

  if (!IS_PEND(job->status) && !(job->status & JOB_STAT_EXIT))
    return;
  
  // The job has been processed previously ? In this case we are waiting
  // for the job to be removed from the LSF output after CLEAN_PERIOD seconds
  std::map<std::string, u_signed64>::const_iterator iter =
    m_processedCache.find(job->submit.jobName);
  if (iter != m_processedCache.end()) {
    return;
  }    
  
  // Parse the jobs external scheduler options to extract the jobs request,
  // subrequest uuid's and nameserver invariant. This is not mandatory and is
  // only need for logging purposes and traceability.
  if (job->submit.extsched == NULL) {
    return;
  }
  
  // We split the string into key, value pairs and add it to a map. We do this
  // instead of extracting one value at a time using a specific sequence. This 
  // allows us to be more tolerant to changes in the extsched options in the 
  // future
  std::istringstream extsched(job->submit.extsched), iss;
  std::vector<std::string> rfs;
  std::string key, value, token;
  u_signed64 type = 0;
  Cuuid_t requestId = nullCuuid, subRequestId = nullCuuid;
  Cns_fileid fileId;
  memset(&fileId, '\0', sizeof(fileId));
  
  while (std::getline(extsched, token, ';')) {
    std::istringstream iss(token);
    std::getline(iss, key, '=');
    iss >> value;
    if (!iss.fail()) {
      if (key == "REQUESTID") {
	string2Cuuid(&requestId, (char *)value.c_str());
      } else if (key == "SUBREQUESTID") {
	string2Cuuid(&subRequestId, (char *)value.c_str());
      } else if (key == "FILEID") {
	fileId.fileid = strutou64(value.c_str());
      } else if (key == "NSHOST") {
	strncpy(fileId.server, value.c_str(), CA_MAXHOSTNAMELEN);
      } else if (key == "TYPE") {
	type = strutou64(value.c_str());
      } else if (key == "RFS") {
	std::istringstream requestedFileSystems(value);
	// Tokenize the requested filesystems using '|' as a delimiter
	while (std::getline(requestedFileSystems, token, '|')) {
	  rfs.push_back(token);
	}
      }
    }
  }
    
  // Check that all required parameters where extracted. If this fails we
  // assume the job was not entered by the submission daemon and ignore it.
  if (!Cuuid_compare(&requestId, &nullCuuid) ||
      !Cuuid_compare(&subRequestId, &nullCuuid) ||
      (fileId.fileid == 0) ||
      (fileId.server[0] == '\0')) {
    return;
  }
  
  // Check if the job has exited via administrative action, a bkill or
  // abnormally terminated on the host.
  if (job->status & JOB_STAT_EXIT) {
    if (!job->exitStatus) {
      if (failSubRequest(requestId, subRequestId, fileId, ESTJOBKILLED)) {
	
	// "Job terminated by service administrator"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("JobId", (int)job->jobId),
	   castor::dlf::Param("Username", job->user),
	   castor::dlf::Param("Type", castor::ObjectsIdStrings[type]),
	   castor::dlf::Param(subRequestId)};
	castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 26, 4, params, &fileId); 
      }
    } else {
      // Although we have the capacity to detect an abnormally exiting job
      // we don't respond to such a condition and rely on the mover to
      // perform the correct cleanup.
    }
    return;
  }
  
  // Check if the job is a candidate for termination (kill) if it has 
  // exceeded its maximum amount of time in the queue in a PENDING state
  std::map<std::string, u_signed64>::const_iterator it =
    m_pendingTimeouts.find(job->submit.queue);

  // If no svcclass entry can be found in the PendingTimeout option check for a
  // default signified by an "all" svcclass name.
  if (it == m_pendingTimeouts.end()) {
    it = m_pendingTimeouts.find("all");
  }

  if (it != m_pendingTimeouts.end()) {
    if ((u_signed64)(time(NULL) - job->submitTime) > (*it).second) {

      // LSF has the capacity to kill jobs on bulk (lsb_killbulkjobs). 
      // However testing has shown that this call fails to invoke the 
      // deletion hook of the scheduler plugin and results in a memory 
      // leak in the plugin. (Definately a LSF bug!)
      if (lsb_forcekilljob(job->jobId) < 0) {
	if ((lsberrno == LSBE_NO_JOB) || (lsberrno == LSBE_JOB_FINISH)) {
	  // No Matching job found, most likely killed by another daemon
	  return;
	}
	
	// "Failed to terminate LSF job"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Function", "lsb_forcekilljob"),
	   castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message"),
	   castor::dlf::Param("JobId", (int)job->jobId)};
	castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 24, 3, params, &fileId);
	return;
      } 
      if (failSubRequest(requestId, subRequestId, fileId, ESTJOBTIMEDOUT)) {
	
	// "Job terminated, timeout occurred"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("JobId", (int)job->jobId),
	   castor::dlf::Param("Username", job->user),
	   castor::dlf::Param("Type", castor::ObjectsIdStrings[type]),
	   castor::dlf::Param("Timeout", (*it).second),
	   castor::dlf::Param(subRequestId)};
	castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 25, 5, params, &fileId);
	return;
      }
    }
  }

  // Check to see if at least one of the jobs requested filesystems is still
  // available. If not we terminate the job to prevent it remaining in LSF 
  // waiting for a resource that may never return.
  if ((m_resReqKill) && (rfs.size())) {
    for (std::vector<std::string>::const_iterator it = rfs.begin();
	 it != rfs.end();
	 it++) {

      // If the diskserver:filesystem combination exists return control to the
      // callee.
      std::map<std::string, 
	castor::stager::FileSystemStatusCodes>::const_iterator iter =
	m_fileSystemStates.find(*it);
      if (iter != m_fileSystemStates.end()) {
	if (iter->second != castor::stager::FILESYSTEM_DISABLED) {
	  return;
	}
      }
    }
    // Kill the job in LSF
    if (lsb_forcekilljob(job->jobId) < 0) {
      if ((lsberrno == LSBE_NO_JOB) || (lsberrno == LSBE_JOB_FINISH)) {
        // No Matching job found, most likely killed by another daemon
        return;
      }

      // "Failed to terminate LSF job"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "lsb_forcekilljob"),
         castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message"),
         castor::dlf::Param("JobId", (int)job->jobId)};
      castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 24, 3, params, &fileId);
      return;
    }

    // Terminate the job
    failSubRequest(requestId, subRequestId, fileId, ESTNOTAVAIL);

    // "Job terminated, all requested filesystems are DISABLED"
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", (int)job->jobId),
       castor::dlf::Param("Username", job->user),
       castor::dlf::Param("Type", castor::ObjectsIdStrings[type]),
       castor::dlf::Param(subRequestId)};
    castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 32, 4, params, &fileId);
  }
}


//-----------------------------------------------------------------------------
// failSubRequest
//-----------------------------------------------------------------------------
bool castor::jobmanager::CancellationThread::failSubRequest
(Cuuid_t requestId, Cuuid_t subRequestId, Cns_fileid fileId, int errorCode) {
  
  // Fail the scheduler job
  try {
    char jobName[CUUID_STRING_LEN + 1];
    Cuuid2string(jobName, CUUID_STRING_LEN + 1, &subRequestId);

    // Flag the job as being processed even if we didn't manager to update the
    // database correctly. Failure to do so will cause the cancellation process
    // to become recursive.
    m_processedCache[jobName] = time(NULL);

    return m_jobManagerService->failSchedulerJob(jobName, errorCode, "");

  } catch (castor::exception::Exception e) {

    // "Exception caught in trying to fail subrequest"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Code", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Method", "CancellationThread::failSubRequest"),
       castor::dlf::Param(subRequestId)};
    castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 28, 4, params, &fileId);
  } catch (...) {
    
    // "Failed to execute failSchedulerJob procedure"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught"),
       castor::dlf::Param("Method", "CancellationThread::failSubRequest"),
       castor::dlf::Param(subRequestId)};
    castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 29, 3, params, &fileId);
  }
  return false;
}
