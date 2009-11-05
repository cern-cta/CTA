/******************************************************************************
 *                      ManagementThread.cpp
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
 * @(#)$RCSfile: ManagementThread.cpp,v $ $Revision: 1.14 $ $Release$ $Date: 2009/03/25 13:23:30 $ $Author: waldron $
 *
 * Cancellation thread used to cancel jobs in the LSF with have been in a
 * PENDING status for too long
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/jobmanager/ManagementThread.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "getconfent.h"
#include "u64subr.h"
#include <sstream>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::jobmanager::ManagementThread::ManagementThread(int timeout)
  throw(castor::exception::Exception) :
  m_cleanPeriod(3600),
  m_defaultQueue("castor"),
  m_timeout(timeout),
  m_initialized(false),
  m_resReqKill(true),
  m_diskCopyPendingTimeout(0) {}

//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::jobmanager::ManagementThread::init() {
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
// Run
//-----------------------------------------------------------------------------
void castor::jobmanager::ManagementThread::run(void *param) {

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

      // "Failed to extract CLEAN_PERIOD and DEFAULT_QUEUE values from
      // lsb.params, using defaults"
      castor::dlf::Param params[] =
	{castor::dlf::Param("CleanPeriod", m_cleanPeriod),
	 castor::dlf::Param("DefaultQueue", m_defaultQueue)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 23, 2, params);
    } else {
      m_cleanPeriod  = paramInfo->cleanPeriod + (m_timeout * 2);
      m_defaultQueue = paramInfo->defaultQueues;
    }

    // If the frequency of the thread, its 'interval' is greater than the
    // CLEAN_PERIOD we will never catch jobs which have exited abnormally as
    // those jobs will never appear in the LSF queues output.
    if (m_timeout > m_cleanPeriod) {

      // "Cancellation thread interval is greater than CLEAN_PERIOD in
      // lsb.params, detection of abnormally EXITING jobs disabled"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Thread Interval", m_timeout),
	 castor::dlf::Param("LSF Clean Period", m_cleanPeriod)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 27, 2, params);
    }
    m_initialized = true;
  }

  // Reset configuration variables
  m_pendingTimeouts.clear();
  for (std::map<std::string,
	 castor::jobmanager::DiskServerResource *>::const_iterator it =
	 m_schedulerResources.begin();
       it != m_schedulerResources.end();
       it++) {
    DiskServerResource *ds = (*it).second;
    delete ds;
  }
  m_schedulerResources.clear();
  m_svcClassesWithNoSpace.clear();

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
      std::istringstream iss(results[i]);
      std::string svcclass;
      u_signed64 timeout;

      // Extract the service class and timeout value from the configuration
      // option.
      std::getline(iss, svcclass, ':');
      iss >> timeout;

      if (iss.fail()) {
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
    free(results);
  }

  // Extract the DiskCopyPendingTimeout value which defines how long a disk
  // copy transfer can remain pending in LSF.
  m_diskCopyPendingTimeout = 0;
  char *value = getconfent("JobManager", "DiskCopyPendingTimeout", 0);
  if (value != NULL) {
    m_diskCopyPendingTimeout = std::strtol(value, 0, 10);
  }

  // The job manager also has the ability to terminate any job in LSF if the
  // all of the requested filesystems are no longer available. This prevents
  // the queues from accumulating jobs that will never run. For example, when
  // a diskserver is removed from production and never returns.
  m_resReqKill = true;
  if ((value = getconfent("JobManager", "ResReqKill", 0)) != NULL) {
    if (!strcasecmp(value, "no")) {
      m_resReqKill = false;
    }
  }

  // Extract the available scheduler resources from the stager database.
  try {
    if (m_resReqKill) {
      m_schedulerResources = m_jobManagerService->getSchedulerResources();
    }
  } catch (castor::exception::Exception e) {

    // "Exception caught trying to get scheduler resources, continuing anyway"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 30, 2, params);
  } catch (...) {

    // "Failed to execute getSchedulerResources procedure, continuing anyway"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 31, 1, params);
  }

  // Extract the list of diskonly service classes which no longer have any
  // available space from the stager database.
  try {
    if (m_resReqKill) {
      m_svcClassesWithNoSpace = m_jobManagerService->getSvcClassesWithNoSpace();
    }
  } catch (castor::exception::Exception e) {

    // "Exception caught trying get space availability of all diskonly service
    // classes, continuing anyway"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 37, 2, params);
  } catch (...) {

    // "Failed to execute getSvcClassesWithNoSpace, continuing anyway"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 38, 1, params);
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


//-----------------------------------------------------------------------------
// ProcessJob
//-----------------------------------------------------------------------------
void castor::jobmanager::ManagementThread::processJob(jobInfoEnt *job) {

  // The job has been processed previously ? In this case we are waiting
  // for the job to be removed from the LSF output after CLEAN_PERIOD seconds
  std::map<std::string, u_signed64>::const_iterator iter =
    m_processedCache.find(job->submit.jobName);
  if (iter != m_processedCache.end()) {
    return;
  }

  // Parse the jobs external scheduler options to extract the needed key value
  // pairs to process the job further.
  if (job->submit.extsched == NULL) {
    return;
  }

  // Logging variables
  std::istringstream extsched(job->submit.extsched);
  std::string buf, key, value;
  std::vector<std::string> rfs, excludedHosts;
  u_signed64 requestType;

  Cuuid_t requestId = nullCuuid, subRequestId = nullCuuid;
  Cns_fileid fileId;
  memset(&fileId, 0, sizeof(fileId));

  while (std::getline(extsched, buf, ';')) {
    std::istringstream iss(buf);
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
	fileId.server[CA_MAXHOSTNAMELEN] = '\0';
      } else if (key == "TYPE") {
	requestType = strtou64(value.c_str());
      } else if (key == "RFS") {
	// Tokenize the requested filesystems using '|' as a delimiter
	std::istringstream requestedFileSystems(value);
	while (std::getline(requestedFileSystems, buf, '|')) {
	  rfs.push_back(buf);
	}
      } else if (key == "EXCLUDEDHOSTS") {
	// Tokenize the excluded hosts using '|' as a delimiter
	std::istringstream excludedHostsInput(value);
	while (std::getline(excludedHostsInput, buf, '|')) {
	  excludedHosts.push_back(buf);
	}

	// Add the source diskserver to the exclusion list
	if (rfs.size()) {
	  std::istringstream iss(rfs[0]);
	  std::getline(iss, buf, ':');
	  excludedHosts.push_back(buf);
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

  // Determine the name of the service class being used.
  std::string svcClass = job->submit.queue;
  if (!strcasecmp(job->submit.queue, m_defaultQueue.c_str())) {
    svcClass = "default";
  }

  // "Invoking ProcessJob"
  castor::dlf::Param params2[] =
    {castor::dlf::Param("JobId", (int)job->jobId),
     castor::dlf::Param("Status", job->status),
     castor::dlf::Param(subRequestId)};
  castor::dlf::dlf_writep(requestId, DLF_LVL_DEBUG, 71, 3, params2, &fileId);

  // Processing for jobs which have finished
  if (IS_FINISH(job->status)) {

    // Common logging parameters
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", (int)job->jobId),
       castor::dlf::Param("Username", job->user),
       castor::dlf::Param("Type", castor::ObjectsIdStrings[requestType]),
       castor::dlf::Param("Status", job->status),
       castor::dlf::Param(subRequestId)};

    // Check for abnormal termination caused by administrative action such
    // as a bkill or abnormal termination on the remote execution host.
    if ((job->status & JOB_STAT_EXIT) && !(job->status & JOB_STAT_DONE) &&
	!(job->status & JOB_STAT_PDONE)) {
      if (terminateRequest(0, requestId, subRequestId, fileId, ESTJOBKILLED)) {
	// "Job terminated by service administrator"
	castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 26, 5, params, &fileId);
      }
    }

    // Run the post checks after a job has ended. We do this to catch
    // situations were LSF has attempted to run the job but failed. As a
    // consequence of this we are left with inconsistencies in the stager
    // database such as SUBREQUEST's stuck in a SUBREQUEST_READY state.
    // Note: the name of the method being called here is misleading, we don't
    // want to terminate the request in this particular case just check it
    // went through ok
    else if (terminateRequest(0, requestId, subRequestId, fileId, ESTSCHEDERR)) {
      // "Job terminated due to scheduling error"
      castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 36, 5, params, &fileId);
    }

    m_processedCache[job->submit.jobName] = time(NULL);
    return;
  }

  // No furthur processing is required unless the job is in a PEND'ing state
  if (!IS_PEND(job->status)) {
    return;
  }

  // Check if the job is a candidate for termination (kill) if it has
  // exceeded its maximum amount of time in the queue in a PENDING state
  u_signed64 timeout = 0;
  if (requestType != OBJ_StageDiskCopyReplicaRequest) {
    std::map<std::string, u_signed64>::const_iterator it =
      m_pendingTimeouts.find(svcClass);

    // If no svcclass entry can be found in the PendingTimeout option check for
    // a default signified by an "all" svcclass name.
    if (it == m_pendingTimeouts.end()) {
      it = m_pendingTimeouts.find("all");
    }
    if (it != m_pendingTimeouts.end()) {
      timeout = (*it).second;
    }
  } else {
    timeout = m_diskCopyPendingTimeout;
  }

  // A timeout was defined ?
  if (timeout) {
    if ((u_signed64)(time(NULL) - job->submitTime) > timeout) {
      if (terminateRequest(job->jobId, requestId, subRequestId, fileId, ESTJOBTIMEDOUT)) {

	// "Job terminated, timeout occurred"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("JobId", (int)job->jobId),
	   castor::dlf::Param("Username", job->user),
	   castor::dlf::Param("Type", castor::ObjectsIdStrings[requestType]),
	   castor::dlf::Param("Timeout", timeout),
	   castor::dlf::Param("SvcClass", svcClass),
	   castor::dlf::Param(subRequestId)};
	castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 25, 6, params, &fileId);
      }
      m_processedCache[job->submit.jobName] = time(NULL);
      return;
    }
  }

  // Terminate the job if the target service classes is a diskonly service class
  // which no longer has any space available.
  if ((m_resReqKill) &&
      ((requestType == OBJ_StageDiskCopyReplicaRequest) ||
       (requestType == OBJ_StagePutRequest) ||
       (requestType == OBJ_StageUpdateRequest))) {
    std::vector<std::string>::const_iterator it =
      std::find(m_svcClassesWithNoSpace.begin(),
		m_svcClassesWithNoSpace.end(), svcClass);
    if (it != m_svcClassesWithNoSpace.end()) {
      if (terminateRequest(job->jobId, requestId, subRequestId, fileId, ENOSPC)) {
	// "Job terminated, svcclass no longer has any space available"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("JobId", (int)job->jobId),
	   castor::dlf::Param("Username", job->user),
	   castor::dlf::Param("Type", castor::ObjectsIdStrings[requestType]),
	   castor::dlf::Param("SvcClass", svcClass),
	   castor::dlf::Param(subRequestId)};
	castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 39, 5, params, &fileId);
      }
      m_processedCache[job->submit.jobName] = time(NULL);
      return;
    }
  }

  // All further processing requires the scheduler resources to be defined.
  if (!m_schedulerResources.size() || !m_resReqKill) {
    return;
  }

  // Check to see if at least one of the jobs requested filesystems is still
  // available. If not we terminate the job too prevent it remaining in LSF
  // waiting for a resource that may never return.
  if (rfs.size()) {

    // Look for the diskserver and filesystem in the scheduler resources map.
    unsigned long disabled = 0;
    for (std::vector<std::string>::const_iterator it = rfs.begin();
	 it != rfs.end();
	 it++) {
      std::istringstream iss(*it);
      std::string diskServer, fileSystem;
      std::getline(iss, diskServer, ':');
      std::getline(iss, fileSystem);

      // This should never fail!! If it does then the submission was in error
      // or someone modified the jobs submission criteria
      if (iss.fail()) {
	continue;
      }

      // Diskserver and or requested filesystem is disabled ?
      std::map<std::string,
	castor::jobmanager::DiskServerResource *>::const_iterator it =
	m_schedulerResources.find(diskServer);
      if (it != m_schedulerResources.end()) {
	DiskServerResource *ds = (*it).second;
	for (unsigned int i = 0; i < ds->fileSystems().size(); i++) {
	  FileSystemResource *fs = ds->fileSystems()[i];
	  if (ds->status() == castor::stager::DISKSERVER_DISABLED) {
	    disabled = rfs.size();
	    break;
	  } else if (fs->mountPoint() != fileSystem) {
	    continue;
	  } else if (requestType == OBJ_StageDiskCopyReplicaRequest) {
	    if (fs->status() == castor::stager::FILESYSTEM_DISABLED) {
	      disabled++;
	    }
	  } else if ((fs->status() != castor::stager::FILESYSTEM_PRODUCTION) ||
		     (ds->status() != castor::stager::DISKSERVER_PRODUCTION )){
	    disabled++;
	  }
	}
      }
    }

    // Termination of the job is required ?
    if (disabled == rfs.size()) {
      if (terminateRequest(job->jobId, requestId, subRequestId, fileId, ESTNOTAVAIL)) {
	if (requestType != OBJ_StageDiskCopyReplicaRequest) {
	  // "Job terminated, all requested filesystems are DRAINING or
	  // DISABLED"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("JobId", (int)job->jobId),
	     castor::dlf::Param("Username", job->user),
	     castor::dlf::Param("Type", castor::ObjectsIdStrings[requestType]),
	     castor::dlf::Param("SvcClass", svcClass),
	     castor::dlf::Param(subRequestId)};
	  castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 32, 5, params, &fileId);
	} else {
	  // "Job terminated, source filesystem for disk2disk copy is
	  // DISABLED, restarting SubRequest"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("JobId", (int)job->jobId),
	     castor::dlf::Param("Username", job->user),
	     castor::dlf::Param("Type", castor::ObjectsIdStrings[requestType]),
	     castor::dlf::Param("RequestedFileSystem", rfs[0]),
	     castor::dlf::Param("SvcClass", svcClass),
	     castor::dlf::Param(subRequestId)};
	  castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 33, 6, params, &fileId);
	}
      }
      m_processedCache[job->submit.jobName] = time(NULL);
      return;
    }
  }

  // Handle cases whereby the diskpool/svcclass has no enabled filesystems
  // for requests which do not force a set of requested filesystems. e.g. PUTs
  if ((!rfs.size()) || (requestType == OBJ_StageDiskCopyReplicaRequest)) {

    // If we don't find any filesystems belonging to the svclass in PRODUCTION
    // fail the request
    bool terminateJob = true;
    for (std::map<std::string,
	   castor::jobmanager::DiskServerResource *>::const_iterator it =
	   m_schedulerResources.begin();
	 it != m_schedulerResources.end();
	 it++) {
      DiskServerResource *ds = (*it).second;
      for (unsigned int i = 0; i < ds->fileSystems().size(); i++) {
	FileSystemResource *fs = ds->fileSystems()[i];
	if ((ds->status() == castor::stager::DISKSERVER_PRODUCTION) &&
	    (fs->status() == castor::stager::FILESYSTEM_PRODUCTION) &&
	    (fs->svcClassName() == svcClass)) {
	  terminateJob = false;
	  break;
	}
      }
    }

    if (terminateJob) {
      if (terminateRequest(job->jobId, requestId, subRequestId, fileId, ESTSVCCLASSNOFS)) {
	// "Job terminated, svcclass has no filesystems in PRODUCTION"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("JobId", (int)job->jobId),
	   castor::dlf::Param("Username", job->user),
	   castor::dlf::Param("Type", castor::ObjectsIdStrings[requestType]),
	   castor::dlf::Param("SvcClass", svcClass),
	   castor::dlf::Param(subRequestId)};
	castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 34, 5, params, &fileId);
      }
      m_processedCache[job->submit.jobName] = time(NULL);
      return;
    }
  }

  // Handle cases where all diskservers in a service class for a
  // StageDiskCopyReplicaRequest are excluded as candidate hosts to run the job
  // as they already contain a copy of the file. The most common reason for
  // this is a misconfigured service class where the number of diskservers
  // is less than the maxReplicaNb
  if (requestType == OBJ_StageDiskCopyReplicaRequest) {

    unsigned long excluded = 0, total = 0;
    for (std::map<std::string,
	   castor::jobmanager::DiskServerResource *>::const_iterator it =
	   m_schedulerResources.begin();
	 it != m_schedulerResources.end();
	 it++) {
      DiskServerResource *ds = (*it).second;
      for (unsigned int i = 0; i < ds->fileSystems().size(); i++) {
	FileSystemResource *fs = ds->fileSystems()[i];
	if (fs->svcClassName() != svcClass) {
	  continue;
	}

	// Check to see if the diskserver is in the exclusion host list
	std::vector<std::string>::const_iterator it2 =
	  std::find(excludedHosts.begin(),
		    excludedHosts.end(), ds->diskServerName());
       	if (it2 != excludedHosts.end()) {
	  excluded++;
	}
	total++;
	break;
      }
    }

    // Terminate the job if all diskservers in the service class are excluded
    if (total == excluded) {
      if (terminateRequest(job->jobId, requestId, subRequestId, fileId, ESTSCHEDERR)) {
	// "Job terminated, not enough hosts to meet jobs requirements"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("JobId", (int)job->jobId),
	   castor::dlf::Param("Username", job->user),
	   castor::dlf::Param("Type", castor::ObjectsIdStrings[requestType]),
	   castor::dlf::Param("SvcClass", svcClass),
	   castor::dlf::Param(subRequestId)};
	castor::dlf::dlf_writep(requestId, DLF_LVL_WARNING, 70, 5, params, &fileId);
      }
      m_processedCache[job->submit.jobName] = time(NULL);
      return;
    }
  }
}


//-----------------------------------------------------------------------------
// TerminateRequest
//-----------------------------------------------------------------------------
bool castor::jobmanager::ManagementThread::terminateRequest
(LS_LONG_INT jobId, Cuuid_t requestId, Cuuid_t subRequestId, Cns_fileid fileId, int errorCode) {

  // "Invoking TerminateRequest"
  castor::dlf::Param params2[] =
    {castor::dlf::Param("JobId", (int)jobId),
     castor::dlf::Param(subRequestId)};
  castor::dlf::dlf_writep(requestId, DLF_LVL_DEBUG, 72, 2, params2, &fileId);

  // LSF has the capacity to kill jobs on bulk (lsb_killbulkjobs). However
  // testing has shown that this call fails to invoke the deletion hook of the
  // scheduler plugin and results in a memory leak in the plugin. (Definitely
  // a LSF bug!)
  if (jobId > 0) {
    if (lsb_forcekilljob(jobId) < 0) {
      if ((lsberrno == LSBE_NO_JOB) || (lsberrno == LSBE_JOB_FINISH)) {
	// No matching job found, most likely killed by another daemon
	return false;
      }

      // "Failed to terminate LSF job"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Function", "lsb_forcekilljob"),
	 castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message"),
	 castor::dlf::Param("JobId", (int)jobId),
	 castor::dlf::Param(subRequestId)};
      castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 24, 4, params, &fileId);
      return false;
    }
  }

  // Run the post checks for the job, this will perform the necessary cleanups
  try {
    char jobName[CUUID_STRING_LEN + 1];
    Cuuid2string(jobName, CUUID_STRING_LEN + 1, &subRequestId);
    return m_jobManagerService->postJobChecks(jobName, errorCode);
  } catch (castor::exception::Exception e) {

    // "Exception caught trying to run post job checks"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param(subRequestId)};
    castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 28, 3, params, &fileId);
  } catch (...) {

    // "Failed to execute postJobChecks procedure"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught"),
       castor::dlf::Param(subRequestId)};
    castor::dlf::dlf_writep(requestId, DLF_LVL_ERROR, 29, 2, params, &fileId);
  }
  return false;
}
