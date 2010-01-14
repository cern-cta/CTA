/******************************************************************************
 *                      SubmissionProcess.cpp
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
 * @(#)$RCSfile: SubmissionProcess.cpp,v $ $Revision: 1.28 $ $Release$ $Date: 2009/07/23 12:21:57 $ $Author: waldron $
 *
 * The Submission Process is used to submit new jobs into the scheduler. It is
 * run inside a separate process allowing for setuid and setgid calls to take
 * place as a prerequisite for submission into LSF.
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/jobmanager/SubmissionProcess.hpp"
#include "castor/jobmanager/JobSubmissionRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "getconfent.h"
#include <errno.h>
#include <pwd.h>
#include <sys/time.h>

// Definitions
#define DEFAULT_RETRY_ATTEMPTS 3
#define DEFAULT_RETRY_INTERVAL 5
#define DEFAULT_MAX_DISKCOPY_RUNTIME 18000


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::jobmanager::SubmissionProcess::SubmissionProcess
(bool reverseUidLookup, std::string sharedLSFResource) :
  m_reverseUidLookup(reverseUidLookup),
  m_sharedLSFResource(sharedLSFResource),
  m_submitRetryAttempts(DEFAULT_RETRY_ATTEMPTS),
  m_submitRetryInterval(DEFAULT_RETRY_INTERVAL),
  m_maxDiskCopyRunTime(DEFAULT_MAX_DISKCOPY_RUNTIME) {}


//-----------------------------------------------------------------------------
// Init
//-----------------------------------------------------------------------------
void castor::jobmanager::SubmissionProcess::init()
  throw(castor::exception::Exception) {

  // Initialize the oracle job manager service.
  castor::IService *orasvc =
    castor::BaseObject::services()->service("OraJobManagerSvc",
                                            castor::SVC_ORAJOBMANAGERSVC);
  if (orasvc == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to get OraJobManagerSVC for SubmissionProcess";
    throw e;
  }
  m_jobManagerService =
    dynamic_cast<castor::jobmanager::IJobManagerSvc *>(orasvc);
  if (m_jobManagerService == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Could not convert newly retrieved service into "
                   << "IJobManagerSvc for SubmissionProcess" << std::endl;
    throw e;
  }

  // Initialize the LSF API
  if (lsb_init((char*)"jobManager") < 0) {

    // "Failed to initialize the LSF batch library (LSBLIB)"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "lsb_init"),
       castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 2, params);
  }

  // Determine the number of times the job manager should try to submit a job
  // into LSF when it encounters an error
  char *value = getconfent("JobManager", "SubmitRetryAttempts", 0);
  int attempts, interval;
  if (value) {
    attempts = strtol(value, 0, 10);
    if (attempts == 0) {
      attempts = 1;
    } else if (attempts < 0) {
      attempts = DEFAULT_RETRY_ATTEMPTS;

      // "Invalid JobManager/SubmitRetryAttempts option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", attempts)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 40, 1, params);
    }
    m_submitRetryAttempts = attempts;
  }

  // Extract the value of the SubmitRetryInterval options from the castor.conf
  // config file. This value determines how long we sleep in between submission
  // attempts. A really small value will stress the LSF master more then
  // necessary, you have been warned!!
  value = getconfent("JobManager", "SubmitRetryInterval", 0);
  if (value) {
    interval = strtol(value, 0, 10);
    if (interval < 1) {
      interval = DEFAULT_RETRY_INTERVAL;

      // "Invalid JobManager/SubmitRetryInterval option, value too small.
      // Using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", interval)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 41, 1, params);
    }
    m_submitRetryInterval = interval;
  }

  // Determine the max run time of a StageDiskCopyReplicaRequest
  value = getconfent("JobManager", "MaxDiskCopyRunTime", 0);
  if (value) {
    m_maxDiskCopyRunTime = strtol(value, 0, 10);
    if (m_maxDiskCopyRunTime < -1) {
      m_maxDiskCopyRunTime = DEFAULT_MAX_DISKCOPY_RUNTIME;

      // "Invalid JobManager/MaxDiskCopyRunTime option, value too small.
      // Using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", m_maxDiskCopyRunTime)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 57, 1, params);
    }
  }
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::jobmanager::SubmissionProcess::run(void *param) {
  castor::jobmanager::JobSubmissionRequest *request =
    (castor::jobmanager::JobSubmissionRequest *)param;

  // At this point we are running in one of the preforked worker processes and
  // as such we can modify the uid and gid of the process as needed by the LSF
  // API for submission of the job into the scheduler
  string2Cuuid(&m_requestUuid, (char *)request->reqId().c_str());
  string2Cuuid(&m_subRequestUuid, (char *)request->subReqId().c_str());
  m_fileId.fileid = request->fileId();
  strncpy(m_fileId.server, request->nsHost().c_str(), CA_MAXHOSTNAMELEN);
  m_fileId.server[CA_MAXHOSTNAMELEN] = '\0';

  // Flag the submission start time for statistical purposes
  timeval tv;
  gettimeofday(&tv, NULL);
  request->setSubmitStartTime((tv.tv_sec * 1000000) + tv.tv_usec);

  // If this is a StageDiskCopyReplicaRequest set the username, euid and egid
  // to the values for the stage super user/group
  int errorCode = 0;
  if (request->requestType() == OBJ_StageDiskCopyReplicaRequest) {
    request->setUsername(STAGERSUPERUSER);
    passwd *pwd = getpwnam(STAGERSUPERUSER);
    if (pwd == NULL) {
      errorCode = SEUSERUNKN;  // Unknown user
    } else {
      request->setEuid(pwd->pw_uid);
      request->setEgid(pwd->pw_gid);
    }
  }

  // If reverse UID lookups are enabled, make sure that the resolution of the
  // uid matches the username reported and that the user belongs to the right
  // group.
  if (m_reverseUidLookup && !errorCode) {
    passwd *pwd = getpwuid(request->euid());
    if (pwd == NULL) {
      errorCode = SEUSERUNKN;  // Unknown user
    } else if (strcmp(request->username().c_str(), pwd->pw_name)) {
      errorCode = ESTUSER;     // Invalid user
    } else if ((gid_t)request->egid() != pwd->pw_gid) {
      errorCode = ESTGROUP;    // Invalid user group
    }
  }

  // If the previous uid checks failed, fail the subrequest
  if (errorCode) {

    // "Reverse UID lookup failure. User credentials are invalid, job will not
    // be scheduled"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Username", request->username()),
       castor::dlf::Param("Euid", request->euid()),
       castor::dlf::Param("Egid", request->egid()),
       castor::dlf::Param("ID", request->id()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 42, 5, params, &m_fileId);
    failRequest(request, errorCode);
    return;
  }

  // Fail jobs that try to submit themselves to a svcclass called "all". This
  // is a reserved keyword in the JobManager to define default configuration
  // options for all queues/svcclasses.
  if (!strcasecmp(request->svcClass().c_str(), "all")) {

    // "Cannot submit job into LSF, svcclass/queue name is a reserved word"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Username", request->username()),
       castor::dlf::Param("SvcClass", request->svcClass())};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 51, 2, params, &m_fileId);
    failRequest(request, SEINTERNAL);
    return;
  }

  // Change the real and effective user id of the process. This is required
  // for LSF as the lsb_submit API function call does a getpid() call and users
  // the result to set the user of the job. Root cannot submit jobs into LSF!!
  if (setreuid(request->euid(), 0) < 0) {

    // "Failed to change real and effective user id of process using setreuid,
    // job cannot be submitted into the scheduler as user root"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", strerror(errno)),
       castor::dlf::Param("Euid", request->euid()),
       castor::dlf::Param("ID", request->id()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 43, 4, params, &m_fileId);
    failRequest(request, SEINTERNAL);
    return;
  }

  // Submit the request into LSF
  try {
    submitJob(request);
  } catch (castor::exception::Exception e) {

    // "Exception caught trying to submit a job into LSF"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("ID", request->id()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 49, 4, params, &m_fileId);
  } catch (...) {

    // "Failed to execute lsfSubmit in SubmissionProcess::run"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught"),
       castor::dlf::Param("ID", request->id()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 50, 3, params, &m_fileId);
  }

  // Free allocated memory
  if (m_job.askedHosts) {
    for (int i = 0; i < m_job.numAskedHosts; i++) {
      free(m_job.askedHosts[i]);
    }
    free(m_job.askedHosts);
  }

  // We cannot change the uid to another user before changing it back to root.
  // If this call fails there is nothing that can be done. A sequent setreuid
  // later will also fail rendering this child process useless, so we end it!
  if (setreuid(0, 0) < 0) {

    // "Failed to reset the real and effective user id back to root.
    // Terminating further processing through this worker process"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", strerror(errno))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_EMERGENCY, 44, 1, params);
    exit(EXIT_FAILURE);
  }
}


//-----------------------------------------------------------------------------
// SubmitJob
//-----------------------------------------------------------------------------
void castor::jobmanager::SubmissionProcess::submitJob
(castor::jobmanager::JobSubmissionRequest *request)
  throw(castor::exception::Exception) {

  // Initialize the LSF submit and submitReply structures
  memset(&m_job,      0, sizeof(submit));
  memset(&m_jobReply, 0, sizeof(submitReply));

  // Populate the submit structure
  for (u_signed64 i = 0; i < LSF_RLIM_NLIMITS; i++) {
    m_job.rLimits[i] = DEFAULT_RLIMIT;
  }

  m_job.options     = SUB_JOB_NAME | SUB_PROJECT_NAME | SUB_RES_REQ;
  m_job.options2    = SUB2_EXTSCHED;
  m_job.beginTime   = 0;
  m_job.termTime    = 0;
  m_job.jobName     = (char *)request->subReqId().c_str();
  m_job.projectName = (char *)request->svcClass().c_str();

  // Set stdin, stdout and stderr to /dev/null. This will disable job output
  // emails sent by LSF after the job ends.
  m_job.options |= SUB_IN_FILE | SUB_OUT_FILE | SUB_ERR_FILE;
  m_job.inFile  = (char*)"/dev/null";
  m_job.outFile = (char*)"/dev/null";
  m_job.errFile = (char*)"/dev/null";

  // Only change the queue if the job is not meant to run in the default
  // service class. By not defining the queue LSF will but the job into the
  // queue which it has defined as default .e.g castor
  if (request->svcClass() != "default") {
    m_job.options |= SUB_QUEUE;
    m_job.queue   = (char *)request->svcClass().c_str();
  }

  // Set the initial and max number of slots needed by the job to start. For
  // StageDiskCopyReplicaRequests this is 2, 1 for the source and 1 for the
  // destination. Note: PARALLEL_SCHED_BY_SLOT=y must be defined in the lsf.conf
  // file for this too work correctly. If not defined, LSF will schedule based
  // on the number of processors available not slots!!
  if (request->requestType() == OBJ_StageDiskCopyReplicaRequest) {
    m_job.numProcessors    = 2;
    m_job.maxNumProcessors = 2;
  } else {
    m_job.numProcessors    = 0;
    m_job.maxNumProcessors = 0;
  }

  // Construct the resource requirements for the job. The requirements differ
  // depending on the type of request. For StageDiskCopyReplicaRequests we
  // specify a 'diskcopy' resource which allows us to use resource counters
  // in LSF to restrict the number of DiskCopy replication requests at the host
  // and queue level inside LSF directly. For other requests its the protocol
  // the job requires
  char resReq[MAXLINELEN];
  if (request->requestType() == OBJ_StageDiskCopyReplicaRequest) {
    strncpy(resReq,
            "span[ptile=1] type==any && select[defined(diskcopy)] rusage[diskcopy=1]]",
            MAXLINELEN);
  } else {
    snprintf(resReq, MAXLINELEN,
             "type==any && select[defined(%s)] rusage[%s=1]]",
             request->protocol().c_str(), request->protocol().c_str());
  }
  m_job.resReq = resReq;

  // Both GET requests and DiskCopy replication requests require a set of hosts
  // to be explicitly defined for the job to run.
  m_job.numAskedHosts = 0;
  if (request->numAskedHosts()) {
    m_job.askedHosts = (char **)malloc(request->numAskedHosts() * sizeof(char *));
    if (m_job.askedHosts == NULL) {

      // "Memory allocation failure, job submission cancelled"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", strerror(errno)),
         castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 53, 2, params, &m_fileId);
      failRequest(request, SEINTERNAL);
      return;
    }

    // Update the asked host list.
    std::istringstream iss(request->askedHosts());
    std::string host;
    while (std::getline(iss, host, ' ')) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("Host", host)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 99, 1, params);
      m_job.askedHosts[m_job.numAskedHosts] = strdup(host.c_str());
      m_job.numAskedHosts++;
    }
    m_job.options |= SUB_HOST;
  }

  // For StageDiskCopyReplicaRequests set the max runtime of the job. This
  // helps us to recover from situations where jobs are stuck due to the crash
  // of a diskserver during a transfer between machines.
  if ((request->requestType() == OBJ_StageDiskCopyReplicaRequest) &&
      (m_maxDiskCopyRunTime != -1)) {
    m_job.rLimits[LSF_RLIMIT_RUN] = m_maxDiskCopyRunTime;
    m_job.options2 |= SUB2_MODIFY_RUN_JOB;
  }

  // Set the external scheduler options. This information is made available to
  // the CASTOR2 LSF plugin and is required for logging and filesystem level
  // scheduling.
  char extSched[MAXLINELEN];
  std::ostringstream oss;
  oss << "SIZE="          << request->xsize()                << ";"
      << "DEFSIZE="       << request->defaultFileSize()      << ";"
      << "RFS="           << request->requestedFileSystems() << ";"
      << "PROTOCOL="      << request->protocol()             << ";"
      << "SVCCLASS="      << request->svcClass()             << ";"
      << "REQUESTID="     << request->reqId()                << ";"
      << "SUBREQUESTID="  << request->subReqId()             << ";"
      << "OPENFLAGS="     << request->openFlags()            << ";"
      << "FILEID="        << request->fileId()               << ";"
      << "NSHOST="        << request->nsHost()               << ";"
      << "TYPE="          << request->requestType()          << ";"
      << "SRCSVCCLASS="   << request->sourceSvcClass()       << ";"
      << "EXCLUDEDHOSTS=" << request->excludedHosts();
  strncpy(extSched, oss.str().c_str(), MAXLINELEN);
  m_job.extsched = extSched;

  // Construct the command line to be executed
  char command[MAX_CMD_DESC_LEN];
  std::ostringstream cmd;
  if (request->requestType() == OBJ_StageDiskCopyReplicaRequest) {
    cmd << "lsgrun -p -m \"$LSB_HOSTS\" /usr/bin/d2dtransfer"
        << " -r " << request->reqId()
        << " -s " << request->subReqId()
        << " -F " << request->fileId()
        << " -H " << request->nsHost()
        << " -D " << request->destDiskCopyId()
        << " -X " << request->sourceDiskCopyId()
        << " -S " << request->svcClass()
        << " -t " << request->requestCreationTime()
        << " -R " << m_sharedLSFResource << "/$LSB_JOBID";
  } else {
    cmd << "/usr/bin/stagerjob"
        << " -r " << request->reqId()
        << " -s " << request->subReqId()
        << " -F " << request->fileId()
        << " -H " << request->nsHost()
        << " -p " << request->protocol()
        << " -i " << request->id()
        << " -T " << request->requestType()
        << " -m " << request->openFlags()

      // The client's identification as a string. This consists of the
      // client's object type, IP address and port
        << " -C \"" <<  request->clientType() << ":"
        << ((request->ipAddress() & 0xFF000000) >> 24) << "."
        << ((request->ipAddress() & 0x00FF0000) >> 16) << "."
        << ((request->ipAddress() & 0x0000FF00) >> 8)  << "."
        << ((request->ipAddress() & 0x000000FF))
        << ":" << request->port() << "\""

        << " -u " << request->euid()
        << " -g " << request->egid()
        << " -X " << request->clientSecure()
        << " -t " << request->requestCreationTime()
        << " -R " << m_sharedLSFResource << "/$LSB_JOBID";
  }
  strncpy(command, cmd.str().c_str(), MAX_CMD_DESC_LEN);
  m_job.command = command;

  // Submit the job into LSF
  for (u_signed64 i = 0; i < m_submitRetryAttempts; i++) {
    LS_LONG_INT jobId = lsb_submit(&m_job, &m_jobReply);
    if (jobId > 0) {

      // Calculate statistics
      timeval tv;
      gettimeofday(&tv, NULL);
      signed64 submissionTime =
        (((tv.tv_sec * 1000000) + tv.tv_usec) - request->submitStartTime());

      // "Job successfully submitted into LSF"
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", (u_signed64)jobId),
         castor::dlf::Param("Type", castor::ObjectsIdStrings[request->requestType()]),
         castor::dlf::Param("Username", request->username()),
         castor::dlf::Param("SvcClass", request->svcClass()),
         castor::dlf::Param("MaxRunTime", m_job.rLimits[LSF_RLIMIT_RUN]),
         castor::dlf::Param("AskedHosts",
                            request->requestType() == 40 ? 0 : m_job.numAskedHosts),
         castor::dlf::Param("SourceDiskCopyId", request->sourceDiskCopyId()),
         castor::dlf::Param("SubmissionTime",
                            submissionTime != 0 ? submissionTime * 0.000001 : 0),
         castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_SYSTEM, 45, 9, params, &m_fileId);

      // Update the subrequest to SUBREQUEST_READY
      m_jobManagerService->updateSchedulerJob(request,
                                              castor::stager::SUBREQUEST_READY);
      break;
    }

    // Any error apart from transient ones are consider fatal
    if ((lsberrno != LSBE_MBATCHD)       || // Mbatchd internal error
        (lsberrno != LSBE_SBATCHD)       || // Sbatchd internal error
        (lsberrno != LSBE_TIME_OUT)      || // Timeout in contacting mbatchd
        (lsberrno != LSBE_CONN_TIMEOUT)  || // Timeout on connect() call
        (lsberrno != LSBE_CONN_REFUSED)  || // Connection refused by server
        (lsberrno != LSBE_CONN_EXIST)    || // server connection already exists
        (lsberrno != LSBE_CONN_NONEXIST) || // server is not connected
        (lsberrno != LSBE_SBD_UNREACH)   || // sbd cannot be reached
        (lsberrno != LSBE_OP_RETRY)) {      // Operation cannot be performed

      // "Failed to submit job into LSF, fatal error encountered"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message"),
         castor::dlf::Param("BadHosts",
                            m_jobReply.badReqIndx >= m_job.numAskedHosts ? "None" :
                            m_job.askedHosts[m_jobReply.badReqIndx]),
         castor::dlf::Param("ResReq", m_job.resReq),
         castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 48, 4, params, &m_fileId);

      // Try and give a meaningful message
      failRequest(request, ESTSCHEDERR);
      break;
    }

    // Maximum submission attempts exceeded ?
    else if ((i + 1) == m_submitRetryAttempts) {

      // "Exceeded maximum number of attempts trying to submit a job into LSF"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message"),
         castor::dlf::Param("MaxAttempts", m_submitRetryAttempts),
         castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 47, 3, params, &m_fileId);
      failRequest(request, SEINTERNAL);
      break;
    }

    // "Failed to submit job into LSF, will try again"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", lsberrno ? lsb_sysmsg() : "no message"),
       castor::dlf::Param("Attempt", i + 1),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_WARNING, 46, 3, params, &m_fileId);
    sleep(m_submitRetryInterval);
  }
}


//-----------------------------------------------------------------------------
// FailRequest
//-----------------------------------------------------------------------------
void castor::jobmanager::SubmissionProcess::failRequest
(castor::jobmanager::JobSubmissionRequest *request, int errorCode) {

  // Fail the submission of the job
  try {
    // A container to hold a list of jobs which have to be failed and the
    // reason why
    std::vector<std::pair<std::string, int> > failedJobs;
    failedJobs.push_back
      (std::pair<std::string, int>(request->subReqId(), errorCode));
    m_jobManagerService->jobFailed(failedJobs);
  } catch (castor::exception::Exception e) {

    // "Exception caught when trying to fail scheduler job"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 55, 3, params, &m_fileId);
  } catch (...) {

    // "Failed to execute jobFailed procedure"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught"),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 76, 2, params, &m_fileId);
  }
}
