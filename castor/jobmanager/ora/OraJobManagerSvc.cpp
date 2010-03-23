/******************************************************************************
 *                      OraJobManagerSvc.cpp
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
 * @(#)$RCSfile: OraJobManagerSvc.cpp,v $ $Revision: 1.18 $ $Release$ $Date: 2008/09/22 12:31:58 $ $Author: waldron $
 *
 * Implementation of the IJobManagerSvc for Oracle
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/jobmanager/ora/OraJobManagerSvc.hpp"
#include "castor/jobmanager/ManagementThread.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "castor/Constants.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/IFactory.hpp"
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "occi.h"
#include <sys/time.h>


//-----------------------------------------------------------------------------
// Instantiation of a static factory class
//-----------------------------------------------------------------------------
static castor::SvcFactory<castor::jobmanager::ora::OraJobManagerSvc>
*s_factoryOraJobManagerSvc =
  new castor::SvcFactory<castor::jobmanager::ora::OraJobManagerSvc>();


//-----------------------------------------------------------------------------
// Static constants initialization
//-----------------------------------------------------------------------------

/// SQL statement for function jobToSchedule
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_jobToScheduleString =
  "BEGIN jobToSchedule(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25); END;";

/// SQL statement for function updateSchedulerJob
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_updateSchedulerJobString =
  "UPDATE SubRequest SET status = :1, lastModificationTime = getTime() WHERE status = 14 AND id = :2";

/// SQL statement for function getSchedulerJobsFromDB
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_getSchedulerJobsFromDBString =
  "BEGIN getSchedulerJobs(:1); END;";

/// SQL statement for function jobFailedProc
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_jobFailedString =
  "BEGIN jobFailed(:1, :2, :3); END;";


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::jobmanager::ora::OraJobManagerSvc::OraJobManagerSvc
(const std::string name) :
  OraCommonSvc(name),
  m_jobToScheduleStatement(0),
  m_updateSchedulerJobStatement(0),
  m_getSchedulerJobsFromDBStatement(0),
  m_jobFailedStatement(0) {}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::jobmanager::ora::OraJobManagerSvc::~OraJobManagerSvc() throw() {
  reset();
}


//-----------------------------------------------------------------------------
// Id
//-----------------------------------------------------------------------------
unsigned int castor::jobmanager::ora::OraJobManagerSvc::id() const {
  return ID();
}


//-----------------------------------------------------------------------------
// ID
//-----------------------------------------------------------------------------
unsigned int castor::jobmanager::ora::OraJobManagerSvc::ID() {
  return castor::SVC_ORAJOBMANAGERSVC;
}


//-----------------------------------------------------------------------------
// Reset
//-----------------------------------------------------------------------------
void castor::jobmanager::ora::OraJobManagerSvc::reset() throw() {
  // Here we attempt to delete the statements correctly. If something goes
  // wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if (m_jobToScheduleStatement)
      deleteStatement(m_jobToScheduleStatement);
    if (m_updateSchedulerJobStatement)
      deleteStatement(m_updateSchedulerJobStatement);
    if (m_getSchedulerJobsFromDBStatement)
      deleteStatement(m_getSchedulerJobsFromDBStatement);
    if (m_jobFailedStatement)
      deleteStatement(m_jobFailedStatement);
  } catch (oracle::occi::SQLException e) {
    // Do nothing
  }

  // Now reset all pointers to 0
  m_jobToScheduleStatement          = 0;
  m_updateSchedulerJobStatement     = 0;
  m_getSchedulerJobsFromDBStatement = 0;
  m_jobFailedStatement              = 0;
}


//-----------------------------------------------------------------------------
// JobToSchedule
//-----------------------------------------------------------------------------
castor::jobmanager::JobSubmissionRequest
*castor::jobmanager::ora::OraJobManagerSvc::jobToSchedule()
  throw(castor::exception::Exception) {

  // Initialize statements
  try {
    if (m_jobToScheduleStatement == NULL) {
      m_jobToScheduleStatement = createStatement(s_jobToScheduleString);
      m_jobToScheduleStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (2, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (3, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (4, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (7, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (8, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (9, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (10, oracle::occi::OCCIINT);
      m_jobToScheduleStatement->registerOutParam
        (11, oracle::occi::OCCIINT);
      m_jobToScheduleStatement->registerOutParam
        (12, oracle::occi::OCCIINT);
      m_jobToScheduleStatement->registerOutParam
        (13, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (14, oracle::occi::OCCISTRING, 10);
      m_jobToScheduleStatement->registerOutParam
        (15, oracle::occi::OCCIINT);
      m_jobToScheduleStatement->registerOutParam
        (16, oracle::occi::OCCIINT);
      m_jobToScheduleStatement->registerOutParam
        (17, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (18, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (19, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (20, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (21, oracle::occi::OCCIINT);
      m_jobToScheduleStatement->registerOutParam
        (22, oracle::occi::OCCISTRING, 2048);
      m_jobToScheduleStatement->registerOutParam
        (23, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (24, oracle::occi::OCCIDOUBLE);
      m_jobToScheduleStatement->registerOutParam
        (25, oracle::occi::OCCICURSOR);
      m_jobToScheduleStatement->setAutoCommit(true);
    }

    // Prepare the execute the statement
    unsigned int nb = m_jobToScheduleStatement->executeUpdate();
    if (nb == 0) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "jobToSchedule did not do anything.";
      throw ex;
    }
    u_signed64 srId = (u_signed64)m_jobToScheduleStatement->getDouble(1);
    if (srId == 0) {
      return 0;  // Nothing to do
    }

    // Return a new JobSubmissionRequest object for scheduling
    castor::jobmanager::JobSubmissionRequest *result =
      new castor::jobmanager::JobSubmissionRequest();
    result->setId(srId);
    result->setSubReqId
      (m_jobToScheduleStatement->getString(2));
    result->setProtocol
      (m_jobToScheduleStatement->getString(3));
    result->setXsize
      ((u_signed64)m_jobToScheduleStatement->getDouble(4));
    result->setRequestedFileSystems
      (m_jobToScheduleStatement->getString(5));
    result->setReqId
      (m_jobToScheduleStatement->getString(6));
    result->setFileId
      ((u_signed64)m_jobToScheduleStatement->getDouble(7));
    result->setNsHost
      (m_jobToScheduleStatement->getString(8));
    result->setSvcClass
      (m_jobToScheduleStatement->getString(9));
    result->setRequestType
      (m_jobToScheduleStatement->getInt(10));
    result->setEuid
      (m_jobToScheduleStatement->getInt(11));
    result->setEgid
      (m_jobToScheduleStatement->getInt(12));
    result->setUsername
      (m_jobToScheduleStatement->getString(13));
    result->setOpenFlags
      (m_jobToScheduleStatement->getString(14));
    result->setIpAddress
      (m_jobToScheduleStatement->getInt(15));
    result->setPort
      (m_jobToScheduleStatement->getInt(16));
    result->setClientVersion
      ((u_signed64)m_jobToScheduleStatement->getDouble(17));
    result->setClientType
      ((u_signed64)m_jobToScheduleStatement->getDouble(18));
    result->setSourceDiskCopyId
      ((u_signed64)m_jobToScheduleStatement->getDouble(19));
    result->setDestDiskCopyId
      ((u_signed64)m_jobToScheduleStatement->getDouble(20));

    // Append the hosts from the requested filesystems attribute to the asked
    // hosts list.
    result->setNumAskedHosts(0);
    if (result->requestedFileSystems() != "") {
      std::istringstream iss(result->requestedFileSystems());
      std::string buf;

      // For StageDiskCopyReplicaRequest's the requested filesystems should
      // only contain one entry! The diskserver and mountpoint of the source
      // file to be copied.
      if (result->requestType() == OBJ_StageDiskCopyReplicaRequest) {
        if (!std::getline(iss, buf, ':')) {
          castor::exception::Internal ex;
          ex.getMessage() << "No Requested FileSystems defined for DiskCopy "
                          << "replication request";
          throw ex;
        }

        // Append an exclamation mark to the end of the execution hostname. This
        // instructs LSF that the requested/asked host is a 'headnode' and that
        // the job must start on this host before any other.
        result->setAskedHosts(buf + "! ");
        result->setNumAskedHosts(result->numAskedHosts() + 1);
      }
      // Non StageDiskCopyReplicaRequest's can have 1..N number of requested
      // filesystems.
      else {
        while (std::getline(iss, buf, ':')) {
          result->setAskedHosts(result->askedHosts().append(buf + " "));
          result->setNumAskedHosts(result->numAskedHosts() + 1);
          std::getline(iss, buf, '|');
        }
      }
    }

    // Append the host group to the list of required hosts, only for
    // StageDiskCopyReplicaRequests and PUT requests
    if ((result->requestType() == OBJ_StageDiskCopyReplicaRequest) ||
        (result->requestedFileSystems() == "")) {
      result->setAskedHosts
        (result->askedHosts().append(result->svcClass() + "Group"));
      result->setNumAskedHosts(result->numAskedHosts() + 1);
    }

    result->setClientSecure(m_jobToScheduleStatement->getInt(21));
    result->setSourceSvcClass(m_jobToScheduleStatement->getString(22));
    result->setRequestCreationTime
      ((u_signed64)m_jobToScheduleStatement->getDouble(23));
    result->setDefaultFileSize
      ((u_signed64)m_jobToScheduleStatement->getDouble(24));

    // Construct the list of excluded hosts
    if (result->requestType() == OBJ_StageDiskCopyReplicaRequest) {
      oracle::occi::ResultSet *rs = m_jobToScheduleStatement->getCursor(25);
      while (oracle::occi::ResultSet::END_OF_FETCH != rs->next()) {
        result->setExcludedHosts
          (result->excludedHosts().append(rs->getString(1) + "|"));
      }
      // Remove trailing '|'
      if (result->excludedHosts() != "") {
        result->setExcludedHosts
          (result->excludedHosts().substr(0, result->excludedHosts().length() - 1));
      }
      m_jobToScheduleStatement->closeResultSet(rs);
    }

    // For statistical purposes
    timeval tv;
    gettimeofday(&tv, NULL);
    result->setSelectTime(tv.tv_sec * 1000000 + tv.tv_usec);
    result->setSubmitStartTime(0);
    return result;

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    // Ignore ORA-01403: no data found, this is expected when there is nothing
    // to be scheduled.
    if (e.getErrorCode() == 1403) {
      return NULL;
    }
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in jobToSchedule."
      << std::endl << e.getMessage();
    throw ex;
  }
  return NULL;
}


//-----------------------------------------------------------------------------
// UpdateSchedulerJob
//-----------------------------------------------------------------------------
void castor::jobmanager::ora::OraJobManagerSvc::updateSchedulerJob
(const castor::jobmanager::JobSubmissionRequest *request,
 const castor::stager::SubRequestStatusCodes status)
  throw(castor::exception::Exception) {

  // Initialize statements
  try {
    if (m_updateSchedulerJobStatement == NULL) {
      m_updateSchedulerJobStatement = createStatement(s_updateSchedulerJobString);
      m_updateSchedulerJobStatement->setAutoCommit(true);
    }

    // Prepare and execute the statement
    m_updateSchedulerJobStatement->setDouble(1, status);
    m_updateSchedulerJobStatement->setDouble(2, request->id());
    unsigned int nb = m_updateSchedulerJobStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "updateSchedulerJob did not do anything.";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateSchedulerJob trying to change SubRequest "
      << "status to " << castor::stager::SubRequestStatusCodesStrings[status]
      << std::endl << e.getMessage();
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// GetSchedulerJobsFromDB
//-----------------------------------------------------------------------------
std::map<std::string, castor::jobmanager::JobInfo>
castor::jobmanager::ora::OraJobManagerSvc::getSchedulerJobsFromDB()
  throw(castor::exception::Exception) {

  try {
    // Initialize statements
    if (m_getSchedulerJobsFromDBStatement == NULL) {
      m_getSchedulerJobsFromDBStatement =
        createStatement(s_getSchedulerJobsFromDBString);
      m_getSchedulerJobsFromDBStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // Execute the query
    m_getSchedulerJobsFromDBStatement->executeUpdate();

    // Loop over the results
    std::map<std::string, castor::jobmanager::JobInfo> databaseJobs;
    oracle::occi::ResultSet *rset =
      m_getSchedulerJobsFromDBStatement->getCursor(1);
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      castor::jobmanager::JobInfo job;
      job.setSubReqId(rset->getString(1));
      job.setReqId(rset->getString(2));
      job.setNoSpace(rset->getInt(3));
      job.setNoFileSystems(rset->getInt(4));
      databaseJobs[rset->getString(1)] = job;
    }

    // Release resources
    m_getSchedulerJobsFromDBStatement->closeResultSet(rset);
    return databaseJobs;

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in synchLSFAndStager."
      << std::endl << e.getMessage();
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// JobFailed
//-----------------------------------------------------------------------------
std::vector<std::string> castor::jobmanager::ora::OraJobManagerSvc::jobFailed
(const std::vector<std::pair<std::string, int> > jobs)
  throw(castor::exception::Exception) {

  // If there are no jobs to fail return immediately!
  std::vector<std::string> failedJobs;
  int nb = jobs.size();
  if (nb == 0) {
    return failedJobs;
  }

  // Container to hold pointers to memory to be freed
  std::vector<void *> allocPtrs;

  try {
    // Initialize statements
    if (m_jobFailedStatement == NULL) {
      m_jobFailedStatement = createStatement(s_jobFailedString);
      m_jobFailedStatement->registerOutParam(3, oracle::occi::OCCICURSOR);
      m_jobFailedStatement->setAutoCommit(true);
    }

    // Build the buffers for the SubRequest uuid
    unsigned int subReqUuidMaxLen = 0;
    for (int i = 0; i < nb; i++) {
      if (jobs[i].first.length() > subReqUuidMaxLen)
        subReqUuidMaxLen = jobs[i].first.length();
    }
    char *subReqUuidBuffer =
      (char *)calloc(nb, subReqUuidMaxLen * sizeof(char));
    if (subReqUuidBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocPtrs.push_back(subReqUuidBuffer);
    ub2 *subReqUuidBufLens = (ub2 *)malloc(nb * sizeof(ub2));
    if (subReqUuidBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocPtrs.push_back(subReqUuidBufLens);

    // Build the buffers to hold the error code
    unsigned int errorCodeMaxLen = 21;
    unsigned char (*errorCodeBuffer)[21] =
      (unsigned char(*)[21]) calloc(nb * errorCodeMaxLen,
                                    sizeof(unsigned char));
    if (errorCodeBuffer == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocPtrs.push_back(errorCodeBuffer);
    ub2 *errorCodeBufLens = (ub2 *)malloc(nb * sizeof(ub2));
    if (errorCodeBufLens == 0) {
      castor::exception::OutOfMemory e;
      throw e;
    }
    allocPtrs.push_back(errorCodeBufLens);

    // Fill the SubRequest uuid and error code buffers
    for (int i = 0; i < nb; i++) {
      // SubRequest uuid
      strncpy(subReqUuidBuffer + (i * subReqUuidMaxLen),
              jobs[i].first.c_str(), subReqUuidMaxLen);
      subReqUuidBufLens[i] = jobs[i].first.length();

      // Error code
      oracle::occi::Number n = (double)(jobs[i].second);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(errorCodeBuffer[i], b.length());
      errorCodeBufLens[i] = b.length();
    }

    // Set the data buffer arrays
    ub4 subReqUuidArraySize = nb;
    ub4 errorCodeArraySize  = nb;
    m_jobFailedStatement->setDataBufferArray
      (1, subReqUuidBuffer, oracle::occi::OCCI_SQLT_CHR, subReqUuidArraySize,
       &subReqUuidArraySize, subReqUuidMaxLen, subReqUuidBufLens);
    m_jobFailedStatement->setDataBufferArray
      (2, errorCodeBuffer, oracle::occi::OCCI_SQLT_NUM, errorCodeArraySize,
       &errorCodeArraySize, errorCodeMaxLen, errorCodeBufLens);

    // Execute the statement
    m_jobFailedStatement->execute();

    // Free resources
    for (unsigned int i = 0; i < allocPtrs.size(); i++) {
      free(allocPtrs[i]);
    }

    // Loop over the results
    oracle::occi::ResultSet *rset =
      m_jobFailedStatement->getCursor(3);
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      failedJobs.push_back(rset->getString(1));
    }

    // Release resources
    m_jobFailedStatement->closeResultSet(rset);
    return failedJobs;

  } catch (oracle::occi::SQLException e) {
    // Free resources
    for (unsigned int i = 0; i < allocPtrs.size(); i++) {
      free(allocPtrs[i]);
    }
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in jobFailed."
      << std::endl << e.getMessage();
    throw ex;
  }
}
