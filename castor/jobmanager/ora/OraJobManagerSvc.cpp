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
 * @(#)$RCSfile: OraJobManagerSvc.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/08/21 06:24:13 $ $Author: waldron $
 *
 * Implementation of the IJobManagerSvc for Oracle
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/jobmanager/ora/OraJobManagerSvc.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/exception/Internal.hpp"
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

/// SQL statement for function failSchedulerJob
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_failSchedulerJobString =
  "BEGIN failSchedulerJob(:1, :2, :3, :4); END;";

/// SQL statement for function jobToSchedule
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_jobToScheduleString =
  "BEGIN jobToSchedule(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18); END;";

/// SQL statement for function updateSchedulerJob
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_updateSchedulerJobString =
  "UPDATE SubRequest SET status = :1 WHERE status = 14 AND id = :2";

/// SQL statement for function fileSystemStates
const std::string castor::jobmanager::ora::OraJobManagerSvc::s_fileSystemStatesString =
  "SELECT diskserver.name, filesystem.mountpoint, diskserver.status, filesystem.status \
     FROM filesystem, diskserver \
    WHERE filesystem.diskserver = diskserver.id";

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::jobmanager::ora::OraJobManagerSvc::OraJobManagerSvc
(const std::string name) :
  OraCommonSvc(name),
  m_failSchedulerJobStatement(0),
  m_jobToScheduleStatement(0),
  m_updateSchedulerJobStatement(0),
  m_fileSystemStatesStatement(0) {}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::jobmanager::ora::OraJobManagerSvc::~OraJobManagerSvc() throw() {
  reset();
}


//-----------------------------------------------------------------------------
// id
//-----------------------------------------------------------------------------
const unsigned int castor::jobmanager::ora::OraJobManagerSvc::id() const {
  return ID();
}


//-----------------------------------------------------------------------------
// ID
//-----------------------------------------------------------------------------
const unsigned int castor::jobmanager::ora::OraJobManagerSvc::ID() {
  return castor::SVC_ORAJOBMANAGERSVC;
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::jobmanager::ora::OraJobManagerSvc::reset() throw() {
  // Here we attempt to delete the statements correctly. If something goes
  // wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if (m_failSchedulerJobStatement)
      deleteStatement(m_failSchedulerJobStatement);
    if (m_jobToScheduleStatement)
      deleteStatement(m_jobToScheduleStatement);
    if (m_updateSchedulerJobStatement)
      deleteStatement(m_updateSchedulerJobStatement);
    if (m_fileSystemStatesStatement)
      deleteStatement(m_fileSystemStatesStatement);
  } catch (oracle::occi::SQLException e) {
    // Do nothing
  }
  
  // Now reset all pointers to 0
  m_failSchedulerJobStatement = 0;
  m_jobToScheduleStatement = 0;
  m_updateSchedulerJobStatement = 0;
  m_fileSystemStatesStatement = 0;
}


//-----------------------------------------------------------------------------
// failSchedulerJob
//-----------------------------------------------------------------------------
bool castor::jobmanager::ora::OraJobManagerSvc::failSchedulerJob
(const std::string subReqId, const int errorCode, const std::string errorMessage)
  throw(castor::exception::Exception) {

  // Initialize statements
  try {
    if (m_failSchedulerJobStatement == NULL) {
      m_failSchedulerJobStatement = createStatement(s_failSchedulerJobString);
      m_failSchedulerJobStatement->registerOutParam(4, oracle::occi::OCCIDOUBLE);
      m_failSchedulerJobStatement->setAutoCommit(true);
    }
    
    // Prepare and execute the statement
    m_failSchedulerJobStatement->setString(1, subReqId);
    m_failSchedulerJobStatement->setInt(2, errorCode);
    m_failSchedulerJobStatement->setString(3, errorMessage);
    m_failSchedulerJobStatement->executeUpdate();
    
    // Return the result of the output parameter, this is an indicator to
    // notify the callee as to whether or not the job was cancelled i.e.
    // a change was made to the subrequest table  
    if (m_failSchedulerJobStatement->getDouble(4) > 0) {
      return true;
    } 
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in failSchedulerJob."
      << std::endl << e.getMessage();
    throw ex;
  }
  return false;
}


//-----------------------------------------------------------------------------
// subrequestToSchedule
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
    result->setSubReqId(m_jobToScheduleStatement->getString(2));
    result->setProtocol(m_jobToScheduleStatement->getString(3));
    result->setXsize((u_signed64)m_jobToScheduleStatement->getDouble(4));
    result->setRequestedFileSystems(m_jobToScheduleStatement->getString(5));
    result->setReqId(m_jobToScheduleStatement->getString(6));
    result->setFileId((u_signed64)m_jobToScheduleStatement->getDouble(7));
    result->setNsHost(m_jobToScheduleStatement->getString(8));
    result->setSvcClass(m_jobToScheduleStatement->getString(9));
    result->setRequestType(m_jobToScheduleStatement->getInt(10));
    result->setEuid(m_jobToScheduleStatement->getInt(11));
    result->setEgid(m_jobToScheduleStatement->getInt(12));
    result->setUsername(m_jobToScheduleStatement->getString(13));
    result->setOpenFlags(m_jobToScheduleStatement->getString(14));
    result->setIpAddress(m_jobToScheduleStatement->getInt(15));
    result->setPort(m_jobToScheduleStatement->getInt(16));
    result->setClientVersion((u_signed64)m_jobToScheduleStatement->getDouble(17));
    result->setClientType((u_signed64)m_jobToScheduleStatement->getDouble(18));
    result->setJobId(0);

    /* For statistics */
    timeval tv;
    gettimeofday(&tv, NULL);
    result->setSelectTime(tv.tv_sec * 1000000 + tv.tv_usec);
    result->setSubmitStartTime(0);
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in jobToSchedule."
      << std::endl << e.getMessage();
    throw ex;
  }
  return NULL;
}


//-----------------------------------------------------------------------------
// updateSchedulerJob
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
      << "status to " << status
      << std::endl << e.getMessage();
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// fileSystemStates
//-----------------------------------------------------------------------------
void castor::jobmanager::ora::OraJobManagerSvc::fileSystemStates
(std::map<std::string, castor::stager::FileSystemStatusCodes> &result)
  throw(castor::exception::Exception) {

  // Initialize statements
  try {
    if (m_fileSystemStatesStatement == NULL) {
      m_fileSystemStatesStatement = createStatement(s_fileSystemStatesString);
      m_fileSystemStatesStatement->setAutoCommit(true);
    }

    // Run through the cursor
    oracle::occi::ResultSet *rs = m_fileSystemStatesStatement->executeQuery();
    while (oracle::occi::ResultSet::END_OF_FETCH != rs->next()) {
      castor::stager::FileSystemStatusCodes status = 
	(castor::stager::FileSystemStatusCodes)rs->getDouble(4);
      if (rs->getDouble(3) == castor::stager::DISKSERVER_DISABLED) {
	status = castor::stager::FILESYSTEM_DISABLED;
      }

      // We store the status under a diskserver:filesystem combination
      std::ostringstream key(rs->getString(1));
      key << ":" << rs->getString(2);
      result[key.str()] = status;
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in fileSystemsStates."
      << std::endl << e.getMessage();
    throw ex;
  }
}

