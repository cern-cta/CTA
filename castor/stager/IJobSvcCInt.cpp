/******************************************************************************
 *                      IJobSvcCInt.cpp
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
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/stager/IJobSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include <string>

extern "C" {

  //----------------------------------------------------------------------------
  // getIJobSvc
  //----------------------------------------------------------------------------
  castor::stager::IJobSvc* getIJobSvc() {
    castor::IService *remsvc = castor::BaseObject::services()->service
      ("RemoteJobSvc", castor::SVC_REMOTEJOBSVC);
    if (remsvc == 0) {
      castor::exception::Internal e;
      e.getMessage() << "Unable to get RemoteJobSvc";
      throw e;
    }
    castor::stager::IJobSvc *jobSvc = dynamic_cast<castor::stager::IJobSvc *>(remsvc);
    if (jobSvc == 0) {
      castor::exception::Internal e;
      e.getMessage() << "Could not convert newly retrieved service into IJobSvc";
      throw e;
    }
    return jobSvc;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_prepareForMigration
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_prepareForMigration
  (u_signed64 subReqId,
   u_signed64 fileSize,
   u_signed64 timeStamp,
   u_signed64 fileId,
   const char* nsHost,
   const char* csumtype,
   const char* csumvalue,
   int* errorCode,
   char** errorMsg) {
    try {
      castor::stager::IJobSvc* jobSvc = getIJobSvc();
      jobSvc->prepareForMigration
	(subReqId, fileSize, timeStamp, fileId, nsHost, csumtype, csumvalue);
    } catch (castor::exception::Exception& e) {
      *errorCode = e.code();
      *errorMsg = strdup(e.getMessage().str().c_str());
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_getUpdateDone
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_getUpdateDone
  (u_signed64 subReqId,
   u_signed64 fileId,
   const char* nsHost,
   int* errorCode,
   char** errorMsg) {
    try {
      castor::stager::IJobSvc* jobSvc = getIJobSvc();
      jobSvc->getUpdateDone(subReqId,fileId,nsHost);
    } catch (castor::exception::Exception& e) {
      *errorCode = e.code();
      *errorMsg = strdup(e.getMessage().str().c_str());
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_getUpdateFailed
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_getUpdateFailed
  (u_signed64 subReqId,
   u_signed64 fileId,
   const char* nsHost,
   int* errorCode,
   char** errorMsg) {
    try {
      castor::stager::IJobSvc* jobSvc = getIJobSvc();
      jobSvc->getUpdateFailed(subReqId, fileId, nsHost);
    } catch (castor::exception::Exception& e) {
      *errorCode = e.code();
      *errorMsg = strdup(e.getMessage().str().c_str());
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_putFailed
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_putFailed
  (u_signed64 subReqId,
   u_signed64 fileId,
   const char* nsHost,
   int* errorCode,
   char** errorMsg) {
    try {
      castor::stager::IJobSvc* jobSvc = getIJobSvc();
      jobSvc->putFailed(subReqId, fileId, nsHost);
    } catch (castor::exception::Exception& e) {
      *errorCode = e.code();
      *errorMsg = strdup(e.getMessage().str().c_str());
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_firstByteWritten
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_firstByteWritten
  (u_signed64 subRequestId,
   u_signed64 fileId,
   const char* nsHost,
   int* errorCode,
   char** errorMsg) {
    try {
      castor::stager::IJobSvc* jobSvc = getIJobSvc();
      jobSvc->firstByteWritten(subRequestId, fileId, nsHost);
    } catch (castor::exception::Exception& e) {
      *errorCode = e.code();
      *errorMsg = strdup(e.getMessage().str().c_str());
      return -1;
    }
    return 0;
  }
}
