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
 * @(#)$RCSfile: IJobSvcCInt.cpp,v $ $Revision: 1.16 $ $Release$ $Date: 2008/09/22 13:31:14 $ $Author: waldron $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/stager/IJobSvc.hpp"
#include "castor/Services.hpp"
#include <errno.h>
#include <string>

extern "C" {

  // C include files
#include "castor/ServicesCInt.hpp"
#include "castor/stager/IJobSvcCInt.hpp"

  //----------------------------------------------------------------------------
  // checkIJobSvc
  //----------------------------------------------------------------------------
  bool checkIJobSvc(Cstager_IJobSvc_t* jobSvc) {
    if (0 == jobSvc || 0 == jobSvc->jobSvc) {
      errno = EINVAL;
      return false;
    }
    return true;
  }

  //----------------------------------------------------------------------------
  // Cstager_IJobSvc_fromIService
  //----------------------------------------------------------------------------
  Cstager_IJobSvc_t*
  Cstager_IJobSvc_fromIService(castor::IService* obj) {
    Cstager_IJobSvc_t* jobSvc = new Cstager_IJobSvc_t();
    jobSvc->errorMsg = "";
    jobSvc->jobSvc = dynamic_cast<castor::stager::IJobSvc*>(obj);
    return jobSvc;
  }

  //----------------------------------------------------------------------------
  // Cstager_IJobSvc_delete
  //----------------------------------------------------------------------------
  int Cstager_IJobSvc_delete(Cstager_IJobSvc_t* jobSvc) {
    try {
      if (0 == jobSvc) return 0;
      if (0 != jobSvc->jobSvc) jobSvc->jobSvc->release();
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    delete jobSvc;
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_getUpdateStart
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_getUpdateStart
  (struct Cstager_IJobSvc_t* jobSvc,
   castor::stager::SubRequest* subreq,
   castor::stager::FileSystem* fileSystem,
   int *emptyFile,
   castor::stager::DiskCopy** diskCopy,
   u_signed64 fileId,
   const char* nsHost) {
    if (!checkIJobSvc(jobSvc)) return -1;
    try {
      bool ef;
      std::list<castor::stager::DiskCopyForRecall*> sourceslist;
      *diskCopy = jobSvc->jobSvc->getUpdateStart
        (subreq, fileSystem, &ef, fileId, nsHost);
      *emptyFile = ef ? 1 : 0;
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_putStart
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_putStart
  (struct Cstager_IJobSvc_t* jobSvc,
   castor::stager::SubRequest* subreq,
   castor::stager::FileSystem* fileSystem,
   castor::stager::DiskCopy** diskCopy,
   u_signed64 fileId,
   const char* nsHost) {
    if (!checkIJobSvc(jobSvc)) return -1;
    try {
      *diskCopy =
        jobSvc->jobSvc->putStart(subreq, fileSystem, fileId, nsHost);
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_errorMsg
  //-------------------------------------------------------------------------
  const char* Cstager_IJobSvc_errorMsg(Cstager_IJobSvc_t* jobSvc) {
    return jobSvc->errorMsg.c_str();
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_prepareForMigration
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_prepareForMigration
  (struct Cstager_IJobSvc_t* jobSvc,
   u_signed64 subReqId,
   u_signed64 fileSize,
   u_signed64 timeStamp,
   u_signed64 fileId,
   const char* nsHost,
   const char* csumtype,
   const char* csumvalue) {
    if (!checkIJobSvc(jobSvc)) return -1;
    try {
      jobSvc->jobSvc->prepareForMigration
	(subReqId, fileSize, timeStamp, fileId, nsHost, csumtype, csumvalue);
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_getUpdateDone
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_getUpdateDone
  (struct Cstager_IJobSvc_t* jobSvc,
   u_signed64 subReqId,
   u_signed64 fileId,
   const char* nsHost) {
    if (!checkIJobSvc(jobSvc)) return -1;
    try {
      jobSvc->jobSvc->getUpdateDone(subReqId,fileId,nsHost);
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_getUpdateFailed
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_getUpdateFailed
  (struct Cstager_IJobSvc_t* jobSvc,
   u_signed64 subReqId,
   u_signed64 fileId,
   const char* nsHost) {
    if (!checkIJobSvc(jobSvc)) return -1;
    try {
      jobSvc->jobSvc->getUpdateFailed(subReqId, fileId, nsHost);
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_putFailed
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_putFailed
  (struct Cstager_IJobSvc_t* jobSvc,
   u_signed64 subReqId,
   u_signed64 fileId,
   const char* nsHost) {
    if (!checkIJobSvc(jobSvc)) return -1;
    try {
      jobSvc->jobSvc->putFailed(subReqId, fileId, nsHost);
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //-------------------------------------------------------------------------
  // Cstager_IJobSvc_firstByteWritten
  //-------------------------------------------------------------------------
  int Cstager_IJobSvc_firstByteWritten
  (struct Cstager_IJobSvc_t* jobSvc,
   u_signed64 subRequestId,
   u_signed64 fileId,
   const char* nsHost) {
    if (!checkIJobSvc(jobSvc)) return -1;
    try {
      jobSvc->jobSvc->firstByteWritten(subRequestId, fileId, nsHost);
    } catch (castor::exception::Exception& e) {
      serrno = e.code();
      jobSvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }
}
