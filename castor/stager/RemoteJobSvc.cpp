/******************************************************************************
 *                      RemoteJobSvc.cpp
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
 * @(#)$RCSfile: RemoteJobSvc.cpp,v $ $Revision: 1.25 $ $Release$ $Date: 2009/02/10 09:19:57 $ $Author: itglp $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IService.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/client/BasicResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/FilesDeletionFailed.hpp"
#include "castor/stager/GetUpdateDone.hpp"
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/PutFailed.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/stager/GCFile.hpp"
#include "castor/stager/RemoteJobSvc.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/Disk2DiskCopyDoneRequest.hpp"
#include "castor/stager/Disk2DiskCopyStartRequest.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/stager/FirstByteWritten.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/rh/GCFilesResponse.hpp"
#include "castor/rh/StartResponse.hpp"
#include "castor/rh/Disk2DiskCopyStartResponse.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/exception/Internal.hpp"
#include <errno.h>
#include <list>

EXTERN_C char *getconfent (char *, char *, int);

//------------------------------------------------------------------------------
// Constants
//------------------------------------------------------------------------------
const char* castor::stager::RMTJOBSVC_CATEGORY_CONF = "REMOTEJOBSVC";
const char* castor::stager::TIMEOUT_CONF = "TIMEOUT";
const int   castor::stager::DEFAULT_REMOTEJOBSVC_TIMEOUT = 1800;


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::stager::RemoteJobSvc>* s_factoryRemoteJobSvc =
  new castor::SvcFactory<castor::stager::RemoteJobSvc>();

//------------------------------------------------------------------------------
// RemoteJobSvc
//------------------------------------------------------------------------------
castor::stager::RemoteJobSvc::RemoteJobSvc(const std::string name) :
  BaseSvc(name) {}

//------------------------------------------------------------------------------
// ~RemoteJobSvc
//------------------------------------------------------------------------------
castor::stager::RemoteJobSvc::~RemoteJobSvc() throw() {}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::stager::RemoteJobSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::stager::RemoteJobSvc::ID() {
  return castor::SVC_REMOTEJOBSVC;
}

//------------------------------------------------------------------------------
// selectTape
//------------------------------------------------------------------------------
castor::stager::Tape*
castor::stager::RemoteJobSvc::selectTape
(const std::string, const int, const int)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteJobSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

//------------------------------------------------------------------------------
// requestToDo
//------------------------------------------------------------------------------
castor::stager::Request*
castor::stager::RemoteJobSvc::requestToDo(std::string)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteJobSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

//------------------------------------------------------------------------------
// GetUpdateStartResponseHandler
//------------------------------------------------------------------------------

/**
 * A dedicated little response handler for the GetUpdateStart
 * requests
 */
class GetUpdateStartResponseHandler : public castor::client::IResponseHandler {
public:
  GetUpdateStartResponseHandler
  (castor::stager::DiskCopy** diskCopy,
   bool *emptyFile) :
    m_diskCopy(diskCopy),
    m_emptyFile(emptyFile){}

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    castor::rh::GetUpdateStartResponse *resp =
      dynamic_cast<castor::rh::GetUpdateStartResponse*>(&r);
    if (0 != resp->errorCode()) {
      castor::exception::Exception e(resp->errorCode());
      e.getMessage() << resp->errorMessage();
      throw e;
    }
    *m_diskCopy = resp->diskCopy();
    *m_emptyFile = resp->emptyFile();
  };
  virtual void terminate()
    throw (castor::exception::Exception) {};
private:
  // Where to store the diskCopy
  castor::stager::DiskCopy** m_diskCopy;
  // Where to store the emptyFile flag
  bool* m_emptyFile;
};

//------------------------------------------------------------------------------
// getUpdateStart
//------------------------------------------------------------------------------
castor::stager::DiskCopy*
castor::stager::RemoteJobSvc::getUpdateStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 bool* emptyFile,
 u_signed64 fileId,
 const std::string nsHost)
  throw (castor::exception::Exception) {
  // placeholders for the result
  castor::stager::DiskCopy* result;
  // Build a response Handler
  GetUpdateStartResponseHandler rh
    (&result, emptyFile);
  // Build the GetUpdateStartRequest
  castor::stager::GetUpdateStartRequest req;
  req.setSubreqId(subreq->id());
  req.setDiskServer(fileSystem->diskserver()->name());
  req.setFileSystem(fileSystem->mountPoint());
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
  // return
  return result;
}

//------------------------------------------------------------------------------
// putStartResponseHandler
//------------------------------------------------------------------------------

/**
 * A dedicated little response handler for the PutStart
 * requests
 */
class PutStartResponseHandler : public castor::client::IResponseHandler {
public:
  PutStartResponseHandler (castor::stager::DiskCopy** diskCopy) :
    m_diskCopy(diskCopy) {}

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    if (0 != r.errorCode()) {
      castor::exception::Exception e(r.errorCode());
      e.getMessage() << r.errorMessage();
      throw e;
    }
    castor::rh::StartResponse *resp =
      dynamic_cast<castor::rh::StartResponse*>(&r);
    if (0 == resp) {
      castor::exception::Internal e;
      e.getMessage() << "PutStartResponseHandler ; invalid repsonse type\n"
		     << "Was expecting StartResponse, got "
		     << castor::ObjectsIdStrings[r.type()];
      throw e;
    }
    *m_diskCopy = resp->diskCopy();
  };
  virtual void terminate()
    throw (castor::exception::Exception) {};
private:
  // Where to store the diskCopy
  castor::stager::DiskCopy** m_diskCopy;
};

//------------------------------------------------------------------------------
// putStart
//------------------------------------------------------------------------------
castor::stager::DiskCopy*
castor::stager::RemoteJobSvc::putStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 u_signed64 fileId,
 const std::string nsHost)
  throw (castor::exception::Exception) {
  // placeholders for the result
  castor::stager::DiskCopy* result;
  // Build a response Handler
  PutStartResponseHandler rh(&result);
  // Build the PutStartRequest
  castor::stager::PutStartRequest req;
  req.setSubreqId(subreq->id());
  req.setDiskServer(fileSystem->diskserver()->name());
  req.setFileSystem(fileSystem->mountPoint());
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
  // return
  return result;
}

//------------------------------------------------------------------------------
// DiskCopyStartResponseHandler
//------------------------------------------------------------------------------

/**
 * A dedicated little response handler for the Disk2DiskCopyStart
 * requests
 */
class DiskCopyStartResponseHandler : public castor::client::IResponseHandler {
public:
  DiskCopyStartResponseHandler (castor::stager::DiskCopyInfo* &diskCopy,
				castor::stager::DiskCopyInfo* &sourceDiskCopy) :
    m_diskCopy(diskCopy),
    m_sourceDiskCopy(sourceDiskCopy) {}

  /// Handle the response
  virtual void handleResponse(castor::rh::Response& r)
    throw(castor::exception::Exception) {
    if (0 != r.errorCode()) {
      castor::exception::Exception e(r.errorCode());
      e.getMessage() << r.errorMessage();
      throw e;
    }
    castor::rh::Disk2DiskCopyStartResponse *resp =
      dynamic_cast<castor::rh::Disk2DiskCopyStartResponse*>(&r);
    if (0 == resp) {
      castor::exception::Internal e;
      e.getMessage() << "Could not cast response into Disk2DiskCopyStartResponse";
      throw e;
    }

    m_diskCopy = resp->diskCopy();
    m_sourceDiskCopy = resp->sourceDiskCopy();
  }

  /// Not implemented
  virtual void terminate()
    throw(castor::exception::Exception) {};

private:

  // Where to store the destination disk copy
  castor::stager::DiskCopyInfo* &m_diskCopy;

  // Where to store the source disk copy
  castor::stager::DiskCopyInfo* &m_sourceDiskCopy;
};

//------------------------------------------------------------------------------
// disk2DiskCopyStart
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::disk2DiskCopyStart
(const u_signed64 diskCopyId,
 const u_signed64 sourceDiskCopyId,
 const std::string diskServer,
 const std::string fileSystem,
 castor::stager::DiskCopyInfo* &diskCopy,
 castor::stager::DiskCopyInfo* &sourceDiskCopy,
 u_signed64 fileId,
 const std::string nsHost)
  throw(castor::exception::Exception) {
  // Build the Disk2DiskCopyStartRequest
  castor::stager::Disk2DiskCopyStartRequest req;
  req.setDiskCopyId(diskCopyId);
  req.setSourceDiskCopyId(sourceDiskCopyId);
  req.setDiskServer(diskServer);
  req.setMountPoint(fileSystem);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Build a response Handler
  DiskCopyStartResponseHandler rh(diskCopy, sourceDiskCopy);
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}

//------------------------------------------------------------------------------
// disk2DiskCopyDone
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::disk2DiskCopyDone
(u_signed64 diskCopyId,
 u_signed64 sourceDiskCopyId,
 u_signed64 fileId,
 const std::string nsHost,
 u_signed64 replicaFileSize)
  throw (castor::exception::Exception) {
  // Build the Disk2DiskCopyDoneRequest
  castor::stager::Disk2DiskCopyDoneRequest req;
  req.setDiskCopyId(diskCopyId);
  req.setSourceDiskCopyId(sourceDiskCopyId);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  req.setReplicaFileSize(replicaFileSize);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}

//------------------------------------------------------------------------------
// disk2DiskCopyFailed
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::disk2DiskCopyFailed
(u_signed64 diskCopyId, bool, u_signed64 fileId, const std::string nsHost)
  throw (castor::exception::Exception) {
  // Build the Disk2DiskCopyDoneRequest; the enoent parameter is irrelevant here
  castor::stager::Disk2DiskCopyDoneRequest req;
  req.setDiskCopyId(diskCopyId);
  req.setSourceDiskCopyId(0);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}

//
//------------------------------------------------------------------------------
// prepareForMigration
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::prepareForMigration
(u_signed64 subReqId,
 u_signed64 fileSize,
 u_signed64 timeStamp,
 u_signed64 fileId,
 const std::string nsHost,
 const std::string csumtype,
 const std::string csumvalue)
  throw (castor::exception::Exception) {
  // Build the MoverCloseRequest
  castor::stager::MoverCloseRequest req;
  req.setSubReqId(subReqId);
  req.setFileSize(fileSize);
  req.setTimeStamp(timeStamp);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  req.setCsumType(csumtype);
  req.setCsumValue(csumvalue);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}
//------------------------------------------------------------------------------
// getRemoteJobClientTimeout
//------------------------------------------------------------------------------
int castor::stager::RemoteJobSvc::getRemoteJobClientTimeout() {

  int ret_timeout = castor::stager::DEFAULT_REMOTEJOBSVC_TIMEOUT;

  char *strtimeout = getconfent((char *)castor::stager::RMTJOBSVC_CATEGORY_CONF,
				(char *)castor::stager::TIMEOUT_CONF,
				0);
  if (strtimeout != 0) {
    char* dp = strtimeout;
    ret_timeout = strtoul(strtimeout, &dp, 0);
    if (*dp != 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Bad RemoteJobSvc timeout value:" << strtimeout << std::endl;
      throw e;
    }
  }

  return ret_timeout;
}

//------------------------------------------------------------------------------
// getUpdateDone
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::getUpdateDone
(u_signed64 subReqId,
 u_signed64 fileId,
 const std::string nsHost)
   throw (castor::exception::Exception) {
  // Build the GetUpdateDoneRequest
  castor::stager::GetUpdateDone req;
  req.setSubReqId(subReqId);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}

//------------------------------------------------------------------------------
// getUpdateFailed
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::getUpdateFailed
(u_signed64 subReqId,
 u_signed64 fileId,
 const std::string nsHost)
  throw (castor::exception::Exception) {
  // Build the GetUpdateFailedRequest
  castor::stager::GetUpdateFailed req;
  req.setSubReqId(subReqId);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}

//------------------------------------------------------------------------------
// putFailed
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::putFailed
(u_signed64 subReqId,
 u_signed64 fileId,
 const std::string nsHost)
  throw (castor::exception::Exception) {
  // Build the PutFailedRequest
  castor::stager::PutFailed req;
  req.setSubReqId(subReqId);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}

//------------------------------------------------------------------------------
// firstByteWritten
//------------------------------------------------------------------------------
void castor::stager::RemoteJobSvc::firstByteWritten
(u_signed64 subRequestId,
 u_signed64 fileId,
 const std::string nsHost)
  throw (castor::exception::Exception) {
  // Build the FirstByteWrittenRequest
  castor::stager::FirstByteWritten req;
  req.setSubReqId(subRequestId);
  req.setFileId(fileId);
  req.setNsHost(nsHost);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteJobClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
}
