/******************************************************************************
 *                      RemoteGCSvc.cpp
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
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IService.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/client/BasicResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/NsFilesDeleted.hpp"
#include "castor/stager/NsFilesDeletedResponse.hpp"
#include "castor/stager/StgFilesDeleted.hpp"
#include "castor/stager/StgFilesDeletedResponse.hpp"
#include "castor/stager/FilesDeletionFailed.hpp"
#include "castor/stager/GetUpdateDone.hpp"
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/PutFailed.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/stager/GCFile.hpp"
#include "castor/stager/RemoteGCSvc.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/rh/GCFilesResponse.hpp"
#include "castor/rh/StartResponse.hpp"
#include "castor/exception/NotSupported.hpp"
#include <errno.h>
#include <list>
#include <vector>

EXTERN_C char *getconfent (char *, char *, int);

//------------------------------------------------------------------------------
// Constants
//------------------------------------------------------------------------------
const char* castor::stager::RMTGCSVC_CATEGORY_CONF = "REMOTEGCSVC";
//const char* castor::stager::TIMEOUT_CONF = "TIMEOUT";   // already defined
const int   castor::stager::DEFAULT_REMOTEGCSVC_TIMEOUT = 1800;


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::stager::RemoteGCSvc>
  s_factoryRemoteGCSvc;

//------------------------------------------------------------------------------
// RemoteGCSvc
//------------------------------------------------------------------------------
castor::stager::RemoteGCSvc::RemoteGCSvc(const std::string name) :
  BaseSvc(name) {}

//------------------------------------------------------------------------------
// ~RemoteGCSvc
//------------------------------------------------------------------------------
castor::stager::RemoteGCSvc::~RemoteGCSvc() throw() {}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::stager::RemoteGCSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::stager::RemoteGCSvc::ID() {
  return castor::SVC_REMOTEGCSVC;
}

//------------------------------------------------------------------------------
// requestToDo
//------------------------------------------------------------------------------
castor::stager::Request*
castor::stager::RemoteGCSvc::requestToDo(std::string)
   {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

//------------------------------------------------------------------------------
// getRemoteGCClientTimeout
//------------------------------------------------------------------------------
int castor::stager::RemoteGCSvc::getRemoteGCClientTimeout() {

  int ret_timeout = castor::stager::DEFAULT_REMOTEGCSVC_TIMEOUT;

  char *strtimeout = getconfent((char *)castor::stager::RMTGCSVC_CATEGORY_CONF,
				(char *)castor::stager::TIMEOUT_CONF,
				0);
  if (strtimeout != 0) {
    char* dp = strtimeout;
    ret_timeout = strtoul(strtimeout, &dp, 0);
    if (*dp != 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Bad RemoteGCSvc timeout value:" << strtimeout << std::endl;
      throw e;
    }
  }

  return ret_timeout;
}

//------------------------------------------------------------------------------
// Files2DeleteResponseHandler
//------------------------------------------------------------------------------
/**
 * A dedicated little response handler for the Files2Delete
 * requests
 */
class Files2DeleteResponseHandler : public castor::client::IResponseHandler {
public:
  Files2DeleteResponseHandler
  (std::vector<castor::stager::GCLocalFile*>* result) :
    m_result(result) {}

  virtual void handleResponse(castor::rh::Response& r)
     {
    if (0 != r.errorCode()) {
      castor::exception::Exception e(r.errorCode());
      e.getMessage() << r.errorMessage();
      throw e;
    }
    castor::rh::GCFilesResponse *resp =
      dynamic_cast<castor::rh::GCFilesResponse*>(&r);
    if (0 == resp) {
      castor::exception::Exception e;
      e.getMessage() << "Could not cast response into GCFilesResponse";
      throw e;
    }
    for (std::vector<castor::stager::GCLocalFile*>::iterator
           it = resp->files().begin();
         it != resp->files().end();
         it++) {
      // Here we transfer the ownership of the GCLocalFile
      // from resp to m_result. So the resp can be cleared
      // without any memory leak. It is even mandatory.
      m_result->push_back(*it);
    }
    // we clear the response as explained above
    resp->files().clear();
  };
  virtual void terminate()
     {};
private:
  // where to store the diskCopy
  std::vector<castor::stager::GCLocalFile*>* m_result;
};

//------------------------------------------------------------------------------
// selectFiles2Delete
//------------------------------------------------------------------------------
std::vector<castor::stager::GCLocalFile*>*
castor::stager::RemoteGCSvc::selectFiles2Delete
(std::string diskServer)
   {
  // Build the Files2DeleteRequest
  castor::stager::Files2Delete req;
  req.setDiskServer(diskServer);
  // Prepare a result vector
  std::vector<castor::stager::GCLocalFile*>* result =
      new std::vector<castor::stager::GCLocalFile*>;
  // Build a response Handler
  Files2DeleteResponseHandler rh(result);
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteGCClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
  return result;
}

//------------------------------------------------------------------------------
// filesDeleted
//------------------------------------------------------------------------------
void castor::stager::RemoteGCSvc::filesDeleted
(std::vector<u_signed64*>& diskCopyIds)
   {
  // Build the FilesDeletedRequest
  castor::stager::FilesDeleted req;
  for (std::vector<u_signed64*>::iterator it = diskCopyIds.begin();
       it != diskCopyIds.end();
       it++) {
    castor::stager::GCFile* file =
      new castor::stager::GCFile;
    file->setDiskCopyId(**it);
    file->setRequest(&req);
    // Here the owner ship of files[i] is transmitted to req !
    req.files().push_back(file);
  }
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteGCClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
  // no need to cleanup files since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
}

//------------------------------------------------------------------------------
// filesDeletionFailed
//------------------------------------------------------------------------------
void castor::stager::RemoteGCSvc::filesDeletionFailed
(std::vector<u_signed64*>& diskCopyIds)
   {
  // Build the FilesDeletionFailedRequest
  castor::stager::FilesDeletionFailed req;
  for (std::vector<u_signed64*>::iterator it = diskCopyIds.begin();
       it != diskCopyIds.end();
       it++) {
    castor::stager::GCFile* file =
      new castor::stager::GCFile;
    file->setDiskCopyId(**it);
    file->setRequest(&req);
    // Here the owner ship of files[i] is transmitted to req !
    req.files().push_back(file);
  }
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteGCClientTimeout());
  client.setOptions(0);
  client.sendRequest(&req, &rh);
  // no need to cleanup files since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
}

//------------------------------------------------------------------------------
// NsFilesDeletedResponseHandler
//------------------------------------------------------------------------------
/**
 * A dedicated little response handler for the nsFilesDeleted
 * requests
 */
class NsFilesDeletedResponseHandler : public castor::client::IResponseHandler {
public:
  NsFilesDeletedResponseHandler
  (std::vector<u_signed64>* result) :
    m_result(result) {}

  virtual void handleResponse(castor::rh::Response& r)
     {
    if (0 != r.errorCode()) {
      castor::exception::Exception e(r.errorCode());
      e.getMessage() << r.errorMessage();
      throw e;
    }
    castor::stager::NsFilesDeletedResponse *resp =
      dynamic_cast<castor::stager::NsFilesDeletedResponse*>(&r);
    if (0 == resp) {
      castor::exception::Exception e;
      e.getMessage() << "Could not cast response into NsFilesDeletedResponse";
      throw e;
    }
    for (std::vector<castor::stager::GCFile*>::iterator
           it = resp->orphanFileIds().begin();
         it != resp->orphanFileIds().end();
         it++) {
      // we are pushing fileids here, despite the function name
      // This is a bad object reuse, I apologize...
      m_result->push_back((*it)->diskCopyId());
    }
    // we clear the response
    resp->orphanFileIds().clear();
  };
  virtual void terminate()
     {};
private:
  // where to store the list of files not found
  std::vector<u_signed64>* m_result;
};

//------------------------------------------------------------------------------
// nsFilesDeleted
//------------------------------------------------------------------------------
std::vector<u_signed64> castor::stager::RemoteGCSvc::nsFilesDeleted
(std::vector<u_signed64> &fileIds,
 std::string nsHost) throw() {
  // Build the nsFilesDeleted Request
  castor::stager::NsFilesDeleted req;
  req.setNsHost(nsHost);
  for (std::vector<u_signed64>::iterator it = fileIds.begin();
       it != fileIds.end();
       it++) {
    castor::stager::GCFile* file = new castor::stager::GCFile;
    file->setDiskCopyId(*it);
    file->setRequest(&req);
    // Here the owner ship of files[i] is transmitted to req !
    req.files().push_back(file);
  }
  // Prepare a result vector
  std::vector<u_signed64> result;
  try {
    // Build a response Handler
    NsFilesDeletedResponseHandler rh(&result);
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(getRemoteGCClientTimeout());
    client.setOptions(0);
    client.sendRequest(&req, &rh);
  } catch (castor::exception::Exception& e) {
    // Exception caught in RemoteGCSvc::nsFilesDeleted
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Error", sstrerror(e.code()))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_STAGERLIB + 0, 2, params);
  }
  // no need to cleanup fileIds since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
  return result;
}

//------------------------------------------------------------------------------
// StgFilesDeletedResponseHandler
//------------------------------------------------------------------------------
/**
 * A dedicated little response handler for the stgFilesDeleted
 * requests
 */
class StgFilesDeletedResponseHandler : public castor::client::IResponseHandler {
public:
  StgFilesDeletedResponseHandler
  (std::vector<u_signed64>* result) :
    m_result(result) {}

  virtual void handleResponse(castor::rh::Response& r)
     {
    if (0 != r.errorCode()) {
      castor::exception::Exception e(r.errorCode());
      e.getMessage() << r.errorMessage();
      throw e;
    }
    castor::stager::StgFilesDeletedResponse *resp =
      dynamic_cast<castor::stager::StgFilesDeletedResponse*>(&r);
    if (0 == resp) {
      castor::exception::Exception e;
      e.getMessage() << "Could not cast response into StgFilesDeletedResponse";
      throw e;
    }
    for (std::vector<castor::stager::GCFile*>::iterator
           it = resp->orphanFileIds().begin();
         it != resp->orphanFileIds().end();
         it++) {
      // we are pushing fileids here, despite the function name
      // This is a bad object reuse, I apologize...
      m_result->push_back((*it)->diskCopyId());
    }
    // we clear the response
    resp->orphanFileIds().clear();
  };
  virtual void terminate()
     {};
private:
  // where to store the list of files not found
  std::vector<u_signed64>* m_result;
};

//------------------------------------------------------------------------------
// stgFilesDeleted
//------------------------------------------------------------------------------
std::vector<u_signed64> castor::stager::RemoteGCSvc::stgFilesDeleted
(std::vector<u_signed64> &diskCopyIds,
 std::string nsHost) throw() {
  // Build the stgFilesDeleted Request
  castor::stager::StgFilesDeleted req;
  req.setNsHost(nsHost);
  for (std::vector<u_signed64>::iterator it = diskCopyIds.begin();
       it != diskCopyIds.end();
       it++) {
    castor::stager::GCFile* file = new castor::stager::GCFile;
    file->setDiskCopyId(*it);
    file->setRequest(&req);
    // Here the owner ship of files[i] is transmitted to req !
    req.files().push_back(file);
  }
  // Prepare a result vector
  std::vector<u_signed64> result;
  try {
    // Build a response Handler
    StgFilesDeletedResponseHandler rh(&result);
    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(getRemoteGCClientTimeout());
    client.setOptions(0);
    client.sendRequest(&req, &rh);
  } catch (castor::exception::Exception& e) {
    // Exception caught in RemoteGCSvc::stgFilesDeleted
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Error", sstrerror(e.code()))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_STAGERLIB + 1, 2, params);
  }
  // no need to cleanup fileIds since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
  return result;
}

// -----------------------------------------------------------------------
// dumpCleanupLogs
// -----------------------------------------------------------------------
void castor::stager::RemoteGCSvc::dumpCleanupLogs()
   {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// removeTerminatedRequests
// -----------------------------------------------------------------------
void castor::stager::RemoteGCSvc::removeTerminatedRequests()
   {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

