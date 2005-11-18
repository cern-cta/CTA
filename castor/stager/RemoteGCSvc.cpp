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
 * @(#)$RCSfile: RemoteGCSvc.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2005/11/18 16:54:05 $ $Author: sponcec3 $
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
#include "castor/stager/RemoteGCSvc.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/PutDoneStart.hpp"
#include "castor/stager/Disk2DiskCopyDoneRequest.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/rh/GCFilesResponse.hpp"
#include "castor/rh/StartResponse.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/exception/Internal.hpp"
#include <errno.h>
#include <list>

EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));

//------------------------------------------------------------------------------
// Constants
//------------------------------------------------------------------------------
const char* castor::stager::RMTGCSVC_CATEGORY_CONF = "REMOTEGCSVC";
//const char* castor::stager::TIMEOUT_CONF = "TIMEOUT";   // already defined
const int   castor::stager::DEFAULT_REMOTEGCSVC_TIMEOUT = 1800;


// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::stager::RemoteGCSvc>* s_factoryRemoteGCSvc =
  new castor::SvcFactory<castor::stager::RemoteGCSvc>();

// -----------------------------------------------------------------------
// RemoteGCSvc
// -----------------------------------------------------------------------
castor::stager::RemoteGCSvc::RemoteGCSvc(const std::string name) :
  BaseSvc(name) {}

// -----------------------------------------------------------------------
// ~RemoteGCSvc
// -----------------------------------------------------------------------
castor::stager::RemoteGCSvc::~RemoteGCSvc() throw() {}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::stager::RemoteGCSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::stager::RemoteGCSvc::ID() {
  return castor::SVC_REMOTEGCSVC;
}

// -----------------------------------------------------------------------
// selectTape
// -----------------------------------------------------------------------
castor::stager::Tape*
castor::stager::RemoteGCSvc::selectTape
(const std::string vid,
 const int side,
 const int tpmode)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// requestToDo
// -----------------------------------------------------------------------
castor::stager::Request*
castor::stager::RemoteGCSvc::requestToDo()
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectSvcClass
// -----------------------------------------------------------------------
castor::stager::SvcClass*
castor::stager::RemoteGCSvc::selectSvcClass
(const std::string name)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectFileClass
// -----------------------------------------------------------------------
castor::stager::FileClass*
castor::stager::RemoteGCSvc::selectFileClass
(const std::string name)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectFileSystem
// -----------------------------------------------------------------------
castor::stager::FileSystem*
castor::stager::RemoteGCSvc::selectFileSystem
(const std::string mountPoint,
 const std::string diskServer)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteGCSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// getRemoteGCClientTimeout
// -----------------------------------------------------------------------
int castor::stager::RemoteGCSvc::getRemoteGCClientTimeout() {

  int ret_timeout = castor::stager::DEFAULT_REMOTEGCSVC_TIMEOUT;

  char *strtimeout = getconfent((char *)castor::stager::RMTGCSVC_CATEGORY_CONF,
				(char *)castor::stager::TIMEOUT_CONF, 
				0);
  if (strtimeout != 0) {
    char* dp = strtimeout;
    int itimeout = strtoul(strtimeout, &dp, 0);
    if (*dp != 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Bad RemoteGCSvc timeout value:" << strtimeout << std::endl;
      throw e;
    }
  }

  return ret_timeout;
}

// -----------------------------------------------------------------------
// Files2DeleteResponseHandler
// -----------------------------------------------------------------------
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
    throw (castor::exception::Exception) {
    if (0 != r.errorCode()) {
      castor::exception::Exception e(r.errorCode());
      e.getMessage() << r.errorMessage();
      throw e;
    }
    castor::rh::GCFilesResponse *resp =
      dynamic_cast<castor::rh::GCFilesResponse*>(&r);
    if (0 == resp) {
      castor::exception::Internal e;
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
    throw (castor::exception::Exception) {};
private:
  // where to store the diskCopy
  std::vector<castor::stager::GCLocalFile*>* m_result;
};

// -----------------------------------------------------------------------
// selectFiles2Delete
// -----------------------------------------------------------------------
std::vector<castor::stager::GCLocalFile*>*
castor::stager::RemoteGCSvc::selectFiles2Delete
(std::string diskServer)
  throw (castor::exception::Exception) {
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
  client.sendRequest(&req, &rh);
  return result;
}

// -----------------------------------------------------------------------
// filesDeleted
// -----------------------------------------------------------------------
void castor::stager::RemoteGCSvc::filesDeleted
(std::vector<u_signed64*>& diskCopyIds)
  throw (castor::exception::Exception) {
  // Build the FilesDeletedRequest
  castor::stager::FilesDeleted req;
  int i = 0;
  for (std::vector<u_signed64*>::iterator it = diskCopyIds.begin();
       it != diskCopyIds.end();
       it++) {
    castor::stager::GCFile* file =
      new castor::stager::GCFile;
    file->setDiskCopyId(**it);
    file->setRequest(&req);
    // Here the owner ship of files[i] is transmitted to req !
    req.files().push_back(file);
    i++;
  }
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteGCClientTimeout());
  client.sendRequest(&req, &rh);
  // no need to cleanup files since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
}

// -----------------------------------------------------------------------
// filesDeletionFailed
// -----------------------------------------------------------------------
void castor::stager::RemoteGCSvc::filesDeletionFailed
(std::vector<u_signed64*>& diskCopyIds)
  throw (castor::exception::Exception) {
  // Build the FilesDeletionFailedRequest
  castor::stager::FilesDeletionFailed req;
  int i = 0;
  for (std::vector<u_signed64*>::iterator it = diskCopyIds.begin();
       it != diskCopyIds.end();
       it++) {
    castor::stager::GCFile* file =
      new castor::stager::GCFile;
    file->setDiskCopyId(**it);
    file->setRequest(&req);
    // Here the owner ship of files[i] is transmitted to req !
    req.files().push_back(file);
    i++;
  }
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteGCClientTimeout());
  client.sendRequest(&req, &rh);
  // no need to cleanup files since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
}
