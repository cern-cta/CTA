/******************************************************************************
 *                      RemoteStagerSvc.cpp
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
 * @(#)$RCSfile: RemoteStagerSvc.cpp,v $ $Revision: 1.46 $ $Release$ $Date: 2005/07/01 13:10:11 $ $Author: sponcec3 $
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
#include "castor/stager/RemoteStagerSvc.hpp"
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
#include <list>

EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));

//------------------------------------------------------------------------------
// Constants
//------------------------------------------------------------------------------
const char* castor::stager::RMTSTGSVC_CATEGORY_CONF = "REMOTESTGSVC";
const char* castor::stager::TIMEOUT_CONF = "TIMEOUT";
const int   castor::stager::DEFAULT_REMOTESTGSVC_TIMEOUT = 1800;


// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::stager::RemoteStagerSvc>
s_factoryRemoteStagerSvc;
const castor::IFactory<castor::IService>&
RemoteStagerSvcFactory = s_factoryRemoteStagerSvc;

// -----------------------------------------------------------------------
// RemoteStagerSvc
// -----------------------------------------------------------------------
castor::stager::RemoteStagerSvc::RemoteStagerSvc(const std::string name) :
  BaseSvc(name) {}

// -----------------------------------------------------------------------
// ~RemoteStagerSvc
// -----------------------------------------------------------------------
castor::stager::RemoteStagerSvc::~RemoteStagerSvc() throw() {}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::stager::RemoteStagerSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::stager::RemoteStagerSvc::ID() {
  return castor::SVC_REMOTESTAGERSVC;
}

// -----------------------------------------------------------------------
// anySegmentsForTape
// -----------------------------------------------------------------------
int castor::stager::RemoteStagerSvc::anySegmentsForTape
(castor::stager::Tape* searchItem)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// segmentsForTape
// -----------------------------------------------------------------------
std::vector<castor::stager::Segment*>
castor::stager::RemoteStagerSvc::segmentsForTape
(castor::stager::Tape* searchItem)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// bestFileSystemForSegment
// -----------------------------------------------------------------------
castor::stager::DiskCopyForRecall*
castor::stager::RemoteStagerSvc::bestFileSystemForSegment
(castor::stager::Segment *segment)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// anyTapeCopyForStream
// -----------------------------------------------------------------------
bool castor::stager::RemoteStagerSvc::anyTapeCopyForStream
(castor::stager::Stream* searchItem)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// bestTapeCopyForStream
// -----------------------------------------------------------------------
castor::stager::TapeCopyForMigration*
castor::stager::RemoteStagerSvc::bestTapeCopyForStream
(castor::stager::Stream* searchItem)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// bestTapeCopyForStream
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::streamsForTapePool
(castor::stager::TapePool* tapePool)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
};

// -----------------------------------------------------------------------
// fileRecalled
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::fileRecalled
(castor::stager::TapeCopy* tapeCopy)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// fileRecallFailed
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::fileRecallFailed
(castor::stager::TapeCopy* tapeCopy)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// tapesToDo
// -----------------------------------------------------------------------
std::vector<castor::stager::Tape*>
castor::stager::RemoteStagerSvc::tapesToDo()
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}


// -----------------------------------------------------------------------
// streamsToDo
// -----------------------------------------------------------------------
std::vector<castor::stager::Stream*>
castor::stager::RemoteStagerSvc::streamsToDo()
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectTape
// -----------------------------------------------------------------------
castor::stager::Tape*
castor::stager::RemoteStagerSvc::selectTape
(const std::string vid,
 const int side,
 const int tpmode)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// subRequestToDo
// -----------------------------------------------------------------------
castor::stager::SubRequest*
castor::stager::RemoteStagerSvc::subRequestToDo
(std::vector<ObjectsIds> &types)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// subRequestFailedToDo
// -----------------------------------------------------------------------
castor::stager::SubRequest*
castor::stager::RemoteStagerSvc::subRequestFailedToDo()
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// requestToDo
// -----------------------------------------------------------------------
castor::stager::Request*
castor::stager::RemoteStagerSvc::requestToDo
(std::vector<ObjectsIds> &types)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// isSubRequestToSchedule
// -----------------------------------------------------------------------
bool castor::stager::RemoteStagerSvc::isSubRequestToSchedule
(castor::stager::SubRequest* subreq,
 std::list<castor::stager::DiskCopyForRecall*>& sources)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// GetUpdateStartResponseHandler
// -----------------------------------------------------------------------
/**
 * A dedicated little response handler for the GetUpdateStart
 * requests
 */
class GetUpdateStartResponseHandler : public castor::client::IResponseHandler {
public:
  GetUpdateStartResponseHandler
  (castor::stager::DiskCopy** diskCopy,
   std::list<castor::stager::DiskCopyForRecall*>& sources,
   bool *emptyFile) :
    m_diskCopy(diskCopy),
    m_sources(sources),
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
    for (std::vector<castor::stager::DiskCopyForRecall*>::iterator it =
           resp->sources().begin();
         it != resp->sources().end();
         it++) {
      m_sources.push_back(*it);
    }
  };
  virtual void terminate()
    throw (castor::exception::Exception) {};
private:
  // where to store the diskCopy
  castor::stager::DiskCopy** m_diskCopy;
  // where to store the sources
  std::list<castor::stager::DiskCopyForRecall*>& m_sources;
  // Where to store the emptyFile flag
  bool* m_emptyFile;
};


// -----------------------------------------------------------------------
// getUpdateStart
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::stager::RemoteStagerSvc::getUpdateStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 std::list<castor::stager::DiskCopyForRecall*>& sources,
 bool* emptyFile)
  throw (castor::exception::Exception) {
  // placeholders for the result
  castor::stager::DiskCopy* result;
  // Build a response Handler
  GetUpdateStartResponseHandler rh
    (&result, sources, emptyFile);
  // Build the GetUpdateStartRequest
  castor::stager::GetUpdateStartRequest req;
  req.setSubreqId(subreq->id());
  req.setDiskServer(fileSystem->diskserver()->name());
  req.setFileSystem(fileSystem->mountPoint());
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
  // return
  return result;
}

// -----------------------------------------------------------------------
// putStartResponseHandler
// -----------------------------------------------------------------------
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
  // where to store the diskCopy
  castor::stager::DiskCopy** m_diskCopy;
};

// -----------------------------------------------------------------------
// putStart
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::stager::RemoteStagerSvc::putStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem)
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
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
  // return
  return result;
}

// -----------------------------------------------------------------------
// putDoneStart
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::stager::RemoteStagerSvc::putDoneStart(u_signed64 subreqId)
  throw (castor::exception::Exception) {
  // placeholders for the result
  castor::stager::DiskCopy* result;
  // Build a response Handler
  PutStartResponseHandler rh(&result);
  // Build the PutStartDoneRequest
  castor::stager::PutDoneStart req;
  req.setSubreqId(subreqId);
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
  // return
  return result;
}

// -----------------------------------------------------------------------
// selectSvcClass
// -----------------------------------------------------------------------
castor::stager::SvcClass*
castor::stager::RemoteStagerSvc::selectSvcClass
(const std::string name)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectFileClass
// -----------------------------------------------------------------------
castor::stager::FileClass*
castor::stager::RemoteStagerSvc::selectFileClass
(const std::string name)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectCastorFile
// -----------------------------------------------------------------------
castor::stager::CastorFile*
castor::stager::RemoteStagerSvc::selectCastorFile
(const u_signed64 fileId, const std::string nsHost,
 u_signed64 svcClass, u_signed64 fileClass,
 u_signed64 fileSize)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectFileSystem
// -----------------------------------------------------------------------
castor::stager::FileSystem*
castor::stager::RemoteStagerSvc::selectFileSystem
(const std::string mountPoint,
 const std::string diskServer)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectDiskPool
// -----------------------------------------------------------------------
castor::stager::DiskPool*
castor::stager::RemoteStagerSvc::selectDiskPool
(const std::string name)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectTapePool
// -----------------------------------------------------------------------
castor::stager::TapePool*
castor::stager::RemoteStagerSvc::selectTapePool
(const std::string name)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectDiskServer
// -----------------------------------------------------------------------
castor::stager::DiskServer*
castor::stager::RemoteStagerSvc::selectDiskServer
(const std::string name)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// selectTapeCopiesForMigration
// -----------------------------------------------------------------------
std::vector<castor::stager::TapeCopy*>
castor::stager::RemoteStagerSvc::selectTapeCopiesForMigration
(castor::stager::SvcClass * svcClass)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// updateAndCheckSubRequest
// -----------------------------------------------------------------------
bool castor::stager::RemoteStagerSvc::updateAndCheckSubRequest
(castor::stager::SubRequest* subreq)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// disk2DiskCopyDone
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::disk2DiskCopyDone
(u_signed64 diskCopyId,
 castor::stager::DiskCopyStatusCodes status)
  throw (castor::exception::Exception) {
  // Build the Disk2DiskCopyDoneRequest
  castor::stager::Disk2DiskCopyDoneRequest req;
  req.setDiskCopyId(diskCopyId);
  req.setStatus(status);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
}

// -----------------------------------------------------------------------
// recreateCastorFile
// -----------------------------------------------------------------------
castor::stager::DiskCopyForRecall*
castor::stager::RemoteStagerSvc::recreateCastorFile
(castor::stager::CastorFile *castorFile,
 castor::stager::SubRequest *subreq)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// prepareForMigration
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::prepareForMigration
(castor::stager::SubRequest *subreq,
 u_signed64 fileSize)
  throw (castor::exception::Exception) {
  // Build the MoverCloseRequest
  castor::stager::MoverCloseRequest req;
  req.setSubReqId(subreq->id());
  req.setFileSize(fileSize);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
}

// -----------------------------------------------------------------------
// resetStream
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::resetStream
(castor::stager::Stream* stream)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// bestFileSystemForJob
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::bestFileSystemForJob
(char** fileSystems,
 char** machines,
 u_signed64* minFree,
 unsigned int fileSystemsNb,
 std::string* mountPoint,
 std::string* diskServer)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// updateFileSystemForJob
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::updateFileSystemForJob
(std::string fileSystem,
 std::string diskServer,
 u_signed64 fileSize)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// getRemoteStagerClientTimeout
// -----------------------------------------------------------------------
int castor::stager::RemoteStagerSvc::getRemoteStagerClientTimeout() {

  int ret_timeout = castor::stager::DEFAULT_REMOTESTGSVC_TIMEOUT;

  char *strtimeout = getconfent((char *)castor::stager::RMTSTGSVC_CATEGORY_CONF,
				(char *)castor::stager::TIMEOUT_CONF, 
				0);
  if (strtimeout != 0) {
    char* dp = strtimeout;
    int itimeout = strtoul(strtimeout, &dp, 0);
    if (*dp != 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Bad RemoteStgSvc timeout value:" << strtimeout << std::endl;
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
castor::stager::RemoteStagerSvc::selectFiles2Delete
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
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
  return result;
}

// -----------------------------------------------------------------------
// filesDeleted
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::filesDeleted
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
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
  // no need to cleanup files since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
}

// -----------------------------------------------------------------------
// filesDeletionFailed
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::filesDeletionFailed
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
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
  // no need to cleanup files since the ownership of its content
  // was transmitted to req and the deletion of req will delete it !
}

// -----------------------------------------------------------------------
// getUpdateDone
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::getUpdateDone
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  // Build the GetUpdateDoneRequest
  castor::stager::GetUpdateDone req;
  req.setSubReqId(subReqId);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
}

// -----------------------------------------------------------------------
// getUpdateFailed
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::getUpdateFailed
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  // Build the GetUpdateFailedRequest
  castor::stager::GetUpdateFailed req;
  req.setSubReqId(subReqId);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
}

// -----------------------------------------------------------------------
// putFailed
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::putFailed
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  // Build the PutFailedRequest
  castor::stager::PutFailed req;
  req.setSubReqId(subReqId);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(getRemoteStagerClientTimeout());
  client.sendRequest(&req, &rh);
}

// -----------------------------------------------------------------------
// archiveSubReq
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::archiveSubReq
(u_signed64 subReqId)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// failedSegments
// -----------------------------------------------------------------------
std::vector<castor::stager::Segment*>
castor::stager::RemoteStagerSvc::failedSegments()
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// stageRelease
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::stageRelease
(const u_signed64 fileId, const std::string nsHost)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}

// -----------------------------------------------------------------------
// stageRm
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::stageRm
(const u_signed64 fileId, const std::string nsHost)
  throw (castor::exception::Exception) {
  castor::exception::NotSupported ex;
  ex.getMessage()
    << "RemoteStagerSvc implementation is not complete"
    << std::endl << "This method is not supported.";
  throw ex;
}
