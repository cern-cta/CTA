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
 * @(#)$RCSfile: RemoteStagerSvc.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2004/12/02 17:56:05 $ $Author: sponcec3 $
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
#include "castor/stager/RemoteStagerSvc.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/UpdateRepRequest.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/rh/ClientResponse.hpp"
#include "castor/exception/NotSupported.hpp"
#include <list>

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
(castor::stager::SubRequest* subreq)
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
  (castor::IClient** result,
   castor::stager::DiskCopy** diskCopy,
   std::list<castor::stager::DiskCopyForRecall*>& sources) :
    m_result(result), m_diskCopy(diskCopy),
    m_sources(sources){}

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    castor::rh::GetUpdateStartResponse *resp =
      dynamic_cast<castor::rh::GetUpdateStartResponse*>(&r);
    *m_result = resp->client();
    *m_diskCopy = resp->diskCopy();
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
  // where to store the result
  castor::IClient** m_result;
  // where to store the diskCopy
  castor::stager::DiskCopy** m_diskCopy;
  // where to store the sources
  std::list<castor::stager::DiskCopyForRecall*>& m_sources;
};


// -----------------------------------------------------------------------
// getUpdateStart
// -----------------------------------------------------------------------
castor::IClient*
castor::stager::RemoteStagerSvc::getUpdateStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 castor::stager::DiskCopy** diskCopy,
 std::list<castor::stager::DiskCopyForRecall*>& sources)
  throw (castor::exception::Exception) {
  // placeholders for the result
  castor::IClient* result;
  // Build a response Handler
  GetUpdateStartResponseHandler rh(&result, diskCopy, sources);
  // Build the GetUpdateStartRequest
  castor::stager::GetUpdateStartRequest req;
  req.setSubreqId(subreq->id());
  req.setDiskServer(fileSystem->diskserver()->name());
  req.setFileSystem(fileSystem->mountPoint());
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client;
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
  PutStartResponseHandler (castor::IClient** result) :
    m_result(result) {}

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    castor::rh::ClientResponse *resp =
      dynamic_cast<castor::rh::ClientResponse*>(&r);
    *m_result = resp->client();
  };
  virtual void terminate()
    throw (castor::exception::Exception) {};
private:
  // where to store the result
  castor::IClient** m_result;
};

// -----------------------------------------------------------------------
// putStart
// -----------------------------------------------------------------------
castor::IClient*
castor::stager::RemoteStagerSvc::putStart
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem)
  throw (castor::exception::Exception) {
  // placeholders for the result
  castor::IClient* result;
  // Build a response Handler
  PutStartResponseHandler rh(&result);
  // Build the PutStartRequest
  castor::stager::PutStartRequest req;
  req.setSubreqId(subreq->id());
  req.setDiskServer(fileSystem->diskserver()->name());
  req.setFileSystem(fileSystem->mountPoint());
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client;
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
(const u_signed64 fileId,
 const std::string nsHost)
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
// updateRep
// -----------------------------------------------------------------------
void castor::stager::RemoteStagerSvc::updateRep(IAddress* address,
                                                IObject* object)
  throw (castor::exception::Exception) {
  // Build the UpdateRepRequest
  castor::stager::UpdateRepRequest req;
  req.setObject(object);
  req.setAddress(address);
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client;
  client.sendRequest(&req, &rh);
}

// -----------------------------------------------------------------------
// recreateCastorFile
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::stager::RemoteStagerSvc::recreateCastorFile
(castor::stager::CastorFile *castorFile)
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
(castor::stager::SubRequest *subreq)
  throw (castor::exception::Exception) {
  // Build the MoverCloseRequest
  castor::stager::MoverCloseRequest req;
  req.setSubReqId(subreq->id());
  // Build a response Handler
  castor::client::BasicResponseHandler rh;
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client;
  client.sendRequest(&req, &rh);
}
