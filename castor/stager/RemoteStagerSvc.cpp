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
 * @(#)$RCSfile: RemoteStagerSvc.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2004/11/30 16:36:19 $ $Author: sponcec3 $
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
#include "castor/stager/ScheduleSubReqRequest.hpp"
#include "castor/stager/UpdateRepRequest.hpp"
#include "castor/rh/ScheduleSubReqResponse.hpp"
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
// ScheduleSubRequestResponseHandler
// -----------------------------------------------------------------------
/**
 * A dedicated little response handler for the ScheduleSubReq
 * requests
 */
class ScheduleSubReqResponseHandler : public castor::client::IResponseHandler {
public:
  ScheduleSubReqResponseHandler
  (castor::stager::DiskCopy** result,
   std::list<castor::stager::DiskCopyForRecall*>& sources) :
    m_result(result), m_sources(sources){}

  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    castor::rh::ScheduleSubReqResponse *resp =
      dynamic_cast<castor::rh::ScheduleSubReqResponse*>(&r);
    *m_result = resp->diskCopy();
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
  castor::stager::DiskCopy** m_result;
  // where to store the sources
  std::list<castor::stager::DiskCopyForRecall*>& m_sources;
};


// -----------------------------------------------------------------------
// scheduleSubRequest
// -----------------------------------------------------------------------
castor::stager::DiskCopy*
castor::stager::RemoteStagerSvc::scheduleSubRequest
(castor::stager::SubRequest* subreq,
 castor::stager::FileSystem* fileSystem,
 std::list<castor::stager::DiskCopyForRecall*>& sources)
  throw (castor::exception::Exception) {
  // placeholders for the result
  castor::stager::DiskCopy* result;
  // Build a response Handler
  ScheduleSubReqResponseHandler rh(&result, sources);
  // Build the ScheduleSubReqRequest
  castor::stager::ScheduleSubReqRequest req;
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

