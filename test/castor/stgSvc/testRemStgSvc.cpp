/******************************************************************************
 *                      testdb.cpp
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
 * @(#)$RCSfile: testRemStgSvc.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/25 14:18:50 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/Constants.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/ScheduleSubReqResponse.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/ScheduleSubReqRequest.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/Services.hpp"
#include <iostream>
#include <list>

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



int main (int argc, char** argv) {

  // initalizes log
  castor::BaseObject::initLog("LogSvc", castor::SVC_STDMSG);

  // Create needed objects
  castor::stager::CastorFile *cf =
    new castor::stager::CastorFile();
  cf->setFileId(777777);  
  cf->setNsHost("TestNsHost");
  castor::stager::DiskCopy *dc =
    new castor::stager::DiskCopy();
  dc->setPath("TestPath");
  dc->setCastorFile(cf);
  dc->setStatus(castor::stager::DISKCOPY_STAGED);
  cf->addDiskCopies(dc);
  castor::stager::SubRequest *sr =
    new castor::stager::SubRequest();
  sr->setFileName("TestSubRequest");
  sr->setProtocol("TestProtocol");
  sr->setStatus(castor::stager::SUBREQUEST_WAITSCHED);
  sr->setCastorFile(cf);
  castor::stager::FileSystem *fs =
    new castor::stager::FileSystem();
  fs->setMountPoint("TestMoutnPoint");
  fs->addCopies(dc);
  dc->setFileSystem(fs);
  castor::stager::DiskServer *ds =
    new castor::stager::DiskServer();
  ds->setName("testDiskServer");
  ds->addFileSystems(fs);
  fs->setDiskserver(ds);
  
  // Fill the database with them
  castor::Services* svcs = castor::BaseObject::services();
  castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
  try {
    svcs->createRep(&ad, sr, false);
    svcs->fillRep(&ad, sr, castor::OBJ_CastorFile, false);
    svcs->fillRep(&ad, cf, castor::OBJ_DiskCopy, false);
    svcs->fillRep(&ad, dc, castor::OBJ_FileSystem, false);
    svcs->fillRep(&ad, fs, castor::OBJ_DiskServer, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught while filling DB : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    // release everything
    svcs->rollback(&ad);
    delete cf;
    delete dc;
    delete sr;
    delete fs;
    delete ds;
    return 1;
  }

  // Test RemoteStagerSvc

  // placeholders for the result
  castor::stager::DiskCopy* result;
  std::list<castor::stager::DiskCopyForRecall*> sources;
  // Build a response Handler
  ScheduleSubReqResponseHandler rh(&result, sources);
  // Build the ScheduleSubReqRequest
  castor::stager::ScheduleSubReqRequest req;
  req.setSubreqId(sr->id());
  req.setDiskServer(ds->name());
  req.setFileSystem(fs->mountPoint());
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client;
  try {
    client.sendRequest(&req, &rh);
    result->print();
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught while calling sendRequest : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  }

  // Cleanup the database
  try {
    svcs->deleteRep(&ad, sr, false);
    svcs->deleteRep(&ad, cf, false);
    svcs->deleteRep(&ad, dc, false);
    svcs->deleteRep(&ad, fs, false);
    svcs->deleteRep(&ad, ds, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught while cleanig DB : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  }
  
  // cleanup memory
  delete cf;
  delete dc;
  delete sr;
  delete fs;
  delete ds;
  delete result;
  
}
