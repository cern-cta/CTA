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
 * @(#)$RCSfile: testRemStgSvc.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2005/01/07 13:39:43 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/Constants.hpp"
#include "castor/IClient.hpp"
#include "castor/client/IResponseHandler.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/Services.hpp"
#include <iostream>
#include <list>

class GetUpdateStartResponseHandler : public castor::client::IResponseHandler {
 public:
  GetUpdateStartResponseHandler
  (castor::stager::DiskCopy** result,
   std::list<castor::stager::DiskCopyForRecall*>& sources) :
    m_result(result), m_sources(sources){}
  
  virtual void handleResponse(castor::rh::Response& r)
    throw (castor::exception::Exception) {
    castor::rh::GetUpdateStartResponse *resp =
      dynamic_cast<castor::rh::GetUpdateStartResponse*>(&r);
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
  cf->setFileId(30720293);  
  cf->setNsHost("cnsuser");
//   castor::stager::DiskCopy *dc =
//     new castor::stager::DiskCopy();
//   dc->setPath("TestPath");
//   dc->setCastorFile(cf);
//   dc->setStatus(castor::stager::DISKCOPY_STAGED);
//   cf->addDiskCopies(dc);
  castor::stager::SubRequest *sr =
    new castor::stager::SubRequest();
  sr->setFileName("/castor/cern.ch/user/s/sponcec3/unixODBC-2.2.6.tar.gz");
  sr->setProtocol("rootd");
  sr->setStatus(castor::stager::SUBREQUEST_WAITSCHED);
  sr->setCastorFile(cf);
  castor::stager::FileSystem *fs =
    new castor::stager::FileSystem();
  fs->setMountPoint("TestMountPoint");
//   fs->addCopies(dc);
//   dc->setFileSystem(fs);
  castor::stager::DiskServer *ds =
    new castor::stager::DiskServer();
  ds->setName("TestDiskServer");
  ds->addFileSystems(fs);
  fs->setDiskserver(ds);
  
  // Fill the database with them
  castor::Services* svcs = castor::BaseObject::services();
  castor::BaseAddress ad;
  ad.setCnvSvcName("OraCnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);
  try {
    svcs->createRep(&ad, sr, false);
    svcs->fillRep(&ad, sr, castor::OBJ_CastorFile, false);
//     svcs->fillRep(&ad, cf, castor::OBJ_DiskCopy, false);
//     svcs->fillRep(&ad, dc, castor::OBJ_FileSystem, false);
    svcs->createRep(&ad, fs, false);
    svcs->fillRep(&ad, fs, castor::OBJ_DiskServer, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught while filling DB : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    // release everything
    svcs->rollback(&ad);
    if (0 != cf) delete cf;
//     if (0 != dc) delete dc;
    if (0 != sr) delete sr;
    if (0 != fs) delete fs;
    if (0 != ds) delete ds;
    return 1;
  }

  // Test RemoteStagerSvc

  // placeholders for the result
  castor::stager::DiskCopy* diskCopy = 0;
  castor::IClient *result = 0;
  std::list<castor::stager::DiskCopyForRecall*> sources;
  // Build a response Handler
  //  GetUpdateStartResponseHandler rh(&result, sources);
  // Build the GetUpdateStartRequest
//   castor::stager::GetUpdateStartRequest req;
//   req.setSubreqId(sr->id());
//   req.setDiskServer(ds->name());
//   req.setFileSystem(fs->mountPoint());
  // Uses a BaseClient to handle the request
//   castor::client::BaseClient client;
  try {
//     client.sendRequest(&req, &rh);
    castor::IService* svc =
      svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
    castor::stager::IStagerSvc* stgSvc =
      dynamic_cast<castor::stager::IStagerSvc*>(svc);
    result = stgSvc->getUpdateStart(sr, fs, &diskCopy, sources);
    if (0 != result) {
      std::cout << result << std::endl;
    } else {
      std::cout << "No IClient returned" << std::endl;
    }
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught while calling sendRequest : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  }
  exit(0);
  // Cleanup the database
  try {
    svcs->deleteRep(&ad, sr, false);
    svcs->deleteRep(&ad, cf, false);
//     svcs->deleteRep(&ad, dc, false);
    svcs->deleteRep(&ad, fs, false);
    svcs->deleteRep(&ad, ds, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught while cleanig DB : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  }
  
  // cleanup memory
  if (0 != cf) delete cf;
//   if (0 != dc) delete dc;
  if (0 != sr) delete sr;
  if (0 != fs) delete fs;
  if (0 != ds) delete ds;
  if (0 != result) delete result;
  if (0 != diskCopy) delete diskCopy;
  
}
