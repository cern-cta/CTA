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
 * @(#)$RCSfile: testdb.cpp,v $ $Revision: 1.12 $ $Release$ $Date: 2005/01/07 13:39:43 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/stager/StagePutRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/rh/Client.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"
#include <iostream>

int main (int argc, char** argv) {

  if (2 != argc) {
    std::cout << "usage : " << argv[0] << " file" << std::endl;
    exit(0);
  }   

  // initalizes log
  castor::BaseObject::initLog("MsgSvc", castor::SVC_STDMSG);

  // Build a StagePutRequest
  castor::stager::StagePutRequest* req = new castor::stager::StagePutRequest();
  req->setUserTag(argv[1]);

  // Build client
  castor::rh::Client* cl = new castor::rh::Client();
  cl->setPort(9999);
  cl->setIpAddress(1);
  req->setClient(cl);

  // Build a SubRequest
  castor::stager::SubRequest* sr = new castor::stager::SubRequest();
  sr->setFileName("/my/disk/copy2.dat");
  sr->setModeBits(448);
  req->addSubRequests(sr);
  sr->setRequest(req);
  
  // Get a Services instance
  castor::Services* svcs = castor::BaseObject::services();

  // Build an address
  castor::BaseAddress ad;
  ad.setCnvSvcName("OraCnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);

  try {
    svcs->createRep(&ad, req, false);
    svcs->fillRep(&ad, req, castor::OBJ_SubRequest, false);
    svcs->createRep(&ad, req->client(), false);
    svcs->fillRep(&ad, req, castor::OBJ_IClient, false);
    svcs->commit(&ad);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    // release the memory
    delete svcs;
    delete sr;
    delete req;
    return 1;
  }

  delete svcs;
  delete sr;
  delete req;
  return 0;
}

