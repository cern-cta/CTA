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
 * @(#)$RCSfile: testdb.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2004/10/18 12:55:26 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/stager/StageInRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/rh/Client.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"
#include <iostream>

int main (int argc, char** argv) {
  // initalizes log
  castor::BaseObject::initLog("", castor::SVC_STDMSG);

  // Prepare a request
  castor::stager::StageInRequest* fr = new castor::stager::StageInRequest();
  
  castor::rh::Client *cl = new castor::rh::Client();
  cl->setIpAddress(0606);
  cl->setPort(0707);
  cl->setRequest(fr);
  fr->setClient(cl);

  castor::stager::SubRequest* s1 = new castor::stager::SubRequest();
  s1->setFileName("First test SubRequest");
  fr->addSubRequests(s1);
  s1->setRequest(fr);

  castor::stager::SubRequest* s2 = new castor::stager::SubRequest();
  s2->setFileName("2nd test SubRequest");
  fr->addSubRequests(s2);
  s2->setRequest(fr);

  // Get a Services instance
  castor::Services* svcs = new castor::Services();

  // Stores the request
  castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
  try {
    svcs->createRep(&ad, fr, false);
    svcs->fillRep(&ad, fr, castor::OBJ_SubRequest, false);
    svcs->fillRep(&ad, fr, castor::OBJ_IClient, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    // release the memory
    delete svcs;
    delete fr;
    return 1;
  }

  // Retrieves it in a separate object
  castor::db::DbAddress ad2(fr->id(), "OraCnvSvc", castor::SVC_ORACNV);
  castor::stager::Request* fr2;
  try{
    castor::IObject* fr2Obj = svcs->createObj(&ad2);
    svcs->fillObj(&ad2, fr2Obj, castor::OBJ_SubRequest);
    svcs->fillObj(&ad2, fr2Obj, castor::OBJ_IClient);
    fr2 = dynamic_cast<castor::stager::Request*>(fr2Obj);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createObj : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    delete svcs;
    delete fr;
    return 1;
  }
  
  // Display both objects for comparison
  std::cout << "Originally :" << std::endl;
  castor::ObjectSet alreadyPrinted;
  fr->print(std::cout, "  ", alreadyPrinted);
  castor::ObjectSet alreadyPrinted2;
  std::cout << "Finally :" << std::endl;
  fr2->print(std::cout, "  ", alreadyPrinted2);
  
  // Now modify the first object
  fr->removeSubRequests(s2);
  castor::stager::SubRequest* s3 = new castor::stager::SubRequest();
  s3->setFileName("3rd test SubRequest");
  fr->addSubRequests(s3);
  s3->setRequest(fr);

  // update the database
  try {
    svcs->updateRep(&ad, fr, false);
    svcs->fillRep(&ad, fr, castor::OBJ_SubRequest, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in updateRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    delete svcs;
    delete fr;
    delete fr2;
    return 1;
  }

  // And update the second representation of the object
  try {
    svcs->updateObj(&ad, fr2);
    svcs->fillObj(&ad, fr2, castor::OBJ_SubRequest);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in updateObj : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    delete svcs;
    delete fr;
    delete fr2;
    return 1;
  }
  
  // Finally display the two modified objects to check
  std::cout << "Originally modified :" << std::endl;
  castor::ObjectSet alreadyPrinted3;
  fr->print(std::cout, "  ", alreadyPrinted3);
  castor::ObjectSet alreadyPrinted4;
  std::cout << "Finally modified :" << std::endl;
  fr2->print(std::cout, "  ", alreadyPrinted4);

  delete svcs;
  delete fr;
  delete fr2;
  return 0;
}

