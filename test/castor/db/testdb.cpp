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
 * @(#)$RCSfile: testdb.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/05/13 12:58:29 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/rh/StageInRequest.hpp"
#include "castor/rh/File.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/Copy.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/ObjectSet.hpp"
#include <iostream>

int main (int argc, char** argv) {
  // Prepare a request
  castor::rh::StageInRequest* fr = new castor::rh::StageInRequest();
  fr->setDescription("This is a test FileRequest");
  fr->setSuspended(true);
  
  castor::rh::Client *cl = new castor::rh::Client();
  cl->setIpAddress(0606);
  cl->setPort(0707);
  cl->setRequest(fr);
  fr->setClient(cl);

  castor::rh::File* f1 = new castor::rh::File();
  f1->setName("First test File");
  fr->addFiles(f1);
  f1->setRequest(fr);

  castor::rh::Copy* c1 = new castor::rh::Copy();
  c1->setNum(45);
  c1->setParentFile(f1);
  f1->addCopy(c1);

  castor::rh::File* f2 = new castor::rh::File();
  f2->setName("2nd test File");
  fr->addFiles(f2);
  f2->setRequest(fr);

  castor::rh::Copy* c2 = new castor::rh::Copy();
  c2->setNum(46);
  c2->setParentFile(f2);
  f2->addCopy(c2);

  castor::rh::Copy* c3 = new castor::rh::Copy();
  c3->setNum(47);
  c3->setParentFile(f2);
  f2->addCopy(c3);

  castor::rh::Copy* c4 = new castor::rh::Copy();
  c4->setNum(48);
  c4->setParentFile(f2);
  f2->addCopy(c4);

  // Get a Services instance
  castor::Services* svcs = new castor::Services();

  // Stores the request
  castor::BaseAddress ad("OraCnvSvc", castor::SVC_ORACNV);
  try {
    svcs->createRep(&ad, fr, true);
  } catch (castor::Exception e) {
    std::cout << "Error caught in createRep :" << std::endl
              << e.getMessage().str() << std::endl;
    // release the memory
    delete svcs;
    delete fr;
    return 1;
  }

  // Retrieves it in a separate object
  castor::db::DbAddress ad2(fr->id(), "OraCnvSvc", castor::SVC_ORACNV);
  castor::rh::FileRequest* fr2;
  try{
    castor::IObject* fr2Obj = svcs->createObj(&ad2);
    fr2 = dynamic_cast<castor::rh::FileRequest*>(fr2Obj);
  } catch (castor::Exception e) {
    std::cout << "Error caught in createObj :" << std::endl
              << e.getMessage().str() << std::endl;
    delete svcs;
    delete fr;
    return 1;
  }
  
  // Display both objects for comparison
  std::cout << "Originally :" << std::endl;
  castor::ObjectSet alreadyPrinted;
  fr->print(std::cout, "", alreadyPrinted);
  castor::ObjectSet alreadyPrinted2;
  std::cout << "Finally :" << std::endl;
  fr2->print(std::cout, "", alreadyPrinted2);

  delete svcs;
  delete fr;
  delete fr2;
  return 0;
}

