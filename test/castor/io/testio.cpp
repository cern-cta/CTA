/******************************************************************************
 *                      testio.cpp
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
 * @(#)$RCSfile: testio.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/05/19 16:47:48 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/rh/StageInRequest.hpp"
#include "castor/rh/File.hpp"
#include "castor/rh/Client.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/io/StreamAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include <castor/io/biniostream.h>
#include "castor/exception/Exception.hpp"
#include <iostream>

int main (int argc, char** argv) {
  // Prepare a request
  castor::rh::StageInRequest* fr = new castor::rh::StageInRequest();
  fr->setDescription("This is a test FileRequest");
  
  castor::rh::Client *cl = new castor::rh::Client();
  cl->setIpAddress(0606);
  cl->setPort(0707);
  cl->setRequest(fr);
  fr->setClient(cl);

  castor::rh::File* f1 = new castor::rh::File();
  f1->setName("First test File");
  fr->addFiles(f1);
  f1->setRequest(fr);

  castor::rh::File* f2 = new castor::rh::File();
  f2->setName("2nd test File");
  fr->addFiles(f2);
  f2->setRequest(fr);

  // Get a Services instance
  castor::Services* svcs = new castor::Services();

  // Stores the request
  castor::io::biniostream buffer;
  castor::io::StreamAddress ad(buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
  try {
    svcs->createRep(&ad, fr, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    // release the memory
    delete svcs;
    delete fr;
    return 1;
  }

  // Displays the content of the string
  std::cout << "\"" << buffer.str() << "\"" << std::endl;

  // Retrieves it in a separate object
  castor::io::StreamAddress ad2 (buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
  castor::rh::FileRequest* fr2;
  try{
    castor::IObject* fr2Obj = svcs->createObj(&ad2);
    fr2 = dynamic_cast<castor::rh::FileRequest*>(fr2Obj);
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
  fr->print(std::cout, "", alreadyPrinted);
  castor::ObjectSet alreadyPrinted2;
  std::cout << "Finally :" << std::endl;
  fr2->print(std::cout, "", alreadyPrinted2);

  delete svcs;
  delete fr;
  delete fr2;
  return 0;
}

