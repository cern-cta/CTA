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
 * @(#)$RCSfile: testdb.cpp,v $ $Revision: 1.13 $ $Release$ $Date: 2005/01/07 13:52:38 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/CastorFile.hpp"
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
  // initalizes log
  castor::BaseObject::initLog("MsgSvc", castor::SVC_STDMSG);

  // Build a castorFile
  castor::stager::CastorFile* cf = new castor::stager::CastorFile();
  cf->setNsHost("TestNameServer");

  // Build a diskCopy
  castor::stager::DiskCopy* dc1 = new castor::stager::DiskCopy();
  dc1->setPath("/my/disk/copy1.dat");
  dc1->setCastorFile(cf);
  cf->addDiskCopies(dc1);
  
  // Build a second diskCopy
  castor::stager::DiskCopy* dc2 = new castor::stager::DiskCopy();
  dc2->setPath("/my/disk/copy2.dat");
  dc2->setCastorFile(cf);
  cf->addDiskCopies(dc2);
  
  // Get a Services instance
  castor::Services* svcs = castor::BaseObject::services();

  // Stores the castorFile and DiskCopies
  castor::BaseAddress ad;
  ad.setCnvSvcName("OraCnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);
  try {
    svcs->createRep(&ad, cf, false);
    svcs->fillRep(&ad, cf, castor::OBJ_DiskCopy, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    // release the memory
    delete svcs;
    delete cf;
    return 1;
  }

  // Retrieves everything in a separate objects
  castor::BaseAddress ad2;
  ad2.setId(cf->id());
  ad2.setCnvSvcName("OraCnvSvc");
  ad2.setCnvSvcType(castor::SVC_ORACNV);
  castor::stager::CastorFile* cf2;
  try{
    castor::IObject* cf2Obj = svcs->createObj(&ad2);
    svcs->fillObj(&ad2, cf2Obj, castor::OBJ_DiskCopy);
    cf2 = dynamic_cast<castor::stager::CastorFile*>(cf2Obj);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createObj : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    delete svcs;
    delete cf;
    return 1;
  }
  
  // Display both objects for comparison
  std::cout << "Originally :" << std::endl;
  castor::ObjectSet alreadyPrinted;
  cf->print(std::cout, "  ", alreadyPrinted);
  castor::ObjectSet alreadyPrinted2;
  std::cout << "Finally :" << std::endl;
  cf2->print(std::cout, "  ", alreadyPrinted2);
  
  // Now modify the first object
  cf->removeDiskCopies(dc2);
  castor::stager::DiskCopy* dc3 = new castor::stager::DiskCopy();
  dc3->setPath("/my/disk/copy3.dat");
  cf->addDiskCopies(dc3);
  dc3->setCastorFile(cf);

  // update the database
  try {
    svcs->updateRep(&ad, cf, false);
    svcs->fillRep(&ad, cf, castor::OBJ_DiskCopy, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in updateRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    delete svcs;
    delete cf;
    delete cf2;
    return 1;
  }

  // And update the second representation of the object
  try {
    svcs->updateObj(&ad, cf2);
    svcs->fillObj(&ad, cf2, castor::OBJ_DiskCopy);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in updateObj : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    delete svcs;
    delete cf;
    delete cf2;
    return 1;
  }
  
  // Finally display the two modified objects to check
  std::cout << "Originally modified :" << std::endl;
  castor::ObjectSet alreadyPrinted3;
  cf->print(std::cout, "  ", alreadyPrinted3);
  castor::ObjectSet alreadyPrinted4;
  std::cout << "Finally modified :" << std::endl;
  cf2->print(std::cout, "  ", alreadyPrinted4);

  delete svcs;
  delete cf;
  delete cf2;
  return 0;
}

