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
 * @(#)$RCSfile: teststgsvc.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/12/01 09:14:57 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/stager/Tape.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"
#include <iostream>

void processTapes(castor::stager::IStagerSvc* stgSvc)
  throw (castor::exception::Exception);

int main (int argc, char** argv) {
  // initalizes log
  castor::BaseObject::initLog("LogSvc", castor::SVC_STDMSG);

  // Prepare 2 tapes and some segments with different status
  castor::stager::Tape* t1 = new castor::stager::Tape();
  t1->setVid("TestTape 1");
  t1->setStatus(castor::stager::TAPE_PENDING);
  
  castor::stager::Segment* s11 = new castor::stager::Segment();
  s11->setErrMsgTxt("Seg 11");
  s11->setStatus(castor::stager::SEGMENT_UNPROCESSED);
  s11->setTape(t1);
  t1->addSegments(s11);
  
  castor::stager::Segment* s12 = new castor::stager::Segment();
  s12->setErrMsgTxt("Seg 12");
  s12->setStatus(castor::stager::SEGMENT_FILECOPIED);
  s12->setTape(t1);
  t1->addSegments(s12);

  castor::stager::Segment* s13 = new castor::stager::Segment();
  s13->setErrMsgTxt("Seg 13");
  s13->setStatus(castor::stager::SEGMENT_FAILED);
  s13->setTape(t1);
  t1->addSegments(s13);

  castor::stager::Segment* s14 = new castor::stager::Segment();
  s14->setErrMsgTxt("Seg 14");
  s14->setStatus(castor::stager::SEGMENT_FAILED);
  s14->setTape(t1);
  t1->addSegments(s14);

  castor::stager::Tape* t2 = new castor::stager::Tape();
  t2->setVid("TestTape 2");
  t2->setStatus(castor::stager::TAPE_PENDING);
  
  // Get a Services instance
  castor::Services* svcs = castor::BaseObject::services();

  // Stores the tapes
  castor::BaseAddress ad;
  ad.setCnvSvcName("OraCnvSvc");
  ad.setCnvSvcType(castor::SVC_ORACNV);
  try {
    svcs->createRep(&ad, t1, false);
    svcs->fillRep(&ad, t1, castor::OBJ_Segment, false);
    svcs->createRep(&ad, t2, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    // release the memory
    delete svcs;
    delete t1;
    delete t2;
    return 1;
  }

  // Display initial objects
  std::cout << "Originally :" << std::endl;
  castor::ObjectSet alreadyPrinted;
  t1->print(std::cout, "  ", alreadyPrinted);
  t2->print(std::cout, "  ", alreadyPrinted);

  // Get the StagerSvc
  castor::IService* svc =
    svcs->service("OraStagerSvc", castor::SVC_ORASTAGERSVC);
  castor::stager::IStagerSvc* stgSvc =
    dynamic_cast<castor::stager::IStagerSvc*>(svc);
  
  // process tapes
  try {
    processTapes(stgSvc);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    stgSvc->release();
    delete svcs;
    return 1;
  }

  // Try it again, to see whether we find something or not
  try {
    processTapes(stgSvc);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in createRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
    stgSvc->release();
    delete svcs;
    return 1;
  }

  // Cleanup database and delete objects
  try {
    svcs->deleteRep(&ad, t1, false);
    svcs->deleteRep(&ad, t2, true);
  } catch (castor::exception::Exception e) {
    std::cout << "Error caught in deleteRep : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  }
  delete t1;
  t1 = 0;
  delete t2;
  t2 = 0;

  return 0;
}

void processTapes(castor::stager::IStagerSvc* stgSvc)
  throw (castor::exception::Exception) {
  // Now try to use tapesToDo
  std::vector<castor::stager::Tape*> tapes =
    stgSvc->tapesToDo();
  std::cout << "Here are the tapes to handle :" << std::endl;
  for (std::vector<castor::stager::Tape*>::iterator it = tapes.begin();
       it != tapes.end();
       it++) {
    std::cout << "  " << (*it)->vid();
  }
  std::cout << std::endl;

  // Loop over the tapes and deal with segments
  for (std::vector<castor::stager::Tape*>::iterator it = tapes.begin();
       it != tapes.end();
       it++) {
    bool isWork = stgSvc->anySegmentsForTape(*it);
    if (isWork) {
      std::cout << "Working on segments of tape "
                << (*it)->vid() << std::endl;
      // get the list of segments to do
      std::vector<castor::stager::Segment*> segs =
        stgSvc->segmentsForTape(*it);
      // Showing them
      std::cout << "Here is the list :" << std::endl;
      for (std::vector<castor::stager::Segment*>::iterator it2 = segs.begin();
           it2 != segs.end();
           it2++) {
        std::cout << "  " << (*it2)->errMsgTxt();
      }
      std::cout << std::endl;
    } else {
      // Not dealing with these tapes
      std::cout << "Nothing to do for tape "
                << (*it)->vid() << std::endl;
    }
  }
}
