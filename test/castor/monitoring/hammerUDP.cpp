/******************************************************************************
 *                      hammerUDP.cpp
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
 * @(#)$RCSfile: hammerUDP.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/10/02 08:34:10 $ $Author: sponcec3 $
 *
 * test program hammering the monitoring with UDP messages of
 * psuedo migrators and/or recallers in order to test
 * rmMasterDaemon stability
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <castor/stager/ITapeSvc.hpp>
#include <castor/Services.hpp>
#include <castor/BaseObject.hpp>
#include <iostream>
#include <stdlib.h>

int main(int argc, char** argv) {

  // get the tape service
  castor::stager::ITapeSvc* tapeSvc = 0;
  castor::IService* svc =
    castor::BaseObject::services()->service
    ("DbTapeSvc", castor::SVC_DBTAPESVC);
  tapeSvc = dynamic_cast<castor::stager::ITapeSvc*>(svc);
  if (0 == tapeSvc) {
    std::cerr << "Couldn't load the tape service, check the castor.conf for DynamicLib entries" << std::endl;
    exit(-1);
  }

  // get host name
  char* hostname = (char*) calloc(200, 1);
  if (gethostname(hostname, 200) < 0) {
    std::cerr << "Couldn't get hostname" << std::endl;
    exit(-1);
  }

  // endless loop flooding the server with UDP messages
  while(1) {
    tapeSvc->sendStreamReport(hostname,
                              "/srv/castor/01/",
                              castor::monitoring::STREAMDIRECTION_READ,
                              true);
  }

}
