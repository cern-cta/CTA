/******************************************************************************
 *                      Server.cpp
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
 * @(#)$RCSfile: Server.cpp,v $ $Revision: 1.53 $ $Release$ $Date: 2007/08/23 06:45:11 $ $Author: waldron $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/rh/Server.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/rh/RHThread.hpp"
#include "castor/rh/IRHSvc.hpp"

#include "castor/System.hpp"
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"
#include "common.h"   // for getconfent

#include <iostream>

//------------------------------------------------------------------------------
// String constants
//------------------------------------------------------------------------------
const char *castor::rh::PORT_ENV = "RH_PORT";
const char *castor::rh::CATEGORY_CONF = "RH";
const char *castor::rh::PORT_CONF = "PORT";
const char *castor::rh::ACCESSLISTS_ENV = "RH_USEACCESSLISTS";
const char *castor::rh::ACCESSLISTS_CONF = "USEACCESSLISTS";

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  try {
    castor::rh::Server server;

    // retrieve the port to be used
    int port = CSP_RHSERVER_PORT;
    char* sport;
    if ((sport = getenv (castor::rh::PORT_ENV)) != 0 
        || (sport = getconfent((char *)castor::rh::CATEGORY_CONF,
                               (char *)castor::rh::PORT_CONF,0)) != 0) {
      port = castor::System::porttoi(sport);
    }
    // See whether access lists should be used
    bool accessLists = false;
    char* saccessLists;
    if ((saccessLists = getenv(castor::rh::ACCESSLISTS_ENV)) != 0 
        || (saccessLists = getconfent((char *)castor::rh::CATEGORY_CONF,
                                      (char *)castor::rh::ACCESSLISTS_CONF,0)) != 0) {
      accessLists = (strcasecmp(saccessLists, "YES") == 0);
    }
    // If yes, get an rhSvc instance
    castor::rh::IRHSvc* rhSvc = 0;
    if (accessLists) {
      castor::IService* svc = castor::BaseObject::services()->service("DbRhSvc", castor::SVC_DBRHSVC);
      rhSvc = dynamic_cast<castor::rh::IRHSvc*>(svc);
      if (0 == rhSvc) {
        // We are not able to get the request handler service.
        // Since access lists were required, we give up.
        castor::exception::Internal ex;
        ex.getMessage() << "Couldn't load the request handler service, check the castor.conf for DynamicLib entries" << std::endl;
        throw ex;
      }
    }

    // Start daemon
    server.addThreadPool
      (new castor::server::TCPListenerThreadPool
       ("RH", new castor::rh::RHThread(accessLists, rhSvc), port, false));
    // parse the command line (note that this may overwrite
    // the port we are listening to)
    server.parseCommandLine(argc, argv);
    // start the server
    server.start();
  } catch (castor::exception::Exception e) {
    std::cerr << "Caught castor exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  } catch (...) {
    std::cerr << "Caught general exception!" << std::endl;
  }

  dlf_shutdown(1);
  return 0;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::Server::Server() :
  castor::server::BaseDaemon("RequestHandler") {

  // Initializes the DLF logging
  castor::dlf::Message messages[] =
    {{ 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Database"},
     { 3, "Could not get Conversion Service for Streaming"},
     { 4, "Exception caught : server is stopping"},
     { 5, "Exception caught : ignored"},
     { 6, "Invalid Request"},
     { 7, "Unable to read Request from socket"},
     { 8, "Processing Request"},
     { 9, "Exception caught"},
     {10, "Reply sent to client"},
     {11, "Unable to send Ack to client"},
     {12, "Request stored in DB"},
     {13, "Waked up all services at once"},
     {-1, ""}};
  dlfInit(messages);
  
  try {
    // create oracle and streaming conversion service
    // so that it is not deleted and recreated all the time
    castor::ICnvSvc *svc =
      svcs()->cnvService("DbCnvSvc", castor::SVC_DBCNV);
    if (0 == svc) {
      // "Could not get Conversion Service for Database" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2);
    }
    castor::ICnvSvc *svc2 =
      svcs()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
    if (0 == svc2) {
      // "Could not get Conversion Service for Streaming" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3);
    }
  } catch(castor::exception::Exception e) {
    // "Exception caught : server is stopping" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 2, params);
  }
}

//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void castor::rh::Server::help(std::string programName)
{
  std::cout << "Usage: " << programName << " [options]\n"
	  "\n"
	  "where options can be:\n"
	  "\n"
	  "\t--foreground      or -f            \tForeground\n"
	  "\t--help            or -h            \tThis help\n"
	  "\t--port            or -p {integer } \tPort to be used\n"
	  "\t--Rthreads        or -R {integer } \tNumber of Request Handler threads\n"
	  "\n"
	  "Comments to: Castor.Support@cern.ch\n";
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::rh::Server::parseCommandLine(int argc, char *argv[])
{
  Coptions_t longopts[] =
    {
      {"foreground", NO_ARGUMENT,       NULL, 'f'},
      {"help",       NO_ARGUMENT,       NULL, 'h'},
      {"Rthreads",   REQUIRED_ARGUMENT, NULL, 'R'},
      {"port",       REQUIRED_ARGUMENT, NULL, 'p'}
    };
  Coptind = 1;
  Copterr = 0;
  char c;
  while ((c = Cgetopt_long(argc, argv, "fhR:p:", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'h':
      help(argv[0]);
      exit(0);
      break;
    case 'R':
      {
        castor::server::BaseThreadPool* p = m_threadPools['R'];
        p->setNbThreads(atoi(Coptarg));
      }
      break;
    case 'p':
      {
        castor::server::TCPListenerThreadPool* p =
          dynamic_cast<castor::server::TCPListenerThreadPool*>
          (m_threadPools['R']);
        p->setPort(castor::System::porttoi(Coptarg));
      }
      break;
    default:
      help(argv[0]);
      exit(0);
    } 
  }
}
