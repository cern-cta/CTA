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
 * @(#)$RCSfile: Server.cpp,v $ $Revision: 1.65 $ $Release$ $Date: 2008/09/01 17:26:48 $ $Author: waldron $
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/rh/Server.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/server/AuthListenerThreadPool.hpp"
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
const char *castor::rh::PORT_SEC_ENV = "RH_SEC_PORT";
const char *castor::rh::PORT_SEC_CONF = "SEC_PORT";
const char *castor::rh::ACCESSLISTS_ENV = "RH_USEACCESSLISTS";
const char *castor::rh::ACCESSLISTS_CONF = "USEACCESSLISTS";
const char *castor::rh::CASTOR_SEC_CONF = "CSEC";


//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  try {
    castor::rh::Server server;

    // parse the command line 
    server.parseCommandLine(argc, argv);

    // "RequestHandler started"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Port", server.m_port),
       castor::dlf::Param("Secure", server.m_secure ? "yes" : "no")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 16, 2, params);

    // start the server
    server.start();
   
  } catch (castor::exception::Exception e) {
    std::cerr << "Caught castor exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  } catch (...) {
    std::cerr << "Caught general exception!" << std::endl;
  }
  
  return 0;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::Server::Server() :
  castor::server::BaseDaemon("RequestHandler"),
  m_port(-1),
  m_secure(false) {

  // Initializes the DLF logging
  castor::dlf::Message messages[] =
    {{  1, "New Request Arrival" },
     {  2, "Could not get Conversion Service for Database" },
     {  3, "Could not get Conversion Service for Streaming" },
     {  4, "Exception caught : server is stopping" },
     {  5, "Exception caught : ignored" },
     {  6, "Invalid Request" },
     {  7, "Unable to read Request from socket" },
     {  8, "Processing Request" },
     {  9, "Exception caught" },
     { 10, "Reply sent to client" },
     { 11, "Unable to send Ack to client" },
     { 12, "Request stored in DB" },
     { 13, "Waked up all services at once" },
     { 14, "Permission Denied" },
     { 15, "Exception caught : failed to rollback transaction" },
     { 16, "RequestHandler started" },
     { -1, "" }};
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
    "\t--foreground            or -f           \tForeground\n"
    "\t--help                  or -h           \tThis help\n"
    "\t--secure                or -s           \tRun the daemon in secure mode\n"
    "\t--config <config-file>  or -c           \tConfiguration file\n"
    "\t--port                  or -p {integer} \tPort to be used\n"
    "\t--Rthreads              or -R {integer} \tNumber of Request Handler threads\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::rh::Server::parseCommandLine(int argc, char *argv[]) throw (castor::exception::Exception)
{
  Coptions_t longopts[] =
    {
      {"foreground", NO_ARGUMENT,       NULL, 'f'},
      {"help",       NO_ARGUMENT,       NULL, 'h'},
      {"Secure",     NO_ARGUMENT,       NULL, 's'},
      {"config",     REQUIRED_ARGUMENT, NULL, 'c'},
      {"Rthreads",   REQUIRED_ARGUMENT, NULL, 'R'},
      {"port",       REQUIRED_ARGUMENT, NULL, 'p'},
    };
  Coptind = 1;
  Copterr = 0;
  char c;
  char *sport;
  int iport = -1;
  int nbThreads = -1;
 
  while ((c = Cgetopt_long(argc, argv, "fhsR:p:c:", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'c':
      {
        char* cfgFile = (char *)malloc(strlen("PATH_CONFIG=") + strlen(Coptarg) + 1);
        if(cfgFile != NULL) {
          sprintf(cfgFile,"PATH_CONFIG=%s", Coptarg);
          printf("Using configuration file %s\n", Coptarg);
          putenv(cfgFile);
        }
        free(cfgFile);
      }
      break;
    case 'h':
      help(argv[0]);
      exit(0);
      break;
    case 's':
      m_secure = true;
      break;
    case 'R':
      nbThreads = atoi(Coptarg); 
      break;
    case 'p':
      iport = castor::System::porttoi(Coptarg);
      break;
    default:
      help(argv[0]);
      exit(0);
    } 
  }

  // See whether access lists should be used
  bool accessLists = false;
  char* saccessLists;
  if ((saccessLists = getenv(castor::rh::ACCESSLISTS_ENV)) != 0
      || (saccessLists = getconfent((char *)castor::rh::CATEGORY_CONF,
                                    (char *)castor::rh::ACCESSLISTS_CONF,0)) != 0) {
    accessLists = (strcasecmp(saccessLists, "YES") == 0);
  }

  if (m_secure) {
    if (iport == -1) {
      if ((sport = getenv (castor::rh::PORT_SEC_ENV)) != 0
          || (sport = getconfent((char *)castor::rh::CATEGORY_CONF,
				 (char *)castor::rh::PORT_SEC_CONF,0)) != 0) {
        m_port = castor::System::porttoi(sport);
      } else { //default port
	m_port = CSP_RHSERVER_SEC_PORT;
      }
    } else {
      m_port = iport;
    }  
    addThreadPool
      (new castor::server::AuthListenerThreadPool
       ("RH", new castor::rh::RHThread(accessLists), m_port));   
  } else { 
    if (iport == -1) {
      if ((sport = getenv (castor::rh::PORT_ENV)) != 0
          || (sport = getconfent((char *)castor::rh::CATEGORY_CONF,
				 (char *)castor::rh::PORT_CONF,0)) != 0) {
        m_port = castor::System::porttoi(sport);
      } else { //default port
	m_port = CSP_RHSERVER_PORT;
      }
    } else {
      m_port = iport;
    }
    addThreadPool
      (new castor::server::TCPListenerThreadPool
       ("RH", new castor::rh::RHThread(accessLists), m_port));
  }

  if (nbThreads != -1) {
    castor::server::BaseThreadPool* p = m_threadPools['R'];
    p->setNbThreads(nbThreads);
  }
}
