/******************************************************************************
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
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/log/SyslogLogger.hpp"
#include "castor/rh/RHThread.hpp"
#include "castor/rh/Server.hpp"
#include "castor/rh/SvcClassCounter.hpp"
#include "castor/rh/UserCounter.hpp"
#include "castor/rh/IRHSvc.hpp"
#include "castor/server/AuthListenerThreadPool.hpp"
#include "castor/server/metrics/MetricsCollector.hpp"
#include "castor/server/metrics/ObjTypeCounter.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/Services.hpp"
#include "castor/System.hpp"
#include "h/common.h"   // for getconfent
#include "h/patchlevel.h"

#include <iostream>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>

//------------------------------------------------------------------------------
// String constants
//------------------------------------------------------------------------------
const char *castor::rh::CATEGORY_CONF = "RH";
const char *castor::rh::PORT_ENV = "RH_PORT";
const char *castor::rh::PORT_CONF = "PORT";
const char *castor::rh::PORT_SEC_ENV = "RH_SEC_PORT";
const char *castor::rh::PORT_SEC_CONF = "SEC_PORT";

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  try {
    castor::log::SyslogLogger log("rhd");
    castor::rh::Server server(std::cout, std::cerr, log);

    // parse the command line
    server.parseCommandLine(argc, argv);

    // "RequestHandler started"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Port", server.m_port),
       castor::dlf::Param("Secure", server.m_secure ? "yes" : "no")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 16, 2, params);

    // start the server
    const bool runAsStagerSuperuser = true;
    server.start(runAsStagerSuperuser);
    return 0;

  } catch (castor::exception::Exception& e) {
    std::cerr << "Caught castor exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  } catch (...) {
    std::cerr << "Caught general exception!" << std::endl;
  }

  return 1;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::Server::Server(std::ostream &stdOut, std::ostream &stdErr,
  castor::log::Logger &log) :
  castor::server::MultiThreadedDaemon(stdOut, stdErr, log),
  m_port(-1),
  m_secure(false),
  m_waitIfBusy(true),
  m_dlopenHandle(0) {

  // Initializes the DLF logging
  castor::dlf::Message messages[] =
    {{  1, "New Request Arrival" },
     {  2, "Could not get Conversion Service for Database" },
     {  3, "Could not get Conversion Service for Streaming" },
     {  4, "Exception caught : server is stopping" },
     {  5, "Exception caught : ignored" },
     {  6, "Invalid Request" },
     {  7, "Unable to read Request from socket" },
     {  9, "Exception caught" },
     { 10, "Reply sent to client" },
     { 11, "Unable to send Ack to client" },
     { 14, "Permission Denied" },
     { 15, "Exception caught : failed to rollback transaction" },
     { 16, "RequestHandler started" },
     { 17, "Security problem" },
     { 18, "Failed to initialize rate limiter" },
     { 19, "Too many requests, enforcing rate limit" },
     { 20, "Exception caught in rate limiter, ignoring" },
     { -1, "" }};
  dlfInit(messages);

  try {
    // create oracle and streaming conversion service
    // so that it is not deleted and recreated all the time
    castor::ICnvSvc *svc =
      BaseObject::services()->cnvService("DbCnvSvc", castor::SVC_DBCNV);
    if (0 == svc) {
      // "Could not get Conversion Service for Database" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2);
    }
    castor::ICnvSvc *svc2 =
      BaseObject::services()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
    if (0 == svc2) {
      // "Could not get Conversion Service for Streaming" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3);
    }
  } catch(castor::exception::Exception& e) {
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
    "\t--secure                or -s           \tRun the daemon in secure mode\n"
    "\t--config <config-file>  or -c           \tConfiguration file\n"
    "\t--port                  or -p {integer} \tPort to be used\n"
    "\t--no-wait               or -n           \tDon't wait to dispatch requests when all threads are busy\n"
    "\t--metrics               or -m           \tEnable metrics collection\n"
    "\t--Rthreads              or -R {integer} \tNumber of Request Handler threads\n"
    "\t--help                  or -h           \tPrint this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::rh::Server::parseCommandLine(int argc, char *argv[]) 
{
  bool foreground = false; // Should the daemon run in the foreground?
  Coptions_t longopts[] =
    {
      {"foreground", NO_ARGUMENT,       NULL, 'f'},
      {"help",       NO_ARGUMENT,       NULL, 'h'},
      {"secure",     NO_ARGUMENT,       NULL, 's'},
      {"config",     REQUIRED_ARGUMENT, NULL, 'c'},
      {"Rthreads",   REQUIRED_ARGUMENT, NULL, 'R'},
      {"port",       REQUIRED_ARGUMENT, NULL, 'p'},
      {"nowait",     NO_ARGUMENT,       NULL, 'n'},
      {"metrics",    NO_ARGUMENT,       NULL, 'm'},
      {NULL,         0,                 NULL,  0 }
    };
  Coptind = 1;
  Copterr = 0;
  char c;
  char *sport;
  int iport = -1;
  int nbThreads = -1;
  bool metrics = false;

  while ((c = Cgetopt_long(argc, argv, "fsR:p:c:nmh", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      foreground = true;
      break;
    case 'c':
      {
        char* cfgFile = (char *)malloc(strlen("PATH_CONFIG=") + strlen(Coptarg) + 1);
        if (cfgFile != NULL) {
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
    case 'n':
      m_waitIfBusy = false;
      break;
    case 'm':
      metrics = true;
      break;
    default:
      help(argv[0]);
      exit(0);
    }
  }

  if (m_secure) {
    if (iport == -1) {
      if ((sport = getenv (castor::rh::PORT_SEC_ENV)) != 0
          || (sport = getconfent((char *)castor::rh::CATEGORY_CONF,
                                 (char *)castor::rh::PORT_SEC_CONF, 0)) != 0) {
        m_port = castor::System::porttoi(sport);
      } else { // default port
        m_port = CSP_RHSERVER_SEC_PORT;
      }
    } else {
      m_port = iport;
    }
    // Temporary workaround till we come up with something more clever to fix
    // bug related with the KRB5 and GSI mixed libraries.
    char filename[CA_MAXNAMELEN];
    snprintf(filename, CA_MAXNAMELEN, "libcastorsec_plugin_KRB5.so.%d.%d",
             MAJORVERSION, MINORVERSION);
    m_dlopenHandle = dlopen (filename, RTLD_LAZY);
    if (!m_dlopenHandle) {
      fprintf (stderr, "%s\n", dlerror());
      exit(EXIT_FAILURE);
    }
    addThreadPool
      (new castor::server::AuthListenerThreadPool
       ("RH", new castor::rh::RHThread(), m_port, m_waitIfBusy, 8, 10));
  } else {
    if (iport == -1) {
      if ((sport = getenv (castor::rh::PORT_ENV)) != 0
          || (sport = getconfent((char *)castor::rh::CATEGORY_CONF,
                                 (char *)castor::rh::PORT_CONF, 0)) != 0) {
        m_port = castor::System::porttoi(sport);
      } else { // default port
        m_port = CSP_RHSERVER_PORT;
      }
    } else {
      m_port = iport;
    }
    addThreadPool
      (new castor::server::TCPListenerThreadPool
       ("RH", new castor::rh::RHThread(), m_port, m_waitIfBusy, 10, 20));
  }

  if (nbThreads != -1) {
    castor::server::BaseThreadPool* p = getThreadPool('R');
    p->setNbThreads(nbThreads);
  }
  
  if(metrics) {
    // initialize the metrics collector thread and add custom metrics
/*
    castor::metrics::MetricsCollector* mc =
      castor::metrics::MetricsCollector::getInstance(this);
    mc->addHistogram(new castor::metrics::Histogram(
      "IncomingRequests", &castor::metrics::ObjTypeCounter::instantiate));
    mc->addHistogram(new castor::metrics::Histogram(
      "SvcClasses", &castor::rh::SvcClassCounter::instantiate));
    mc->addHistogram(new castor::metrics::Histogram(
      "Users", &castor::rh::UserCounter::instantiate));
*/
  }
  setCommandLineHasBeenParsed(foreground);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::rh::Server::~Server() throw() {
  if(NULL != m_dlopenHandle) {
    dlclose(m_dlopenHandle);
  }
}

