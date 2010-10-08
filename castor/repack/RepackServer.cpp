/******************************************************************************
 *                      RepackServer.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 *
 * @author CastorDev
 *****************************************************************************/

// Include Files
#include "RepackServer.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/db/DbParamsSvc.hpp"
#include "castor/Constants.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "RepackWorker.hpp"
#include "RepackFileChecker.hpp"
#include "RepackFileStager.hpp"
#include "RepackMonitor.hpp"
#include "RepackCleaner.hpp"
#include "RepackKiller.hpp"
#include "RepackRestarter.hpp"
#include "RepackServer.hpp"

#include "castor/Services.hpp"
#include <Cgetopt.h>
#include <u64subr.h>


extern "C" {
  char* getconfent(const char *, const char *, int);
}

// hardcoded schema version of the Repack database
const std::string REPACKSCHEMAVERSION = "2_1_8_0";

#define SLEEP_TIME 10
#define DEFAULT_MAX_FILES 6000000
#define DEFAULT_MAX_TAPES 300
#define CSP_REPACKPOLLTIME 240 // the standard polling time
#define CSP_REPACKSERVER_PORT 62800


//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  try {

    castor::repack::RepackServer server;

    // Create a db parameters service and fill with appropriate defaults
    castor::IService* s = castor::BaseObject::sharedServices()->service("DbParamsSvc", castor::SVC_DBPARAMSSVC);
    castor::db::DbParamsSvc* params = dynamic_cast<castor::db::DbParamsSvc*>(s);
    if(params == 0) {
      std::cerr << "Could not instantiate the parameters service" << std::endl;
      return -1;
    }

    params->setSchemaVersion(REPACKSCHEMAVERSION);
    params->setDbAccessConfFile(ORAREPACKCONFIGFILE);

    // service to access the database ... just to check, we don't need that

    castor::IService* orasvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
    castor::repack::IRepackSvc* mySvc = dynamic_cast<castor::repack::IRepackSvc*>(orasvc);


    if (0 == mySvc) {
      std::cerr << "Couldn't load the ora repack  service, check the castor.conf for DynamicLib entries" << std::endl;
      return -1;
    }

    /// The Repack Worker Instance
     server.addThreadPool(
      new castor::server::TCPListenerThreadPool("Worker",
                                              new castor::repack::RepackWorker(&server),
                                              server.getListenPort()
                                            ));
     server.getThreadPool('W')->setNbThreads(1);


    /// The Repack File Checker
     server.addThreadPool(
      new castor::server::SignalThreadPool("checker",
					   new castor::repack::RepackFileChecker(&server),
					   SLEEP_TIME
                                             ));
     server.getThreadPool('c')->setNbThreads(1);

    /// The Repack File Stager Instance
	  server.addThreadPool(
      new castor::server::SignalThreadPool("Stager",
					   new castor::repack::RepackFileStager(&server),
					   SLEEP_TIME
                                          ));
	  server.getThreadPool('S')->setNbThreads(1);

    /// The Repack Killer Instance
	  server.addThreadPool(
      new castor::server::SignalThreadPool("killer",
					   new castor::repack::RepackKiller(&server),
					   SLEEP_TIME
                                          ));

	  server.getThreadPool('k')->setNbThreads(1);


    /// The Repack Restarter Instance
	  server.addThreadPool(
      new castor::server::SignalThreadPool("restarter",
					   new castor::repack::RepackRestarter(&server),
					   SLEEP_TIME
                                          ));

	  server.getThreadPool('r')->setNbThreads(1);

    /// The Repack Monitor Instance (only 1 !)
	  server.addThreadPool(
      new castor::server::SignalThreadPool("Monitor",
                                            new castor::repack::RepackMonitor(&server),
                                            server.getPollTime()
                                          ));
	  server.getThreadPool('M')->setNbThreads(1);

    /// The Repack Cleaner
	  server.addThreadPool(
      new castor::server::SignalThreadPool("Cleaner",
                                            new castor::repack::RepackCleaner(&server),SLEEP_TIME));
	  server.getThreadPool('C')->setNbThreads(1);


    /// Read the command line parameters
    server.parseCommandLine(argc, argv);
    server.runAsStagerSuperuser();
    /// Start the Repack Server
    server.start();


    }
     catch (castor::exception::Exception& e) {
      		std::cerr << "Caught castor exception : "
       		<< sstrerror(e.code()) << std::endl
       		<< e.getMessage().str() << std::endl;
     }
     catch (...) {
       	std::cerr << "Caught general exception!" << std::endl;
       	return 1;
     }

     return 0;
}

//------------------------------------------------------------------------------
// RepackServer Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------
castor::repack::RepackServer::RepackServer() :
	castor::server::BaseDaemon("repackd")
{
  // Initializes the DLF logging. This includes
  // defining the predefined messages
  castor::dlf::Message messages[] =
    {{ 1, "Could not get Conversion Service for Database"},
     { 2, "Could not get the OraRepackSvc"},
     { 3, "Exception caught : server is stopping"},
     { 4, "Exception caught : ignored"},
     { 5, "RepackWorker: New Request Arrival"},
     { 6, "RepackWorker: Invalid Request"},
     { 7, "RepackWorker: Unable to read Request from socket"},
     { 8, "RepackWorker: Sending reply to client"},
     { 9, "RepackWorker: Unable to send the response to client "},
     {10, "RepackWorker : Getting tapes for the input tapepool"},
     {11, "RepackWorker : No Such tape"},
     {12, "RepackWorker : Tape not marked as FULL" },
     {13, "RepackFileChecker : Getting tapes to check "},
     {14, "RepackFileChecker : Getting segments for tape"},
     {15, "RepackFileChecker : No segments for tape"},
     {16, "RepackFileChecker : Tape can be submitted"},
     {17, "RepackFileChecker : Tape is kept on-hold"},
     {18, "RepackFileStager : Getting tapes to handle"},
     {19, "RepackKiller : Abort issued for tape"},
     {20, "RepackRestarter : Restart issued for tape"},
     {21, "RepackFileStager : Submition issued for tape"},
     {22, "RepackFileStager : No files on tape to repack"},
     {23, "RepackFileStager : Staging files"},
     {24, "RepackKiller : Not able to send stager_rm request"},
     {25, "RepackKiller : Stager_rm request sent"},
     {26, "RepackRestarter : Restarted of unknow request is impossible"},
     {27, "RepackRestarter : Restarted failed because of temporary stager problem"},
     {28, "RepackRestarter :  There are still files in staging/migrating. Restart abort"},
     {29, "RepackFileStager :  Error sending the repack request to the stager"},
     {30, "RepackFileStager : Repack request sent to the stager"},
     {31, "RepackFileStager : Changing CUUID to stager one"},
     {32, "RepackFileStager : Error getting answer from the stager"},
     {33, "RepackFileStager : Failed to submit recall for file to Stager"},
     {34, "RepackMonitor : Getting tape to monitor"},
     {35, "RepackMonitor : Updating tape"},
     {36, "RepackMonitor : Stager query failed"},
     {37, "RepackMonitor : Repack completed"},
     {38, "RepackCleaner : Repack failed and we trigger an automatic retry"},
     {39, "RepackMonitor : Repack failed"},
     {40, "RepackMonitor : Updated tape"},
     {41, "RepackCleaner : Getting finished tapes to check" },
     {42, "RepackCleaner : Checking tape" },
     {43, "RepackCleaner : Resurrecting on-hold tapes" },
     {44, "RepackCleaner : Files left on the tape" },
     {45, "RepackCleaner : Success in repacking the tape" },
     {46, "FileListHelper : error in getting the file path from the nameserver" },
     {47, "FileListHelper : list of files  retrieved from  the nameserver" },
     {48, "RepackCleaner : impossible to reclaim the tape" },
     {49, "RepackCleaner : reclaimed tape" },
     {50, "RepackCleaner : impossible to change tapepool" },
     {51, "RepackCleaner : changed tapepool" },
     {52, "RepackCleaner : impossible to get tapes to clean" },
     {53, "RepackCleaner : impossible to update tape" },
     {54, "RepackCleaner : impossible to restart  on-hold tapes" },
     {55, "RepackFileChecker : impossible to get tapes to check" },
     {56, "RepackFileChecker : impossible to update tape" },
     {57, "RepackFileStager : impossible to get tapes to stage" },
     {58, "RepackFileStager : impossible to stage subrequest" },
     {59, "RepackKiller : impossible to get tapes to abort" },
     {60, "RepackKiller : impossible to abort subrequest" },
     {61, "RepackRestarter : impossible to get tapes to restart" },
     {62, "RepackRestarter : impossible to restart tape" },
     {63, "RepackKiller : Getting tapes to handle"},
     {64, "RepackRestarter : Getting tapes to handle"},
     {65, "RepackMonitor : impossible to get tapes to monitor" },
     {66, "RepackMonitor : impossible to get statistics about tape" },
     {67, "RepackWorker : impossible to perform"},
     {68, "RepackMonitor : number of files on tape increased!"},
     {69, "RepackMonitor : tape empty but the process is still ONGOING"},
     {-1, ""}};

  dlfInit(messages);

  /** the Command line parameters */
  m_cmdLineParams << "fsh";



  char* tmp=NULL;
  char* dp=NULL;

  /* --------------------------------------------------------------------- */
  /** This gets the repack port configuration in the enviroment, this
      is the only on, which is taken from the enviroment */

  if ( (tmp = getenv ("REPACK_PORT")) != 0 ) {
      char* dp = tmp;
      m_listenPort = strtoul(tmp, &dp, 0);

      if (*dp != 0) {
        castor::exception::Internal ex;
        ex.getMessage() << "Bad port value in enviroment variable "
                        << tmp << std::endl;
        throw ex;
      }
      if ( m_listenPort > 65535 ){
        castor::exception::Internal ex;
        ex.getMessage() << "Given port no. in enviroment variable "
                        << "exceeds 65535 !"<< std::endl;
        throw ex;
    }
  }
  else
    m_listenPort = CSP_REPACKSERVER_PORT;



  /* --------------------------------------------------------------------- */

  /** the RepackServer reads the configuration for Nameserver, stager etc.
      at the beginning and keeps the information for the threads
    */

  if ( !(tmp = getconfent("CNS", "HOST",0)) &&
       !(tmp = getenv("CNS_HOST")) ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with nameserver "
                    << "entry in castor config file or enviroment variable"
		    << std::endl;
    throw ex;
  }
  m_ns = std::string(tmp);


  /* --------------------------------------------------------------------- */

  /** the stager name
  */
  if ( !(tmp = getenv("RH_HOST")) &&
       !(tmp = getconfent("RH", "HOST",0))
     ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with stager "
                    << "entry in castor config file or enviroment variable"
		    << std::endl;
    throw ex;
  }
  m_stager = std::string(tmp);



  /* --------------------------------------------------------------------- */

  /** Get the repack service class, which is used for staging in files
    * The diskpool in this service class is used for recall process.
    */
  if ( !(tmp = getconfent("REPACK", "SVCCLASS",0)) &&
       !(tmp = getenv ("REPACK_SVCCLASS")) ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with service class "
                    << "entry in castor config file or enviroment variable"
		    << std::endl;
    throw ex;
  }
  m_serviceClass = std::string(tmp);



  // Parameters to limitate concurrent repack processes

  // Number of files

   if ((tmp = getconfent("REPACK", "MAXFILES",0))) {
     dp=tmp;
     m_maxFiles = strtoul(tmp, &dp, 0);
     if  (*dp != 0 || m_maxFiles<=0) {
       m_maxFiles = DEFAULT_MAX_FILES;
     }
   } else {
     m_maxFiles = DEFAULT_MAX_FILES;
   }


   // Number of tapes

   if ((tmp = getconfent("REPACK", "MAXTAPES",0))) {
     dp=tmp;
     m_maxTapes = strtoul(tmp, &dp, 0);
     if  (*dp != 0 || m_maxTapes<=0) {
       m_maxTapes = DEFAULT_MAX_TAPES;
     }
   } else {
     m_maxTapes = DEFAULT_MAX_TAPES;
   }



  /* --------------------------------------------------------------------- */

  /** Get the repack service class, which is used for staging in files
    * The diskpool in this service class is used for recall process.
    */
  if ( !(tmp = getconfent("REPACK", "PROTOCOL",0)) &&
       !(tmp = getenv("REPACK_PROTOCOL")) ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with protocol "
                    << "entry in castor config file" << std::endl;
    throw ex;
  }
  m_protocol = std::string(tmp);


  /* --------------------------------------------------------------------- */

  /** Get the polling time for the Monitor and service. This value should be not less
      than 120 ( to stage a file takes about that time).
    */
  if ( (tmp = getenv ("REPACK_POLLTIME")) != 0 ){
    dp = tmp;
    m_pollingTime = strtoul(tmp, &dp, 0);

    if (*dp != 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Bad polling time value in enviroment variable "
                      << tmp << std::endl;
      throw ex;
    }
    if ( m_pollingTime > 65535 ){
      castor::exception::Internal ex;
      ex.getMessage() << "Given polling time no. in enviroment variable "
                      << "exceeds limit !" << std::endl;
      throw ex;
    }
  }
  else
    m_pollingTime = CSP_REPACKPOLLTIME;


  /** Get IRepackSvc */

  // Create a db parameters service and fill with appropriate defaults
  castor::IService* s = castor::BaseObject::sharedServices()->service("DbParamsSvc", castor::SVC_DBPARAMSSVC);
  castor::db::DbParamsSvc* params = dynamic_cast<castor::db::DbParamsSvc*>(s);
  if(params == 0) {

    castor::exception::Internal ex;
    ex.getMessage() << "Could not instantiate the parameters service "
		    << std::endl;
    throw ex;
  }

  params->setSchemaVersion(REPACKSCHEMAVERSION);
  params->setDbAccessConfFile(ORAREPACKCONFIGFILE);

}




castor::repack::RepackServer::~RepackServer() throw()
{
    m_ns.clear();
    m_stager.clear();
    m_serviceClass.clear();
    m_protocol.clear();
    m_maxFiles=0;
    m_maxTapes=0;
    m_pollingTime = 0;
    m_listenPort =0 ;
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::repack::RepackServer::parseCommandLine(int argc, char *argv[])
{
  Coptions_t* longopts = new Coptions_t[2];

  longopts[0].name = "foreground";
  longopts[0].has_arg = NO_ARGUMENT;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';
  longopts[1].name = "help";
  longopts[1].has_arg = NO_ARGUMENT;
  longopts[1].flag = NULL;
  longopts[1].val = 'h';

  Coptind = 1;
  Copterr = 0;
  char c;

  while ((c = Cgetopt_long(argc, argv, (char*)m_cmdLineParams.str().c_str(), longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'h':
      help(argv[0]);
      exit(0);
      break;
    default:
      help(argv[0]);
      exit(0);
    }

  }
  delete [] longopts;
}

//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void castor::repack::RepackServer::help(std::string programName)
{
  std::cout << "\nUsage: " << programName << " [options]\n"
	    << "where options can be:\n\n"
            << "\t--foreground      or -f                \tForeground\n"
            << "\t--help            or -h                \tThis help\n"
            << "Comments to: Castor.Support@cern.ch "<<std::endl;

}



