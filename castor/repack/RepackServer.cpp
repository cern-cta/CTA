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
#include "castor/repack/RepackServer.hpp"

#include "castor/db/DbParamsSvc.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"

// hardcoded schema version of the Repack database
const std::string REPACKSCHEMAVERSION = "2_1_7_10";


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
        castor::exception::Internal e;
        e.getMessage() << "Could not instantiate the parameters service";
        throw e;
     }
     params->setSchemaVersion(REPACKSCHEMAVERSION);
     params->setDbAccessConfFile(ORAREPACKCONFIGFILE);



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
					   10
                                             ));
     server.getThreadPool('c')->setNbThreads(1); 

    /// The Repack File Stager Instance
	  server.addThreadPool(
      new castor::server::SignalThreadPool("Stager", 
					   new castor::repack::RepackFileStager(&server),
					   10
                                          ));
	  server.getThreadPool('S')->setNbThreads(1);

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
                                            new castor::repack::RepackCleaner(&server),10));
	  server.getThreadPool('C')->setNbThreads(1); 


    /// Read the command line parameters
    server.parseCommandLine(argc, argv);

    /// Start the Repack Server
    server.start();


    }
     catch (castor::exception::Exception e) {
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
	castor::server::BaseDaemon("Repack")
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
     {19, "RepackFileStager : Abort issued for tape"},
     {20, "RepackFileStager : Restart issued for tape"},
     {21, "RepackFileStager : Submition issued for tape"},
     {22, "RepackFileStager : No files on tape to repack"},
     {23, "RepackFileStager : Staging files"},
     {24, "RepackFileStager : Not able to send stager_rm request"},
     {25, "RepackFileStager : Stager_rm request sent"},
     {26, "RepackFileStager : Restarted of unknow request is impossible"},
     {27, "RepackFileStager : Restarted failed because of temporary stager problem"},
     {28, "RepackFileStager :  There are still files in staging/migrating. Restart abort"},
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
     {46, "OraRepackSvc : error in storeRequest" },
     {47, "OraRepackSvc : error in updateSubRequest" },
     {48, "OraRepackSvc : error in updateSubRequestSegments" },
     {49, "OraRepackSvc : error in insertSubRequestSegments" },
     {50, "OraRepackSvc : error in getSubRequestByVid" },
     {51, "OraRepackSvc : error in getSubRequestsByStatus" },     
     {52, "OraRepackSvc : error in getAllSubRequests" },
     {53, "OraRepackSvc : error in validateRepackSubRequests" }, 
     {54, "OraRepackSvc : error in resurrectTapesOnHold" }, 
     {55, "OraRepackSvc : error in restartSubRequest" }, 
     {56, "OraRepackSvc : error in changeSubRequestsStatus" }, 
     {57, "OraRepackSvc : error in changeAllSubRequestsStatus" },
     {58, "OraRepackSvc : commit" },
     {59, "FileListHelper : error in getting the file path from the nameserver" },
     {60, "FileListHelper : list of files  retrieved from  the nameserver" },
     {61, "RepackCleaner : impossible to reclaim the tape" },
     {62, "RepackCleaner : reclaimed tape" },
     {63, "RepackCleaner : impossible to change tapepool" },
     {64, "RepackCleaner : changed tapepool" },
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
    m_listenPort = castor::repack::CSP_REPACKSERVER_PORT;



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
  m_ns = new std::string(tmp);


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
  m_stager = new std::string(tmp);


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
  m_serviceClass = new std::string(tmp);

 
  // Parameters to limitate concurrent repack processes 

  // Number of files

   if ( tmp = getconfent("REPACK", "MAXFILES",0)){
     dp=tmp;
     m_maxFiles = strtoul(tmp, &dp, 0);
     if  (*dp != 0 || m_maxFiles<=0) {
       m_maxFiles = castor::repack::DEFAULT_MAX_FILES;
     }
   } else {
     m_maxFiles = castor::repack::DEFAULT_MAX_FILES;   
   }
  
   // Number of tapes

   if ( tmp = getconfent("REPACK", "MAXTAPES",0)){
     dp=tmp;
     m_maxTapes = strtoul(tmp, &dp, 0);
     if  (*dp != 0 || m_maxTapes<=0) {
       m_maxTapes = castor::repack::DEFAULT_MAX_TAPES;
     }
   } else {
     m_maxTapes = castor::repack::DEFAULT_MAX_TAPES; 
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
  m_protocol = new std::string(tmp);

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
    m_pollingTime = castor::repack::CSP_REPACKPOLLTIME;
  /* --------------------------------------------------------------------- */

  if (tmp) free(tmp);
  tmp=NULL;

}




castor::repack::RepackServer::~RepackServer() throw()
{
    if ( m_ns != NULL ) delete m_ns;
    if ( m_stager != NULL ) delete m_stager;
    if ( m_serviceClass != NULL ) delete m_serviceClass;
    if ( m_protocol != NULL ) delete m_protocol;
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



