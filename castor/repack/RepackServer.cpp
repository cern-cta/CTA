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
 * @(#)$RCSfile: RepackServer.cpp,v $ $Revision: 1.24 $ $Release$ $Date: 2006/12/13 12:59:16 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm 
 *****************************************************************************/

// Include Files
#include "castor/repack/RepackServer.hpp"





//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {

	try {

    castor::repack::RepackServer server;
	
    /// The Repack Worker Instance
    server.addThreadPool(
      new castor::server::ListenerThreadPool("Worker", 
                                              new castor::repack::RepackWorker(&server),
                                              server.getListenPort()
                                            ));
	  server.getThreadPool('W')->setNbThreads(1);
	
    /// The Repack File Checker
    server.addThreadPool(
      new castor::server::SignalThreadPool("checker",
                                            new castor::repack::RepackFileChecker(&server)
                                             ));
    server.getThreadPool('c')->setNbThreads(1);

    /// The Repack File Stager Instance
    server.addThreadPool(
      new castor::server::SignalThreadPool("Stager", 
                                            new castor::repack::RepackFileStager(&server)
                                          ));
	  server.getThreadPool('S')->setNbThreads(1);

    /// The Repack Monitor Instance (only 1 !)
	  server.addThreadPool(
      new castor::server::SignalThreadPool("Monitor", 
                                            new castor::repack::RepackMonitor(&server),
                                            0,
                                            server.getPollTime()
                                          ));
	  server.getThreadPool('M')->setNbThreads(1);
    
    /// The Repack Cleaner
	  server.addThreadPool(
      new castor::server::SignalThreadPool("Cleaner",
                                            new castor::repack::RepackCleaner(&server),
                                            0,
                                            10
                                          ));
	  server.getThreadPool('C')->setNbThreads(1);

    /// The Repack Synchroniser (normally switched off, parseCommandLine will enable it */
    server.addThreadPool(
      new castor::server::SignalThreadPool("Zychroniser",
                                            new castor::repack::RepackSynchroniser(&server),
                                            0
                                          ));
    server.getThreadPool('Z')->setNbThreads(0); 
    
    /// Read the command line parameters
    server.parseCommandLine(argc, argv);

    /// Start the Repack Server
    server.start();


    } catch (castor::exception::Internal i){
    	std::cerr << "Caught castor internal exception : "
			<< sstrerror(i.code()) << std::endl
			<< i.getMessage().str() << std::endl;
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
	castor::server::BaseDaemon("RepackServer")
{
  // Initializes the DLF logging. This includes
  // defining the predefined messages
   

  castor::BaseObject::initLog("Repack", castor::SVC_STDMSG);
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Database"},
     { 3, "Could not get Conversion Service for Streaming"},
     { 4, "Exception caught : server is stopping"},
     { 5, "Exception caught : ignored"},
     { 6, "Invalid Request"},
     { 7, "Unable to read Request from socket"},
     { 8, "Processing Request"},
     { 9, "Exception caught"},
     {10, "Sending reply to client"},		// remove ?
     {11, "Unable to send Ack to client"},
     {12, "DatabaseHelper: Request stored in DB"},
     {13, "DatabaseHelper: Unable to store Request in DB"},
     {14, "FileListHelper: Fetching files from Nameserver"},							// FileListHelper::getFileList()
     {15, "FileListHelper: Cannot get file pathname"},									// FileListHelper::getFilePathnames
     {16, "RepackWorker : No such Tape"},													// RepackWorker:getTapeInfo()
     {17, "RepackWorker : Tape has unkown status, repack abort for this tape"},// RepackWorker:getTapeInfo()
     {18, "RepackWorker : Tape is marked as FREE, no repack to be done"},		// RepackWorker:getTapeInfo()
     {19, "RepackWorker : No such pool"},													// RepackWorker:getPoolInfo()
     {20, "RepackWorker : Adding tapes for pool repacking"},							// RepackWorker:getPoolInfo()
     {21, "RepackFileStager: Unable to stage files"},									// RepackFileStager:stage_files
     {22, "RepackFileStager: New request for staging files"},							// RepackFileStager:run()
     {23, "RepackFileStager: Error during multi-Repack check"},// RepackFileStager:stage_files
     {24, "FileListHelper: Retrieved segs for SubRequest"},							//FileListHelper:getFileListSegs()
     {25, "RepackFileStager: Stager request sent and RepackSubRequest updated"},		// RepackFileStager:stage_files
     {26, "RepackFileStager: Staging files"},												// RepackFileStager:stage_files
     {27, "DatabaseHelper: Unable to update RepackSubRequest"},
     {28, "DatabaseHelper: Tape already in repack que!"},
     {29, "RepackFileChecker: Getting Segs for RepackSubRequest"},
     {30, "DatabaseHelper: RepackSubRequest updated"},
     {31, "RepackFileStager: Found same TapeCopy on two tapes"},
     {33, "RepackFileStager: Changing CUUID to stager one"},
     {34, "RepackCleaner: No files found for cleanup phase"},
     {35, "RepackCleaner: Cleaner started"},
     {36, "There are no more files on tape to repack"},
     {39, "RepackFileChecker: No files found on tape"},               // RepackFileStager:stage_files
     {37, "RepackFileStager: checkExistingTapeCopy failed"}, 
     {38, "RepackFileStager: Failed to submit recall for file to Stager"},
     {40, "RepackMonitor: Changing status"},
     {41, "Stager query failed"},
     {42, "RepackMonitor: Files in invalid status found"},
     {43, "Will continue to send new RepackRequest for remaining files"},
     {44, "There are still files in staging/migrating. Restart abort"},
     {45, "RepackFileStager: File has already a STAGED diskcopy. To be restarted later"},
     {46, "FileListHelper: Found same file twice on tape" },
     {47, "No results yet found for this CUUID. Will try again later." },
     {48, "RepackMonitor: Stager doesn't know this request id. Assuming it is over."},
     {99, "TODO::MESSAGE"},
     {-1, ""}};
  castor::dlf::dlf_init("Repack", messages);

  /** the Command line parameters */
  m_cmdLineParams << "fsh";

  
  /* --------------------------------------------------------------------- */
  /** This gets the repack port configuration in the enviroment, this 
      is the only on, which is taken from the enviroment */
  char* tmp;

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
  free(tmp);

  /* --------------------------------------------------------------------- */

  /** the RepackServer reads the configuration for Nameserver, stager etc.
      at the beginning and keeps the information for the threads
    */
  char* tmp2;
  if ( !(tmp2 = getconfent("CNS", "HOST",0)) &&
       !(tmp2 = getenv("CNS_HOST")) ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with nameserver "
                    << "entry in castor config file or enviroment variable"
		    << std::endl;
    throw ex; 
  }
  m_ns = new std::string(tmp2);


  /* --------------------------------------------------------------------- */

  /** the stager name 
  */
  if ( !(tmp2 = getconfent("RH", "HOST",0)) &&
       !(tmp2 = getenv("RH_HOST")) ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with stager "
                    << "entry in castor config file or enviroment variable" 
		    << std::endl;
    throw ex; 
  }
  m_stager = new std::string(tmp2);


  /* --------------------------------------------------------------------- */

  /** Get the repack service class, which is used for staging in files 
    * The diskpool in this service class is used for recall process.
    */
  if ( !(tmp2 = getconfent("REPACK", "SVCCLASS",0)) && 
       !(tmp2 = getenv ("REPACK_SVCCLASS")) ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with service class "
                    << "entry in castor config file or enviroment variable"
		    << std::endl;
    throw ex; 
  }
  m_serviceClass = new std::string(tmp2);

  /* --------------------------------------------------------------------- */

  /** Get the repack service class, which is used for staging in files 
    * The diskpool in this service class is used for recall process.
    */
  if ( !(tmp2 = getconfent("REPACK", "PROTOCOL",0)) &&
       !(tmp2 = getenv("REPACK_PROTOCOL")) ){
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialise RepackServer with protocol "
                    << "entry in castor config file" << std::endl;
    throw ex; 
  }
  m_protocol = new std::string(tmp2);

  /* --------------------------------------------------------------------- */

  /** Get the polling time for the Monitor and Cleaner service. This value should be not less
      than 120 ( to stage a file takes about that time).
    */
  if ( (tmp2 = getenv ("REPACK_POLLTIME")) != 0 ){
    char* dp = tmp2;
    m_pollingTime = strtoul(tmp2, &dp, 0);
      
    if (*dp != 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Bad polling time value in enviroment variable " 
                      << tmp2 << std::endl;
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


}




castor::repack::RepackServer::~RepackServer() throw()
{
    if ( m_ns != NULL ) delete m_ns;
    if ( m_stager != NULL ) delete m_stager;
    if ( m_serviceClass != NULL ) delete m_serviceClass;
    if ( m_protocol != NULL ) delete m_protocol;
    m_pollingTime = 0;
}




//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::repack::RepackServer::parseCommandLine(int argc, char *argv[])
{
  const char* cmdParams = m_cmdLineParams.str().c_str();
  Coptions_t* longopts = new Coptions_t[m_threadPools.size()+4];
  char tparam[] = "Xthreads";
  
  longopts[0].name = "foreground";
  longopts[0].has_arg = NO_ARGUMENT;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';
  longopts[1].name = "synchronise";
  longopts[1].has_arg = NO_ARGUMENT;
  longopts[1].flag = NULL;
  longopts[1].val = 's';
  longopts[2].name = "help";
  longopts[2].has_arg = NO_ARGUMENT;
  longopts[2].flag = NULL;
  longopts[2].val = 'h';
  
  std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
  int i = 3;
  for(tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++, i++) {
    tparam[0] = tp->first;
    longopts[i].name = strdup(tparam);
    longopts[i].has_arg = REQUIRED_ARGUMENT;
    longopts[i].flag = NULL;
    longopts[i].val = tp->first;
    };
  longopts[i].name = 0;

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long(argc, argv, (char*)cmdParams, longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 's':
      { 
        /** if we synchronise, foreground mode is forced, so we can talk to the user */
        if ( !m_foreground )
          m_foreground = true;
        
        /** only the RepackSychroniser is active */
        m_synchronise = true;
        m_threadPools['W']->setNbThreads(0);
        m_threadPools['C']->setNbThreads(0);
        m_threadPools['M']->setNbThreads(0);
        m_threadPools['S']->setNbThreads(0);
        m_threadPools['c']->setNbThreads(0);
        m_threadPools['Z']->setNbThreads(1);
        break;
      }
      break;
    case 'h':
      help(argv[0]);
      exit(0);
      break;
    default:
      castor::server::BaseThreadPool* p = m_threadPools[c];
      if(p != 0) {
        p->setNbThreads(atoi(Coptarg));
      }
      else {
        help(argv[0]);
        exit(0);
      }
      break;
    }
  }
  delete [] longopts;
}





