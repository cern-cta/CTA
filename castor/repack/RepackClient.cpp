/******************************************************************************
 *                      RepackClient.cpp
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
 * @(#)$RCSfile: RepackClient.cpp,v $ $Revision: 1.30 $ $Release$ $Date: 2007/09/21 13:37:47 $ $Author: gtaur $
 *
 * The Repack Client.
 * Creates a RepackRequest and send it to the Repack server, specified in the 
 * castor config file, or from the enviroment.
 * 
 * For the enviroment serveral possibilities are possible:
 * REPACK_HOST and REPACK_HOST_ALT , the REPACK_HOST_ALT  is an alternative, if 
 * no server entry is found in the config file or REPACK_HOST is not set.
 * REPACK_PORT and REPACK_PORT_ALT : same as for the REPACK_HOST
 *
 * 
 * @author Felix Ehm
 *****************************************************************************/


/* Client  includes */
#include "castor/repack/RepackClient.hpp"
#include "castor/repack/FileListHelper.hpp"
#include <time.h>
#include <ios>

/** 
 * By including the Header file, the Factory is automatically active !! 
 */
#include "castor/io/StreamRepackRequestCnv.hpp"
#include "castor/io/StreamRepackSubRequestCnv.hpp"
#include "castor/io/StreamRepackAckCnv.hpp"







//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) 
{
   castor::repack::RepackClient *client = new castor::repack::RepackClient();
   try{
      client->run(argc, argv);
   } 
   catch (castor::exception::Exception ex) {
      std::cout << ex.getMessage().str() << std::endl;
   }

}



namespace castor {

 namespace repack {


  const char* HOST_ENV_ALT = "REPACK_HOST_ALT";
  const char* HOST_ENV = "REPACK_HOST";
  const char* PORT_ENV_ALT = "REPACK_PORT_ALT";
  const char* PORT_ENV = "REPACK_PORT";
  const char* CATEGORY_CONF = "REPACK";
  const char* HOST_CONF = "HOST";
  const char* PORT_CONF = "PORT";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackClient::RepackClient()
{
  /* the default server port */
  m_defaultport = CSP_REPACKSERVER_PORT;
  /* the default repackserver host */
  m_defaulthost = "localhost";
  std::string clientname = "RepackClient";

  cp.vid =  NULL;
  cp.pool = NULL;
  cp.serviceclass = NULL;
  cp.stager = NULL ;
  cp.retryMax = -1;

  svc = svcs()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
  if (0 == svc) {
      // "Could not get Conversion Service for Streaming" message
	std::cerr << "Could not get Conversion Service for Streaming" << std::endl;
	return;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RepackClient::~RepackClient() throw()
{
  svc->release();
}





//------------------------------------------------------------------------------
// parseInput
//------------------------------------------------------------------------------
bool RepackClient::parseInput(int argc, char** argv)
{
  const char* cmdParams = "o:a:AS:sR:V:P:r:hx:m:";
  if (argc == 1){
    return false;
  }

  if (getuid() == 0){
    std::cout <<"You cannot use repack commands if you are root."<< std::endl; 
    return false;
  }
  struct Coptions longopts[] = {
    {"output_svcclass", REQUIRED_ARGUMENT, 0, 'o'},
    {"status", 0,NULL, 's' },
    {"stager", REQUIRED_ARGUMENT,NULL, 'S' },
    {"delete", REQUIRED_ARGUMENT,NULL, 'R' },
    {"volumeid", REQUIRED_ARGUMENT, NULL, 'V'},
    {"restart", REQUIRED_ARGUMENT, NULL, 'r'},
    {"pool", REQUIRED_ARGUMENT, NULL, 'P'},
    {"archive", REQUIRED_ARGUMENT, NULL, 'a'},
    {"archiveAll", NO_ARGUMENT, NULL, 'A'},
    {"details", REQUIRED_ARGUMENT, 0, 'x'},
    {"help", NO_ARGUMENT,NULL, 'h' },
    {"retryMax", REQUIRED_ARGUMENT, 0, 'm'},
    {NULL, 0, NULL, 0}
  };

  
  Coptind = 1;
  Copterr = 0;
  char c;

  while ((c = Cgetopt_long(argc, argv, (char*)cmdParams, longopts, NULL)) != -1) {
    switch (c) {
    case 'h':
      help();
      exit(0);
      break;
    case 'V':
      cp.vid = Coptarg; // store it for later use in building Request
      cp.command = REPACK;
      break;
    case 'P':
      cp.pool = Coptarg; // store it for later use in building Request
      cp.command = REPACK;
      break;
    case 'R':
      cp.vid = Coptarg;
      cp.command = REMOVE_TAPE;
      break;
    case 'x' :
      cp.vid = Coptarg;
      cp.command = GET_STATUS;
      return true;
    case 'S':
      cp.stager = Coptarg;
      break;
    case 's':
      cp.command = GET_STATUS_ALL;
      return true;
    case 'r':
      cp.vid = Coptarg;
      cp.command = RESTART;
      return true;
    case 'o':
      cp.serviceclass = Coptarg;
      break;
    case 'a':
      cp.command = ARCHIVE;
      cp.vid = Coptarg;
      return true;
      break;
   case 'A':
      cp.command = ARCHIVE_ALL;
      return true;
      break;
   case 'm':
      cp.retryMax=atoi(Coptarg);
      return true;
      break;
    }
  }
  if ( (cp.command == GET_STATUS || cp.command == ARCHIVE) && cp.vid == NULL ) return false; 
  return ( cp.pool != NULL || cp.vid != NULL );
}




//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void RepackClient::usage() 
{
	std::cout << " Usage:  \n ------  \n repack -V VID1[:VID2:..] | -P PoolID  [-o serviceclass] [-S stager_hostname]" << "   (to start a repack request)"<< std::endl
                  << " repack -s " 
                  << "   (to have the global status of the all repack sequests not archived)" << std::endl
                  << " repack -x VID " 
                  << "   (to have dettailed status of a specific repack sequest not archivied)" << std::endl   	
		  << " repack -R VID1[:VID2:..] " 
                  << "   (to kill a repack request)" << std::endl
		  << " repack -r VID1[:VID2:..] " 
		  << "   (to restart a failed or finished repack subrequest)" << std::endl
		  << " repack -a VID1 " 
                  << "   (to archive the finished  tapes with the vid  specified)" << std::endl
		  << " repack -A " 
                  << "   (to archive all  tapes finished)" << std::endl
		  << " repack -h " 
		  << "   (to have more information about the client)" << std::endl<< std::endl
	          << " repack -m num " 
		  << "   (to set the number of retry in case of failure of the request (default is one))" << std::endl<< std::endl;
}


//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void RepackClient::help(){
	std::cout << "\n *** The RepackClient *** \n" << std::endl
	<< " This client sends a request to the repack server with tapes or one pool to be repacked. "<< std::endl 
	<< " Several tapes can be added by sperating them by ':'.It is also possible to repack a tape pool."<< std::endl 
	<< " Here, only the name of one tape pool is allowed and a output ." << std::endl;

	std::cout << " The hostname and port can be changed in your castor " << std::endl 
	<< " config file or by defining them in your enviroment." << std::endl << std::endl 
	<< " * The enviroment variables are: "<< std::endl 
        << "   REPACK_HOST or REPACK_HOST_ALT "<< std::endl
	<< "   REPACK_PORT or REPACK_PORT_ALT "<< std::endl  << std::endl
	<< " * The castor config file :" << std::endl
        << "   REPACK HOST <hostname> "<< std::endl
	<< "   REPACK PORT <port number> "<< std::endl << std::endl
	<< " If the enviroment has no vaild entries, the castor configuration file" << std::endl
	<< " is read." << std::endl << std::endl;
		
        usage();
}



//------------------------------------------------------------------------------
// addTapes
//------------------------------------------------------------------------------
int RepackClient::addTapes(RepackRequest *rreq)
{
	char* vid;
  /* add the given tapes as SubRequests */
	if ( cp.vid != NULL ) {
	  for (vid = strtok (cp.vid, ":"); vid;  vid = strtok (NULL, ":")) {
	  	RepackSubRequest *sreq = new RepackSubRequest();
	  	sreq->setVid(vid);
	  	sreq->setRepackrequest(rreq);
	  	rreq->addRepacksubrequest(sreq);
	  }
	  return 0;
	}	
	return -1;
}



//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::repack::RepackRequest* RepackClient::buildRequest() throw ()
{
  char cName[CA_MAXHOSTNAMELEN];
  struct passwd *pw = Cgetpwuid(getuid());
  
  /** Get Remote Host and Port Number from environment or from castor config 
    *  file
    */
  try {
    setRemotePort();
    setRemoteHost();
  }catch (castor::exception::Exception ex){
    std::cerr << ex.getMessage().str() << " Aborting.." << std::endl;
    return NULL;
  }
  
  if ( gethostname(cName, CA_MAXHOSTNAMELEN) != 0 ){
  	std::cerr << "Cannot get own hostname !" << std::endl << " Aborting.." << std::endl;
  	return NULL;
  }
  
  /// be sure to repack to the default service class tape pool
  if ( cp.command == REPACK && cp.serviceclass == NULL ){
    std::string answer;
    std::cout << "Do you want to repack to the tape pool specified in default repack service class? [ y\\n ] :";
    std::cin >> answer;
    if ( answer.compare("n") == 0 )
      return NULL;
  }
  

  RepackRequest* rreq = new RepackRequest();
  addTapes(rreq);
  rreq->setCommand(cp.command);
  if ( cp.stager ) rreq->setStager(cp.stager);
  
  /* or, we want to repack a pool */
  if ( cp.pool != NULL ) {
  	if ( !rreq->repacksubrequest().size() )
       rreq->setPool(cp.pool);
    else
    {
    	std::cerr << "You must specify either a pool name or one or more volumes." 
    			  << std::endl;
    	usage();
    	delete rreq;
    	return NULL;  	
     }
  }
  
  rreq->setPid(getpid());
  rreq->setUserName(pw->pw_name);
  rreq->setUid((u_signed64)pw->pw_uid);
  rreq->setGid((u_signed64)pw->pw_gid);
  rreq->setCreationTime(time(NULL));
  rreq->setMachine(cName);
  rreq->setRetryMax(cp.retryMax != -1 ? cp.retryMax : 1);
  
  if ( cp.serviceclass != NULL ) rreq->setSvcclass(cp.serviceclass);
  return rreq;
  
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void RepackClient::run(int argc, char** argv) 
{

  try {
    // parses the command line
    if (!parseInput(argc, argv)) {
      usage();
      return;
    }
    // builds a request and prints it
    castor::repack::RepackRequest* req = buildRequest();
    
    if ( req == NULL ){
    	return;
    }
   
    //req->print();

    // creates a socket
    castor::io::ClientSocket s(m_defaultport, m_defaulthost);
    s.connect();
    // sends the request
    s.sendObject(*req);
    castor::IObject* obj = s.readObject();
	
	RepackAck* ack = dynamic_cast<castor::repack::RepackAck*>(obj);
	if ( ack != 0 ){
		handleResponse(ack);
	}
    // free memory
    delete req;
    delete ack;
  } catch (castor::exception::Exception ex) {
    	std::cerr << ex.getMessage().str() << std::endl;
  }
}




//------------------------------------------------------------------------------
// handleResponse
//------------------------------------------------------------------------------
void RepackClient::handleResponse(RepackAck* ack) {
	
  time_t creation_time = 0;
  time_t submit_time = 0;

  if ( ack->errorCode() ){
    std::cerr << "Repackserver respond :" << std::endl
              << ack->errorMessage() << std::endl;
    return;
  }

  if ( ack->repackrequest().size() > 0 ){
    RepackRequest* rreq = ack->repackrequest().at(0);
    std::cout << "===============================================================================================================" 
              << std::endl;

    std::vector<RepackSubRequest*>::iterator sreq;

    char* nsStr;
  
    if ( !(nsStr = getconfent("CNS", "HOST",0)) && !(nsStr = getenv("CNS_HOST")) ){
	   nsStr=strdup("castorns");
    }  

    std::string nameServer(nsStr);
    // if (nsStr != NULL) free(nsStr);

    FileListHelper m_filehelper(nameServer);

    switch ( rreq->command() ){
      case GET_STATUS :
       creation_time = (long)rreq->creationTime();
       submit_time = (long)rreq->repacksubrequest().at(0)->submitTime();
       struct passwd *pw;
       pw = Cgetpwuid((uid_t)rreq->uid());
       std::cout << 
        "Details for Request created on " << ctime (&creation_time) <<  std::endl ;

        if ( submit_time )
	  std::cout << "=> submitted : " << ctime (&submit_time);
        else
          std::cout << "=> not yet submitted";
        std::cout << std::endl << std::endl <<
        std::setw(30) << std::left << "machine" << 
        std::setw(10) << std::left << "user" <<
        std::setw(20) << std::left << "service class" << 
        std::setw(15) << std::left << "stager" 
	<< std::endl <<
        std::setw(30) << std::left << rreq->machine() <<
        std::setw(10) << std::left <<  pw->pw_name << 
        std::setw(20) << std::left << rreq->svcclass()
        << std::left << rreq->stager()
        << std::endl << std::endl;
        sreq = rreq->repacksubrequest().begin();

        while ( sreq != rreq->repacksubrequest().end() ){
          std::cout << "===============================================================================================================" << std::endl;
          printTapeDetail((*sreq));
          std::cout << "===============================================================================================================" << std::endl;
          std::cout << "\n   ***   Details file by file   ***\n" << std::endl;

	  // Query the name server to retrieve more information related with the situation of segment 

	  std::vector<RepackSegment*>::iterator segs = (*sreq)->repacksegment().begin();
          if (segs == (*sreq)->repacksegment().end())
	     std::cout << "          No File found. \n" << std::endl;
	  while ( segs != (*sreq)->repacksegment().end() ) {
	    m_filehelper.printFileInfo((*segs)->fileid(),(*segs)->copyno());
	      std::cout << "===============================================================================================================" << std::endl;
	    segs++;
	  }
	  sreq++;
	}
	break;
 
      case GET_STATUS_ALL: 
      case ARCHIVE: 
      case ARCHIVE_ALL:
      case REPACK: 
      {
        std::cout << "vid\tcuuid\t\t\t\t\ttotal  size     staging  migration  failed  staged   status" <<std::endl;
        std::cout << "--------------------------------------------------------------------------------------------------------------"
                  << std::endl;
        std::vector<RepackSubRequest*>::iterator tape = rreq->repacksubrequest().begin();
        while ( tape != rreq->repacksubrequest().end() ){
          printTapeDetail((*tape));
          tape++;
        }
	std::cout << "===============================================================================================================" << std::endl;
        break;
      }
    }

  }

}


//------------------------------------------------------------------------------
// printTapeDetail()
//------------------------------------------------------------------------------
void RepackClient::printTapeDetail(RepackSubRequest *tape){
  char buf[21];
  std::map<int,std::string> statuslist;
  statuslist[SUBREQUEST_READYFORSTAGING] = "START";
  statuslist[SUBREQUEST_STAGING] = "STAGING";
  statuslist[SUBREQUEST_MIGRATING] = "MIGRATING";
  statuslist[SUBREQUEST_READYFORCLEANUP] = "CLEANUP";
  statuslist[SUBREQUEST_DONE] = "FINISHED";
  statuslist[SUBREQUEST_ARCHIVED] = "ARCHIVED";
  statuslist[SUBREQUEST_TOBESTAGED] = "TOBESTAGED";
  statuslist[SUBREQUEST_RESTART] = "RESTART";
  statuslist[SUBREQUEST_FAILED] = "FAILED";
   
  u64tostru(tape->xsize(), buf, 0);

  std::cout << tape->vid() << 
      "\t" << tape->cuuid() <<
      "\t" << std::setw(7)<< std::left << tape->files() <<
      std::setw(9) << std::left << buf <<
      std::setw(9) << std::left << tape->filesStaging() <<
      std::setw(11) << std::left << tape->filesMigrating() <<
      std::setw(9) << std::left << tape->filesFailed() <<
      std::setw(8) << std::left << tape->filesStaged() <<
      std::setw(9) << statuslist[tape->status()] <<
      std::endl;
}


//------------------------------------------------------------------------------
// setRemotePort()
//------------------------------------------------------------------------------
void RepackClient::setRemotePort()
  throw (castor::exception::Exception) {
  char* port;
  // Repack server port. Can be given through the environment
  // variable REPACK_PORT or in the castor.conf file as a
  // REPACK_PORT entry. If none is given, default is used
  if ((port = getenv (PORT_ENV)) != 0 
      || (port = getenv ((char*)PORT_ENV_ALT)) != 0 ) {
    char* dp = port;
    errno = 0;
    int iport = strtoul(port, &dp, 0);
    if (*dp != 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Bad port value." << std::endl;
      throw e;
    }
    if (iport > 65535) {
      castor::exception::Exception e(errno);
      e.getMessage()
        << "Invalid port value : " << iport
        << ". Must be < 65535." << std::endl;
      throw e;
    }
    m_defaultport = iport;
  }
  
  stage_trace(3, "Setting up Server Port - Using %d", m_defaultport);
}

//------------------------------------------------------------------------------
// setRhHost
//------------------------------------------------------------------------------
void RepackClient::setRemoteHost()
  throw (castor::exception::Exception) {
  // RH server host. Can be passed given through the
  // RH_HOST environment variable or in the castor.conf
  // file as a RH/HOST entry
  char* host;
  if ((host = getenv (HOST_ENV)) != 0
      || (host = getenv (HOST_ENV_ALT)) != 0
      || (host = getconfent((char*)CATEGORY_CONF,(char *)HOST_CONF,0)) != 0) {
    m_defaulthost = host;
  } else {
    castor::exception::Exception e(serrno);
      e.getMessage()
        << "No repack server hostname in config file or in enviroment found" << std::endl;
      throw e;
  }
  stage_trace(3, "Looking up Server Host - Using %s", m_defaulthost.c_str());
}




      }  //end of namespace repack

}     //end of namespace castor
