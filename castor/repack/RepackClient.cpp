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
 * @(#)$RCSfile: RepackClient.cpp,v $ $Revision: 1.48 $ $Release$ $Date: 2009/07/23 12:21:58 $ $Author: waldron $
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
 * @author Giulia Taurelli
 *****************************************************************************/


/* Client  includes */
#include "castor/repack/RepackClient.hpp"
#include "castor/repack/FileListHelper.hpp"
#include <time.h>
#include <ios>
#include <algorithm>
#include <string>
#include <vector>
#include <iostream>
#include <iomanip>

/**
 * By including the Header file, the Factory is automatically active !!
 */
#include "castor/io/StreamRepackRequestCnv.hpp"
#include "castor/io/StreamRepackSubRequestCnv.hpp"
#include "castor/io/StreamRepackAckCnv.hpp"
#include "castor/repack/RepackAck.hpp"
#include "castor/repack/RepackSubRequestStatusCode.hpp"
#include "castor/repack/RepackResponse.hpp"
#include "RepackUtility.hpp"

#define CSP_REPACKSERVER_PORT 62800

void printTime(time_t* rawtime){
  if (!rawtime || *rawtime==0){
    std::cout<<"    No_Time       ";
    return;
  }
  tm * timeptr = localtime(rawtime);
  std::string mon_name[12];
  mon_name[0]="Jan";mon_name[1]="Feb"; mon_name[2]="Mar";
  mon_name[3]= "Apr"; mon_name[4]= "May"; mon_name[5]= "Jun";
  mon_name[6]= "Jul"; mon_name[7]="Aug"; mon_name[8]="Sep";
  mon_name[9]=  "Oct"; mon_name[10]=  "Nov"; mon_name[11]= "Dec";
  if (timeptr->tm_mday < 10) std::cout<<"0";
  if ((timeptr->tm_year)%100 < 10) {
    std::cout<< (timeptr->tm_mday)<<"-"<<mon_name[timeptr->tm_mon]<<"-0"<< (timeptr->tm_year)%100<<"_";
  } else {
    std::cout<< (timeptr->tm_mday)<<"-"<<mon_name[timeptr->tm_mon]<<"-"<< (timeptr->tm_year)%100<<"_";
  }
  if (timeptr->tm_hour <10) std::cout<<"0";
  std::cout << timeptr->tm_hour <<":";
  if (timeptr->tm_min <10) std::cout<<"0";
  std::cout<<timeptr->tm_min<<"   ";

}


class sortAnswers{
public:
  bool operator()(castor::repack::RepackResponse* resp1, castor::repack::RepackResponse* resp2) const{
    if (resp1 == NULL && resp2 != NULL) return false;
    if (resp1 != NULL && resp2 == NULL) return true;
    if (resp1->errorCode() > resp2->errorCode()) return true;
    return false;
  };
};


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
      exit(1);
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
  /* the default repack server host */
  m_defaulthost = "localhost";
  std::string clientname = "RepackClient";

  cp.vid =  NULL;
  cp.pool = NULL;
  cp.serviceclass = NULL;
  cp.stager = NULL ;
  cp.retryMax = 0;
  cp.reclaim =0;
  cp.finalPool= NULL;
  svc = svcs()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
  if (0 == svc) {
      // "Could not get Conversion Service for Streaming" message
	std::cout << "Could not get Conversion Service for Streaming" << std::endl;
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
  const char* cmdParams = "o:aA:S:sR:V:P:r:hx:m:cn:e:";
  if (argc == 1){
    return false;
  }

  if (getuid() == 0){
    std::cout <<"You cannot use repack commands if you are root."<< std::endl;
    exit(1);
  }

  struct Coptions longopts[] = {
    {"output_svcclass", REQUIRED_ARGUMENT, 0, 'o'},
    {"status", REQUIRED_ARGUMENT, 0, 'S' },
    {"statusAll", NO_ARGUMENT,NULL, 's' },
    {"delete", REQUIRED_ARGUMENT,NULL, 'R' },
    {"volumeid", REQUIRED_ARGUMENT, NULL, 'V'},
    {"restart", REQUIRED_ARGUMENT, NULL, 'r'},
    {"pool", REQUIRED_ARGUMENT, NULL, 'P'},
    {"archive", REQUIRED_ARGUMENT, NULL, 'A'},
    {"archiveAll", NO_ARGUMENT, NULL, 'a'},
    {"details", REQUIRED_ARGUMENT, 0, 'x'},
    {"help", NO_ARGUMENT,NULL, 'h' },
    {"retryMax", REQUIRED_ARGUMENT, 0, 'm'},
    {"reclaim", NO_ARGUMENT, NULL, 'c'},
    {"newPool",REQUIRED_ARGUMENT, NULL,'n'},
    {"errors", NO_ARGUMENT, NULL, 'e'},
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
      cp.command = REMOVE;
      if (cp.vid == NULL) return false;
      else return true;
    case 'x' :
      cp.vid = Coptarg;
      cp.command = GET_NS_STATUS;
      if (cp.vid == NULL) return false;
      else return true;
    case 's':
      cp.command = GET_STATUS_ALL;
      return true;
    case 'S':
      cp.command = GET_STATUS;
      cp.vid = Coptarg;
      if (cp.vid == NULL) return false;
      else return true;
    case 'r':
      cp.vid = Coptarg;
      cp.command = RESTART;
      if (cp.vid == NULL) return false;
      else return true;
    case 'o':
      cp.serviceclass = Coptarg;
      break;
    case 'A':
      cp.command = ARCHIVE;
      cp.vid = Coptarg;
      if (cp.vid == NULL) return false;
      else return true;
   case 'a':
      cp.command = ARCHIVE_ALL;
      return true;
   case 'm':
      cp.retryMax=atoi(Coptarg);
      if (cp.retryMax < 0) cp.retryMax=0;
      break;
    case 'c':
      cp.reclaim=1;
      break;
    case 'n':
      cp.finalPool=Coptarg;
      break;
   case 'e':
      cp.command = GET_ERROR;
      cp.vid = Coptarg;
      if  (cp.vid == NULL) return false;
      return true;
    }
  }
  return ( cp.pool != NULL || cp.vid != NULL );

}




//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void RepackClient::usage()
{
	std::cout << " Usage:  \n ------  \n repack -V VID1[:VID2:..] | -P PoolID [-o serviceclass] [-m num] [-c] [-n finalPool" << " to start a repack request (-m to set the number of retry in case of failure of the request. Default is one). If the option -c is given the tapes in the request will be reclaimed. Option -n is used to specified a new tape pool in which the tape should be moved after being repacked."<< std::endl
                  << " repack -s "
                  << "   (to have the global status of the all repack sequests not archived)" << std::endl
                  << " repack -S VID[:VID2:..] "
                  << "   (to have the status of the repack requests not archivedfor that vid)" << std::endl
                  << " repack -x VID[:VID2:..] "
                  << "   (to have the name server information for the files which were in that tape before starting the repack process)" << std::endl
		  << " repack -R VID1[:VID2:..] "
                  << "   (to abort a repack request)" << std::endl
		  << " repack -r VID1[:VID2:..] "
		  << "   (to restart a failed or finished repack subrequest)" << std::endl
		  << " repack -A VID1[:VID2:..] "
                  << "   (to archive the finished  tape specified by VID)" << std::endl
		  << " repack -a "
                  << "   (to archive all the finished tapes)" << std::endl
		  << " repack -e VID1[:VID2:..] "
                  << "  (to know the status and extra information of the failed files which where in  the  specified tape)" << std::endl
		  << " repack -h "
		  << "   (to have more information about the client)" << std::endl<< std::endl;

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
    std::cout << ex.getMessage().str() << " Aborting.." << std::endl;
    return NULL;
  }

  if ( gethostname(cName, CA_MAXHOSTNAMELEN) != 0 ){
  	std::cout << "Cannot get own hostname !" << std::endl << " Aborting.." << std::endl;
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
  rreq->setCommand((castor::repack::RepackCommandCode)cp.command);
  if ( cp.stager ) rreq->setStager(cp.stager);

  /* or, we want to repack a pool */
  if ( cp.pool != NULL ) {
  	if ( !rreq->repacksubrequest().size() )
       rreq->setPool(cp.pool);
    else
    {
    	std::cout << "You must specify either a pool name or one or more volumes."
    			  << std::endl;
    	usage();
    	freeRepackObj(rreq);
    	return NULL;
     }
  }

  rreq->setPid(getpid());
  rreq->setUserName(pw->pw_name);
  rreq->setUserId((u_signed64)pw->pw_uid);
  rreq->setGroupId((u_signed64)pw->pw_gid);
  rreq->setCreationTime(time(NULL));
  rreq->setMachine(cName);
  rreq->setRetryMax(cp.retryMax);
  rreq->setReclaim(cp.reclaim);
  if (cp.finalPool !=NULL) rreq->setFinalPool(cp.finalPool);
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
      exit(1);
    }
    // builds a request and prints it
    castor::repack::RepackRequest* req = buildRequest();

    if ( req == NULL ){
    	exit(1);
    }

    // creates a socket
    castor::io::ClientSocket s(m_defaultport, m_defaulthost);
    s.setTimeout(100000);
    s.connect();
    // sends the request
    s.sendObject(*req);
    castor::IObject* obj = s.readObject();

    RepackAck* ack = dynamic_cast<castor::repack::RepackAck*>(obj);
    if ( ack != 0 ) handleResponse(ack);

    // free memory

    freeRepackObj(req);
    freeRepackObj(ack);

  } catch (castor::exception::Exception ex) {
    	std::cout << ex.getMessage().str() << std::endl;
	exit(1);
  }
}


//------------------------------------------------------------------------------
// handleResponse
//------------------------------------------------------------------------------
void RepackClient::handleResponse(castor::repack::RepackAck* ack) {

  time_t submit_time = 0;

  std::vector<castor::repack::RepackResponse*> resps=ack->repackresponse();
  if (resps.empty()){
    switch (ack->command()){
          case ARCHIVE_ALL:
	    std::cout<<"No tape to archive"<<std::endl;
	    break;
          case GET_STATUS_ALL:
	    std::cout<<"No tape found"<<std::endl;
	    break;
    default:
      std::cout << "No Answer from the repack server" << std::endl;
      exit(1);
    }
    return;
  }

  std::sort(resps.begin(), resps.end(), sortAnswers());

  std::vector<RepackResponse*>::iterator resp=resps.begin();
  std::vector<RepackSegment*>::iterator segment;
  std::vector<RepackSegment*> segments;

  char* nsStr;
  if ( !(nsStr = getconfent("CNS", "HOST",0)) && !(nsStr = getenv("CNS_HOST")) ){
    nsStr=strdup("castorns");
  }

  std::string nameServer(nsStr);
  FileListHelper m_filehelper(nameServer);


  while (resp != resps.end()){
    RepackSubRequest* sub=(*resp)->repacksubrequest();
    if ((*resp)->errorCode() !=0){
      if (sub==NULL) {
	// global error
	std::cout << "Repackserver respond :" << std::endl
        << (*resp)->errorMessage() << std::endl;
	exit(1);
      }

      // tape related error

      std::cout<<"Operation for tape "<<sub->vid()
               <<" reported the following error : "
	       <<(*resp)->errorMessage()<<std::endl;
      resp++;
      continue;
    }

    switch ( ack->command() ){
      case GET_NS_STATUS :
	submit_time = sub->submitTime();

          std::cout << "\n   *** Repacking of tape "<<sub->vid()<<" : details file by file ***\n" << std::endl;

	  // Query the name server to retrieve more information related with the situation of segment

	  segments= sub->repacksegment();
          if (segments.empty()){
	     std::cout << "no  file for this tape in Repack database \n" << std::endl;

	  } else {
	    std::cout << "===================================================================================================================" << std::endl;

	    std::cout <<std::setw(15)<<"Fileid"
		      <<std::setw(10)<<"Copyno"
		      <<std::setw(10)<<"Fsec"
		      <<std::setw(18)<<"Segsize"
		      <<std::setw(11)<<"Comp"
		      <<std::setw(10)<<"Vid"
		      <<std::setw(10)<<"Fseq"
		      <<std::setw(17)<<"Blockid"
		      <<std::setw(10)<<"Checksum"<< std::endl;

	    std::cout << "===================================================================================================================" << std::endl;
	    segment=segments.begin();
	    while ( segment != segments.end() ) {
	      m_filehelper.printFileInfo((*segment)->fileid(),(*segment)->copyno());
	      segment++;
	    }
	  }
	  break;

    case REPACK:
    case ARCHIVE:
    case ARCHIVE_ALL:
    case REMOVE:
    case RESTART:
      std::cout<<"Operation succeeded for tape "<<sub->vid()<<std::endl;
      break;

    case GET_STATUS_ALL:
      if (resp == resps.begin()) {
	  // tittle just at the begining
	std::cout << "\n======================================================================================================================="
		    << std::endl;
	std::cout <<std::setw(13)<< "CurrentTime  "<<
	  std::setw(17)<<"SubmitTime  "<<
	  std::setw(12)<<"Vid"<<
	  std::setw(13)<<"Total"<<
	  std::setw(12)<<"Size"<<
	  std::setw(10)<<"toRecall"<<
	  std::setw(10)<<"toMigr"<<
	  std::setw(10)<<"Failed"<<
	  std::setw(10)<<"Migrated"<<
	  std::setw(12)<<"Status" <<std::endl;

	std::cout << "-----------------------------------------------------------------------------------------------------------------------"<< std::endl;
      }

      printTapeDetail(sub);
      break;

    case GET_STATUS:
      if (resp == resps.begin()) {
	std::cout << "\n=======================================================================================================================" << std::endl;

	std::cout <<std::setw(13)<< "CurrentTime  "<<
	  std::setw(17)<<"SubmitTime  "<<
	  std::setw(12)<<"Vid"<<
	  std::setw(13)<<"Total"<<
	  std::setw(12)<<"Size"<<
	  std::setw(10)<<"toRecall"<<
	  std::setw(10)<<"toMigr"<<
	  std::setw(10)<<"Failed"<<
	  std::setw(10)<<"Migrated"<<
	  std::setw(12)<<"Status" <<std::endl;
        std::cout << "-----------------------------------------------------------------------------------------------------------------------"<< std::endl;
      }
      printTapeDetail(sub);
      break;
    case GET_ERROR:
      std::cout<<std::endl<<"     *** Tape  "<< sub->vid() <<"  ***"<<std::endl<<std::endl;
      segments= sub->repacksegment();
      if (segments.empty()){
	std::cout<<"no errors found"<<std::endl<<std::endl;
      } else {
	segment=segments.begin();
	std::cout<<"--------------------------------------------------------------------------------------------"<<std::endl;
	std::cout<<std::setw(15)<<std::right<<"Fileid"
		 <<std::setw(15)<<std::right<<"CopyNo"
		 <<std::setw(15)<<std::right<<"ErrorCode"
		 <<std::setw(32)<<std::right<<"ErrorMessage"
		 <<std::endl;
	std::cout<<"--------------------------------------------------------------------------------------------"<<std::endl;
	while (segment != segments.end()) {
	  printSegmentDetail((*segment));
	  segment++;
	}
	std::cout<<std::endl;
      }
      break;

    } //end switch
    resp++;
  } // end loop for each tape
  std::cout<<std::endl;
}



//------------------------------------------------------------------------------
// printTapeDetail()
//------------------------------------------------------------------------------
void RepackClient::printTapeDetail(RepackSubRequest *tape){
  char buf[21];
  time_t submit_time = 0;
  time_t current_time = time(NULL);
  submit_time = (long)tape->submitTime();
  std::map<int,std::string> statuslist;

  statuslist[RSUBREQUEST_TOBECHECKED] = "START";
  statuslist[RSUBREQUEST_TOBESTAGED] = "TOBESTAGED";
  statuslist[RSUBREQUEST_ONGOING] = "ONGOING";
  statuslist[RSUBREQUEST_TOBECLEANED] = "CLEANUP";
  statuslist[RSUBREQUEST_DONE] = "DONE";
  statuslist[RSUBREQUEST_FAILED] = "FAILED";
  statuslist[RSUBREQUEST_TOBEREMOVED] = "DELETING";
  statuslist[RSUBREQUEST_TOBERESTARTED] = "RESTART";
  statuslist[RSUBREQUEST_ARCHIVED] = "ARCHIVED";
  statuslist[RSUBREQUEST_ONHOLD] = "ONHOLD";

  u64tostru(tape->xsize(), buf, 0);
  printTime(&current_time);
  printTime(&submit_time);
  if (tape->status() == RSUBREQUEST_TOBECHECKED) {
    // not checked the name server yet

    std::cout<<
      std::setw(8)  <<std::right <<tape->vid()<<
      std::setw(11)  <<std::right <<  " N/A"<<
      std::setw(12) << std::right << " N/A" <<
      std::setw(10) << std::right << " N/A" <<
      std::setw(10) << std::right << " N/A" <<
      std::setw(10) << std::right << " N/A"  <<
      std::setw(10) << std::right << " N/A" <<
      std::setw(12) << statuslist[tape->status()] <<
      std::endl;

    return;
  }
  if (tape->status() == RSUBREQUEST_TOBESTAGED || tape->status() == RSUBREQUEST_ONHOLD ) {
    // not sent to the stager  yet

    std::cout<<
      std::setw(8)  <<std::right <<tape->vid() <<
      std::setw(11)  <<std::right <<   tape->files() <<
      std::setw(11) << std::right << buf <<'B'<<
      std::setw(10) << std::right << "N/A" <<
      std::setw(10) << std::right << "N/A" <<
      std::setw(10) << std::right << "N/A"  <<
      std::setw(10) << std::right << "N/A" <<
      std::setw(12) << statuslist[tape->status()] <<
      std::endl;

    return;
  }

  std::cout <<
    std::setw(8) <<std::right <<tape->vid() <<
    std::setw(11) <<std::right <<  tape->files() <<
    std::setw(11) << std::right << buf <<"B"<<
    std::setw(10) << std::right << tape->filesStaging() <<
    std::setw(10) << std::right << tape->filesMigrating() <<
    std::setw(10) << std::right << tape->filesFailed() + tape->filesFailedSubmit()   <<
    std::setw(10) << std::right << tape->filesStaged() <<
    std::setw(12) << statuslist[tape->status()] <<
  std::endl;
}



//------------------------------------------------------------------------------
// printSegmentDetail()
//------------------------------------------------------------------------------
void RepackClient::printSegmentDetail(RepackSegment *segment){
  std::map<int,std::string> statuslist;

  if (segment->errorCode() !=0 ){
    std::cout << std::setw(15) <<std::right<< segment->fileid() <<
      std::setw(15) << std::right<<segment->copyno() <<
      std::setw(15) << std::right <<segment->errorCode() <<"   "<<
      std::setw(50)<<std::left << segment->errorMessage() << std::endl;
  }
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

}




      }  //end of namespace repack

}     //end of namespace castor



