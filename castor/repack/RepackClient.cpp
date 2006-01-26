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
 * @(#)$RCSfile: RepackClient.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2006/01/26 13:50:02 $ $Author: felixehm $
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



/** 
 * By including the Header file, the Factory is automatically active !! 
 */
#include "castor/io/StreamRepackRequestCnv.hpp"
#include "castor/io/StreamRepackSubRequestCnv.hpp"





/** The main function 
  * 
  */
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


RepackClient::RepackClient()
{
  /* the default server port */
  m_defaultport = CSP_REPACKSERVER_PORT;
  /* the default repackserver host */
  m_defaulthost = "localhost";
  std::string clientname = "RepackClient";

  cp.vid =  NULL;
  cp.pool = NULL;

  /* DLF Logging */
  castor::BaseObject::initLog(clientname, castor::SVC_STDMSG);
  // Initializes the DLF logging. This includes
  // registration of the predefined messages
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Streaming"},
     {-1, ""}};
  castor::dlf::dlf_init((char *)clientname.c_str(), messages);

  svc = svcs()->cnvService("StreamCnvSvc", castor::SVC_STREAMCNV);
  if (0 == svc) {
      // "Could not get Conversion Service for Streaming" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2);
  }
}


RepackClient::~RepackClient() throw()
{
  svc->release();
}


/** 
  * parses the input and validates the parameters
  * @return bool 
  */
bool RepackClient::parseInput(int argc, char** argv)
{
  const char* cmdParams = "V:h:P"; //m_cmdLineParams.str().c_str();
  if (argc == 1){
    usage(argv[0]);
    return false;
  }
  struct Coptions longopts[] = {
    {"volumeid", REQUIRED_ARGUMENT, NULL, 'v'},
    /*{"library", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
    {"min_free", REQUIRED_ARGUMENT, 0, 'm'},
    {"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
    {"nodelete", NO_ARGUMENT, &nodelete_flag, 1},
    {"output_tppool", REQUIRED_ARGUMENT, 0, 'o'},
    {"otp", REQUIRED_ARGUMENT, 0, 'o'},*/
    {"help", NO_ARGUMENT,NULL, 'h' },
    {0, 0, 0, 0}
  };

  
  Coptind = 1;
  Copterr = 0;
  char c;

  while ((c = Cgetopt_long(argc, argv, (char*)cmdParams, longopts, NULL)) != -1) {
    switch (c) {
    case 'h':
      usage(argv[0]);
      exit(0);
      break;
    case 'V':
      cp.vid = Coptarg; // store it for later use in building Request
      break;
    case 'P':
      cp.pool = Coptarg; // store it for later use in building Request
      break;
    default:
      break;
    }
  }
  
  if ( cp.pool != NULL && cp.vid != NULL )
  	return false;
  
  else
  	return true;
}


void RepackClient::usage(std::string message) throw ()
{
   std::cout << "Usage: "<< message  << " -V [VolumeID] -P [PoolID]" << std::endl;
}

/** Builds the Request, which is send to the repack server.
* The Request includes the Job (Volume ID) and a IClient Object.
* !! Note that the Caller has to delete the Request Object !!
* @return pointer to Request Object
*/
castor::repack::RepackRequest* RepackClient::buildRequest() throw ()
{
  setRemotePort();
  setRemoteHost();
  char* vid;
  struct passwd *pw = Cgetpwuid(getuid());

  RepackRequest* rreq = new RepackRequest();
  
  if ( cp.vid != NULL ) {
	  for (vid = strtok (cp.vid, ":"); vid;  vid = strtok (NULL, ":")) {
	  	RepackSubRequest *sreq = new RepackSubRequest();
	  	sreq->setVid(vid);
	  	rreq->addSubRequest(sreq);
	  }
  }
  if ( cp.pool != NULL )
       rreq->setPool(cp.pool);

  rreq->setPid(getpid());
  rreq->setUserName(pw->pw_name);
  rreq->setCreationTime(time(NULL));
  
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
      return;
    }
    // builds a request and prints it
    castor::repack::RepackRequest* req = buildRequest();
    req->print();

    // creates a socket
    castor::io::ClientSocket s(m_defaultport, m_defaulthost);
    s.connect();
    // sends the request
    s.sendObject(*req);

    // free memory
    delete req;
  } catch (castor::exception::Exception ex) {
    std::cout << ex.getMessage().str() << std::endl;
  }
}

//------------------------------------------------------------------------------
// setRemotePort()
//------------------------------------------------------------------------------
void RepackClient::setRemotePort()
  throw (castor::exception::Exception) {
  char* port;
  // RH server port. Can be given through the environment
  // variable RH_PORT or in the castor.conf file as a
  // RH/PORT entry. If none is given, default is used
  if ((port = getenv (PORT_ENV)) != 0 
      || (port = getenv ((char*)PORT_ENV_ALT)) != 0
      || (port = getconfent((char*)CATEGORY_CONF, (char*)PORT_CONF,0)) != 0) {
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
  } else {
    m_defaultport = m_defaultport;
    clog() << "Contacting server on default port ("
           << m_defaultport << ")." << std::endl;
    
  }
  stage_trace(3, "Setting up Server Port - Using %d", m_defaultport);
  clog() << "Setting up Server Port - Using " << m_defaultport << std::endl;
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
    m_defaulthost = m_defaulthost;
//     castor::exception::Exception e(ETPRM);
//     e.getMessage()
//       << "Unable to deduce the name of the RH server.\n"
//       << "No -h option was given, RH_HOST is not set and "
//       << "your castor.conf file does not contain a RH/HOST entry."
//       << std::endl;
//     throw e;
  }
  stage_trace(3, "Looking up Server Host - Using %s", m_defaulthost.c_str());
  clog() << "Looking up Server Host - Using " << m_defaulthost << std::endl;
}




      }  //end of namespace repack

}     //end of namespace castor
