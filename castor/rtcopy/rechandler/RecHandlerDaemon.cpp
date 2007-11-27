/******************************************************************************
*                      RecHandlerDaemon.cpp
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
* @(#)$RCSfile: RecHandlerDaemon.cpp,v $ $Author: gtaur $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/

// Include Files

#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"

#include "castor/server/SignalThreadPool.hpp"

#include "castor/rtcopy/rechandler/RecHandlerDaemon.hpp"
#include "castor/rtcopy/rechandler/RecHandlerThread.hpp"
#include "castor/infoPolicy/IPolicySvc.hpp"
#include "castor/Services.hpp"
#include "castor/infoPolicy/RecallPySvc.hpp"

#include <Cgetopt.h>
#include <u64subr.h>
#include <string> 


// -----------------------------------------------------------------------
// External C function used for getting configuration from shift.conf file
// -----------------------------------------------------------------------


extern "C" {
  char* getconfent(const char *, const char *, int);
}
 
#define SLEEP_TIME        48   // seconds default if it is not specified 

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------

int main(int argc, char* argv[]) {
  
  // service to access the database

  castor::IService* orasvc = castor::BaseObject::services()->service("OraPolicySvc", castor::SVC_ORAPOLICYSVC);
  castor::infoPolicy::IPolicySvc* myService = dynamic_cast<castor::infoPolicy::IPolicySvc*>(orasvc);
  if (0 == myService) {
    // we don't have DLF yet, and this is a major fault, so log to stderr and exit
    std::cerr << "Couldn't load the policy service, check the castor.conf for DynamicLib entries" << std::endl;
    return -1;
  }
    
  try {

    // get the policy svc

    char* pr=NULL;
    std::string recallPolicyName;
    std::string recallFunctionName;

    if ( (pr = getconfent("Policy","RECALL",1)) != NULL ){ 
      recallPolicyName=pr;
    } else {
        castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No policy for recall in castor.conf")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 8, 1, params);
    }

    if ( (pr = getconfent("Policy","RECALLFUNCTION",1)) != NULL ){ 
      recallFunctionName=pr;
    } else {
        castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No global function name for recall policy for recall in castor.conf")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 8, 1, params);
    }

    castor::infoPolicy::RecallPySvc* recallPySvc=NULL;

      // started the interpreter 
      // this call might raise fatal exceptions if the information in castor.conf are not
      // valid ( be aware that castor.conf parsing is based only on tabs) 
      
      if ( !recallPolicyName.empty() && !recallFunctionName.empty() )
	recallPySvc= new castor::infoPolicy::RecallPySvc(recallPolicyName,recallFunctionName);

    

    // new BaseDaemon as Server 
    
    castor::rtcopy::rechandler::RecHandlerDaemon*  newRecHandler= new castor::rtcopy::rechandler::RecHandlerDaemon();
    newRecHandler->parseCommandLine(argc,argv);

    // svc class and time to be executed

    u_signed64 sleepTime=newRecHandler->timeSleep();
    
    newRecHandler->addThreadPool(
      new castor::server::SignalThreadPool("RecHandlerThread", new castor::rtcopy::rechandler::RecHandlerThread(myService,recallPySvc), sleepTime));
    
    newRecHandler->getThreadPool('R')->setNbThreads(1);
    newRecHandler->start();
    
  }// end try block
     catch (castor::exception::Exception e) {
       std::cerr << "Caught Fatal exception!\n" << e.getMessage().str() << std::endl;

    // "Exception caught problem to start the daemon"
    castor::dlf::Param params[] =
	  {castor::dlf::Param("Standard Message", sstrerror(e.code())),
    castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 2, params);
    return -1;
  }
  catch (...) {
    
    std::cerr << "Caught general exception!" << std::endl;
    castor::dlf::Param params2[] =
	  {castor::dlf::Param("Standard Message", "Caught general exception in cleaning daemon.")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params2);
    return -1;
    
  }
  return 0;
}

//------------------------------------------------------------------------------
// RecHandlerDaemon Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------

castor::rtcopy::rechandler::RecHandlerDaemon::RecHandlerDaemon() : castor::server::BaseDaemon("RecHandlerDaemon") 
{
  
  m_timeSleep=SLEEP_TIME;      // seconds

  // Initializes the DLF logging. This includes
  // registration of the predefined messages

  castor::dlf::Message messages[] =
   {{ 1, "Starting RecHandler Daemon done"},
    { 2, "Stopped  RecHandler Daemon done"},
    { 3, "No tape to check"},
    { 4, "RecHandler policy called"},
    { 5, "Recall allowed"},
    { 6, "No tape to resurrect" },
    { 7, "Fatal Error"},
    { 8, "No Policy file available"},
    { 9, "Error in loading the policy file"},
    { 10, "Error in the executing the policy script"},
    {-1, ""}};
  
 dlfInit(messages);

}



void castor::rtcopy::rechandler::RecHandlerDaemon::parseCommandLine(int argc, char* argv[]){
  if (argc <= 1 ) {
    usage();
    return;
  }
  Coptind = 1;
  Copterr = 1;
  int c;  
  while ( (c = Cgetopt(argc,argv,"t:f")) != -1 ) {
    switch (c) {
    case 't':
      m_timeSleep = strutou64(Coptarg);
      break;
    case 'f':
      m_foreground = true;
      break;
    default:
      usage();
      return;
    }
  }
}

void castor::rtcopy::rechandler::RecHandlerDaemon::usage(){
  std::cout << "\nUsage: " << "RecHandlerDaemon" 
            << " [options]\n "
            << "Where options are:\n" 
            << "-f     : to run in foreground\n"
            << "-t sleepTime(seconds)  : sleep time (in seconds) between two checks. Default=300\n"
	    <<std::endl; 
}
