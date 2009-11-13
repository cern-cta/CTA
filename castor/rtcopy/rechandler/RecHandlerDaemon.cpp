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
* @(#)$RCSfile: RecHandlerDaemon.cpp,v $ $Author: waldron $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/

// Include Files
#include "castor/rtcopy/rechandler/RecHandlerThread.hpp"
#include "castor/rtcopy/rechandler/RecHandlerDaemon.hpp"

#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"

#include "castor/server/SignalThreadPool.hpp"

#include "castor/rtcopy/rechandler/DlfCodes.hpp"
#include "castor/rtcopy/rechandler/IRecHandlerSvc.hpp"
#include "castor/Services.hpp"

#include <Cgetopt.h>
#include <u64subr.h>
#include <string>
#include <memory>


// -----------------------------------------------------------------------
// External C function used for getting configuration from castor.conf file
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
  // just to check that the configuration is correct

  castor::IService* orasvc = castor::BaseObject::services()->service("OraRecHandlerSvc", castor::SVC_ORARECHANDLERSVC);
  castor::rtcopy::rechandler::IRecHandlerSvc* myService = dynamic_cast<castor::rtcopy::rechandler::IRecHandlerSvc*>(orasvc);
  if (0 == myService) {
    // we don't have DLF yet, and this is a major fault, so log to stderr and exit
    std::cerr << "Couldn't load the policy service, check the castor.conf for DynamicLib entries" << std::endl;
    return -1;
  }

  castor::infoPolicy::RecallPySvc* recallPySvc=NULL;

  try {

    char* pr=NULL;
    std::string recallPolicyName;
    std::string recallFunctionName;
    
    // new BaseDaemon as Server

    castor::rtcopy::rechandler::RecHandlerDaemon  recHandler;

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, castor::rtcopy::rechandler::DAEMON_START, 0,NULL);

    recHandler.parseCommandLine(argc,argv);

    // svc class and time to be executed

    u_signed64 sleepTime=recHandler.timeSleep();

    // get policy information

    if ( (pr = getconfent("Policy","Recall",0)) != NULL ){
      recallPolicyName=pr;
    } else {
        castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No policy for recall in castor.conf")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::rtcopy::rechandler::NO_POLICY, 1, params);
    }

    if ( (pr = getconfent("Policy","RecallFunction",0)) != NULL ){
      recallFunctionName=pr;
    } else {
        castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No global function name for recall policy for recall in castor.conf")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::rtcopy::rechandler::NO_POLICY , 1, params);
    }

    if ( !recallPolicyName.empty() && !recallFunctionName.empty() ){
	recallPySvc= new castor::infoPolicy::RecallPySvc(recallPolicyName,recallFunctionName);
    } else {
      std::cerr << "Couldn't find a valid recall function, check the castor.conf" << std::endl;
      return -1;
    }
    
    std::auto_ptr<castor::rtcopy::rechandler::RecHandlerThread> recThread(new castor::rtcopy::rechandler::RecHandlerThread (recallPySvc));
    std::auto_ptr<castor::server::SignalThreadPool> recPool(new castor::server::SignalThreadPool("RecHandlerThread", recThread.release(), sleepTime));
    recHandler.addThreadPool(recPool.release());

    recHandler.getThreadPool('R')->setNbThreads(1);
    recHandler.start();

  }// end try block
     catch (castor::exception::Exception e) {
       std::cerr << "Caught Fatal exception!\n" << e.getMessage().str() << std::endl;

    // "Exception caught problem to start the daemon"
    castor::dlf::Param params[] =
	  {castor::dlf::Param("Standard Message", sstrerror(e.code())),
    castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, castor::rtcopy::rechandler::FATAL_ERROR, 2, params);
    return -1;
  }
  catch (...) {

    std::cerr << "Caught general exception!" << std::endl;
    castor::dlf::Param params2[] =
	  {castor::dlf::Param("Standard Message", "Caught general exception in cleaning daemon.")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, castor::rtcopy::rechandler::FATAL_ERROR, 1, params2);
    return -1;

  }
  
  if (recallPySvc) delete recallPySvc;

  return 0;
}

//------------------------------------------------------------------------------
// RecHandlerDaemon Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------

castor::rtcopy::rechandler::RecHandlerDaemon::RecHandlerDaemon() : castor::server::BaseDaemon("rechandlerd")
{

  m_timeSleep=SLEEP_TIME;      // seconds

  // Initializes the DLF logging. This includes
  // registration of the predefined messages

  castor::dlf::Message messages[] =
   {

     { DAEMON_START, "Starting RecHandler Daemon done"},
     { DAEMON_STOP, "Stopped  RecHandler Daemon done"},
     { NO_POLICY , "Wrong policy configuration"},
     { FATAL_ERROR, "Fatal Error"},
     { TAPE_NOT_FOUND, "No tape found"}, 
     { TAPE_FOUND , "Tape found"},
     { POLICY_INPUT, "Input given to the policy"},
     { ALLOWED_WITHOUT_POLICY , "Recall allowed without policy"},
     { ALLOWED_BY_POLICY, "Recall allowed by policy/sending vdqm priority"},
     { PRIORITY_SENT, "Priority sent to vdqm"},
     { NOT_ALLOWED, "Recall not allowed"},
     { PYTHON_ERROR, "Python error"},
     { RESURRECT_TAPES, "Result save into db"},
     {-1, ""}};
  dlfInit(messages);
  
}



void castor::rtcopy::rechandler::RecHandlerDaemon::parseCommandLine(int argc, char* argv[]){
  Coptind = 1;
  Copterr = 1;
  int c;
  while ( (c = Cgetopt(argc,argv,"t:fh")) != -1 ) {
    switch (c) {
    case 't':
      m_timeSleep = strutou64(Coptarg);
      break;
    case 'f':
      m_foreground = true;
      break;
    case 'h':
      usage();
      exit(0);
    default:
      usage();
      exit(0);
    }
  }
}

void castor::rtcopy::rechandler::RecHandlerDaemon::usage(){
  std::cout << "\nUsage: " << "RecHandlerDaemon"
            << " [options]\n "
            << "Where options are:\n"
            << "-f     : to run in foreground\n"
            << "-t sleepTime(seconds)  : sleep time (in seconds) between two checks. Default=300\n"
            << "-h : to ask for help\n"
	    <<std::endl;

}
