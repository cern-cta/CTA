/******************************************************************************
*                      MigHunterDaemon.cpp
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
* @(#)$RCSfile: MigHunterDaemon.cpp,v $ $Author: waldron $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/

// Include Files
#include "castor/rtcopy/mighunter/MigHunterThread.hpp"
#include "castor/rtcopy/mighunter/MigHunterDaemon.hpp"
#include "castor/rtcopy/mighunter/StreamThread.hpp"
#include "castor/rtcopy/mighunter/DlfCodes.hpp"

#include <iostream>
#include <string>
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include <Cgetopt.h>
#include <u64subr.h>

#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/rtcopy/mighunter/IMigHunterSvc.hpp"
#include <list>
#include <memory>

extern "C" {
  char* getconfent(const char *, const char *, int);
}


// default values

#define MIN_BYTE_VOLUME   1     // bytes
#define SLEEP_TIME        7200  // seconds (2 hours)


//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------

int main(int argc, char* argv[]){

 // service to access the database
 // now I just check that everything is ok
  
  castor::IService* orasvc = castor::BaseObject::services()->service("OraMigHunterSvc", castor::SVC_ORAMIGHUNTERSVC);
  castor::rtcopy::mighunter::IMigHunterSvc* mySvc = dynamic_cast<castor::rtcopy::mighunter::IMigHunterSvc*>(orasvc);


  if (0 == mySvc) {
    // we don't have DLF yet, and this is a major fault, so log to stderr and exit
    std::cerr << "Couldn't load the policy  service, check the castor.conf for DynamicLib entries" << std::endl;
    return -1;
  }
  
  // new BaseDaemon as Server

  castor::rtcopy::mighunter::MigHunterDaemon migHunter; //dlf available now

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, castor::rtcopy::mighunter::DAEMON_START, 0,NULL);
  
  
  castor::infoPolicy::MigrationPySvc* migrSvc = NULL;
  castor::infoPolicy::StreamPySvc* strSvc = NULL;

  
  // create the policy
  try{
  
    
    // get migration policy name

    char* pm = getconfent("Policy","Migration",0);
   
    
    if (pm==NULL) {

       castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No policy for migration in castor.conf")};
       castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, castor::rtcopy::mighunter::NO_POLICY, 1, params);

       
    } else {

      migrSvc = new castor::infoPolicy::MigrationPySvc(pm); 

    }

    // get stream policy name

    char* ps = getconfent("Policy","Stream",0);
   
    
    if (ps == NULL){
      castor::dlf::Param params[] =
	{castor::dlf::Param("message","No policy for stream in castor.conf")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, castor::rtcopy::mighunter::NO_POLICY, 1, params);

    } else {
      strSvc = new castor::infoPolicy::StreamPySvc(ps);
      
    };
 
    
    migHunter.parseCommandLine(argc, argv);

    u_signed64 minByteVolume=migHunter.byteVolume();
    u_signed64 sleepTime=migHunter.timeSleep();
    bool doClone=migHunter.doClone();

    std::list<std::string>::const_iterator svcClassName = migHunter.listSvcClass().begin(); 
    
    // clean up the database
    while (svcClassName != migHunter.listSvcClass().end()){
          mySvc->migHunterCleanUp(*svcClassName);
      svcClassName++;
    }

    // create thread pools
    // mighunter
    
    std::auto_ptr<castor::rtcopy::mighunter::MigHunterThread> migThread(new castor::rtcopy::mighunter::MigHunterThread(migHunter.listSvcClass(),minByteVolume,doClone,migrSvc));
    std::auto_ptr<castor::server::SignalThreadPool> migPool(new castor::server::SignalThreadPool("MigHunterThread",migThread.release(),sleepTime));
    migHunter.addThreadPool(migPool.release());
    migHunter.getThreadPool('M')->setNbThreads(1);
    
    //stream

    std::auto_ptr<castor::rtcopy::mighunter::StreamThread> strThread( new castor::rtcopy::mighunter::StreamThread( migHunter.listSvcClass(), strSvc));
    std::auto_ptr<castor::server::SignalThreadPool> strPool(new castor::server::SignalThreadPool("StreamThread",strThread.release(),sleepTime));
    migHunter.addThreadPool(strPool.release());
    migHunter.getThreadPool('S')->setNbThreads(1);
    
    
    migHunter.start();

    

  }// end try block
  catch (castor::exception::Exception e) {
    std::cerr << "Caught castor exception : "
     << sstrerror(e.code()) << std::endl
     << e.getMessage().str() << std::endl;

    castor::dlf::Param params0[] =
      {castor::dlf::Param("errorCode",e.code()),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
       castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::rtcopy::mighunter::FATAL_ERROR, 2, params0);
    return -1;
  }
  catch (...) {

    std::cerr << "Caught general exception!" << std::endl;
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::rtcopy::mighunter::FATAL_ERROR, 0, NULL);
    return -1;

  }

  
  if (migrSvc) delete migrSvc;
  if (strSvc) delete strSvc;
  return 0;
}

//------------------------------------------------------------------------------
// MigHunterDaemon Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------

castor::rtcopy::mighunter::MigHunterDaemon::MigHunterDaemon() : castor::server::BaseDaemon("mighunterd")
{
    m_timeSleep= SLEEP_TIME;
    m_byteVolume= MIN_BYTE_VOLUME;
    m_doClone=false;

  // Initializes the DLF logging. This includes
  // registration of the predefined messages
  // Initializes the DLF logging. This includes
  // defining the predefined messages

  castor::dlf::Message messages[] =
  {

    {DAEMON_START, "Service shutdown"} ,
    {DAEMON_STOP, "Service shutdown"} ,
    {FATAL_ERROR, "Fatal Error"}, 
    {NO_POLICY, "No Policy file available"},
    {PARSING_OPTIONS,"parameters option parsed"},
    {NO_TAPECOPIES, "No tapecopy found"},
    {TAPECOPIES_FOUND, "No migration candidate found"},
    {NO_TAPEPOOLS, "No tapepool found"} , 
    {NOT_ENOUGH, "not enough data to create streams"},
    {NO_DRIVES, "no drive assigned to this service class"}, 
    {POLICY_INPUT ,"input to call the migration policy"}, 
    {ALLOWED_WITHOUT_POLICY,"allowed without policy"},
    {ALLOWED_BY_POLICY ,"allowed by policy"}, 
    {NOT_ALLOWED , "not allowed"},
    {POLICY_RESULT ,"summary of migration policy results"},
    {ATTACHED_TAPECOPIES , "attaching tapecopies to streams"},
    {DB_ERROR , "db error"},
    {RESURRECT_TAPECOPIES, "resurrecting tapecopies" },
    {INVALIDATE_TAPECOPIES, "invalidating tapecopies"},
    {NS_ERROR,"Error retrieving the file stat from the nameserver"},
    {NO_STREAM, "No stream found"},
    {STREAMS_FOUND, "Streams found"},
    {STREAM_INPUT , "input to call stream policy"},
    {START_WITHOUT_POLICY, "started without policy"},
    {START_BY_POLICY, "started by policy"}, 
    {NOT_STARTED , "stopped"},
    {STREAM_POLICY_RESULT ,"summary of stream policy results"},
    {STARTED_STREAMS, "stream started"},
    {STOP_STREAMS, "stream stopped"}, 
    {-1, ""}
  };
  dlfInit(messages);
}


void castor::rtcopy::mighunter::MigHunterDaemon::parseCommandLine(int argc, char* argv[]){
  if (argc < 1 ) {
    usage();
    return;
  }
  Coptind = 1;
  Copterr = 1;
  int c; // ????

  std::string optionStr="Option used: ";

  while ( (c = Cgetopt(argc,argv,"Ct:v:fh")) != -1 ) {
    switch (c) {
    case 'C':
      optionStr+=" -C ";
      m_doClone = true;
      break;
    case 't':
      optionStr+=" -t ";
      optionStr+=Coptarg;
      m_timeSleep = strutou64(Coptarg);
      break;
    case 'v':
      optionStr+=" -v ";
      optionStr+=Coptarg;
      m_byteVolume = strutou64(Coptarg);
      break;
    case 'f':
      optionStr+=" -f ";
      m_foreground = true;
      break;
    case 'h':
      optionStr+=" -h ";
      usage();
      exit(0);
    default:
      usage();
      exit(0);
    }
  }

  std::string svcClassesStr="Used the following Svc Classes: ";

  for (int i=Coptind; i<argc; i++ ) {
   m_listSvcClass.push_back(argv[i]);
   svcClassesStr+=" ";
   svcClassesStr+=argv[i];
   svcClassesStr+=" ";
  }
  if (m_listSvcClass.empty()) {
    m_listSvcClass.push_back("default");
    svcClassesStr+=" default ";
  }

  castor::dlf::Param params[] = {castor::dlf::Param("message", optionStr.c_str()),
				 castor::dlf::Param("message", svcClassesStr.c_str())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, PARSING_OPTIONS, 2, params);

}

void castor::rtcopy::mighunter::MigHunterDaemon::usage(){
  std::cout << "\nUsage: " << "MigHunterDaemon"
            << "[options] svcClass1 svcClass2 svcClass3 ...\n"
            << "-C  : clone tapecopies from existing to new streams (very slow!)\n"
            << "-f     : to run in foreground\n"
            << "-t sleepTime(seconds)  : sleep time (in seconds) between two checks. Default= 7200 \n"
            << "-v volume(bytes)       : data volume threshold below migration will not start\n"<<std::endl;
}
