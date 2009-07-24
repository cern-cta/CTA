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
* @(#)$RCSfile: MigHunterDaemon.cpp,v $ $Author: gtaur $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/

// Include Files
#include "castor/rtcopy/mighunter/MigHunterThread.hpp"
#include "castor/rtcopy/mighunter/MigHunterDaemon.hpp"

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
    
  castor::rtcopy::mighunter::MigHunterDaemon newMigHunter; //dlf available now

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 1, 0,NULL);
  
    
  // create the policy
  try{
    char* pm=NULL;
    char* ps=NULL;
    std::string migrationPolicyName;
    std::string streamPolicyName;
    
    // get migration policy name

    if ( (pm = getconfent("Policy","Migration",0)) != NULL ){ 
      migrationPolicyName=pm;
    } else {
       castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No policy for migration in castor.conf")};
       castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 12, 1, params);

    }

    // get stream policy name

    if ( (ps = getconfent("Policy","Stream",0)) != NULL ){ 
      streamPolicyName=ps;
    } else {
      castor::dlf::Param params[] =
	  {castor::dlf::Param("message","No policy for stream  in castor.conf")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 12, 1, params);

    }

    // start interpreter and load the files
    castor::infoPolicy::MigrationPySvc* migrSvc=NULL;
    castor::infoPolicy::StreamPySvc* strSvc=NULL;
    
    //migration
    if (pm != NULL)
	migrSvc = new castor::infoPolicy::MigrationPySvc(migrationPolicyName);

    // stream
    if (ps != NULL )
	strSvc = new  castor::infoPolicy::StreamPySvc(streamPolicyName);
  
    
    newMigHunter.parseCommandLine(argc, argv);
    
    u_signed64 minByteVolume=newMigHunter.byteVolume();
    u_signed64 sleepTime=newMigHunter.timeSleep();
    bool doClone=newMigHunter.doClone();
    std::vector<std::string> listSvcClass=newMigHunter.listSvcClass();


    // clean up the database
    std::vector<std::string>::iterator svcClassName=listSvcClass.begin();
    while(svcClassName != listSvcClass.end()){
      mySvc->migHunterCleanUp(*svcClassName);
      svcClassName++;
    }

    newMigHunter.addThreadPool(
      new castor::server::SignalThreadPool("MigHunterThread", new castor::rtcopy::mighunter::MigHunterThread(listSvcClass,minByteVolume,doClone,migrSvc,strSvc), sleepTime));
    newMigHunter.getThreadPool('M')->setNbThreads(1);
    newMigHunter.start();
  
  }// end try block
  catch (castor::exception::Exception e) {
    std::cerr << "Caught castor exception : "
     << sstrerror(e.code()) << std::endl
     << e.getMessage().str() << std::endl;
         
    castor::dlf::Param params0[] =
      {castor::dlf::Param("errorCode",e.code()),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
       castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 14, 2, params0);
    return -1;
  }
  catch (...) {
    
    std::cerr << "Caught general exception!" << std::endl;  
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 14, 0, NULL);
    return -1;
    
  } 
  return 0;
}

//------------------------------------------------------------------------------
// MigHunterDaemon Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------

castor::rtcopy::mighunter::MigHunterDaemon::MigHunterDaemon() : castor::server::BaseDaemon("Mighunter") 
{
    m_timeSleep= SLEEP_TIME;
    m_byteVolume= MIN_BYTE_VOLUME;
    m_doClone=false;

  // Initializes the DLF logging. This includes
  // registration of the predefined messages
  // Initializes the DLF logging. This includes
  // defining the predefined messages

  castor::dlf::Message messages[] =
  {{1, "Service startup"},
   {2, "Service shutdown"},  
   {3, "No migration candidate found"},
   {4, "Error in creating or updating stream"},
   {5, "Executing the policy for migration"},
   {6,"Error retrieving the file stat from the nameserver"},
   {7, "Summary of migration policy results"},
   {8, "Error in attaching tapecopy to stream"},
   {9, "No eligible stream found for the service class"},
   {10, "Executing stream policy"},
   {11, "Summary of stream policy results"},
   {12, "No Policy file available"},
   {13,"Syntax error in the policy script"},
   {14,"Fatal Error"},
   {15,"Parameters used"},
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
				 castor::dlf::Param("message", svcClassesStr.c_str()),};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 15, 2, params);

}

void castor::rtcopy::mighunter::MigHunterDaemon::usage(){
  std::cout << "\nUsage: " << "MigHunterDaemon" 
            << "[options] svcClass1 svcClass2 svcClass3 ...\n"
            << "-C  : clone tapecopies from existing to new streams (very slow!)\n"
            << "-f     : to run in foreground\n"
            << "-t sleepTime(seconds)  : sleep time (in seconds) between two checks. Default= 7200 \n"
            << "-v volume(bytes)       : data volume threshold below migration will not start\n"<<std::endl; 
}
