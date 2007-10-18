/******************************************************************************
 *                castor/stager/daemon/StagerMainDaemon.cpp
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
 * @(#)$RCSfile: StagerDaemon.cpp,v $ $Revision: 1.14 $ $Release$ $Date: 2007/10/18 16:48:52 $ $Author: itglp $
 *
 * Main stager daemon
 *
 * @author castor dev team
 *****************************************************************************/

#include <iostream>
#include <string>
#include <errno.h>
#include <serrno.h>
#include <getconfent.h>

#include "castor/Constants.hpp"
#include "castor/BaseObject.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/BaseDaemon.hpp"
#include "castor/server/SignalThreadPool.hpp"

#include "castor/stager/dbService/StagerMainDaemon.hpp"
#include "castor/stager/dbService/JobRequestSvc.hpp"
#include "castor/stager/dbService/PreRequestSvc.hpp"
#include "castor/stager/dbService/StgRequestSvc.hpp"

// default values
#define STAGER_JOBREQNOTIFYPORT    10001
#define STAGER_PREPREQNOTIFYPORT   10002
#define STAGER_STAGEREQNOTIFYPORT  10003
#define STAGER_QUERYREQNOTIFYPORT  10004
#define STAGER_ERRORNOTIFYPORT     10010
#define STAGER_JOBNOTIFYPORT       10011
#define STAGER_GCNOTIFYPORT        10012


int getConfigPort(const char* configLabel, int defaultValue) {
  char* value;
  int notifyPort = 0;

  if ((value = getconfent("STAGER", (char*)configLabel, 0))) {
    notifyPort = std::strtol(value, 0, 10);
    if (notifyPort == 0) {
      notifyPort = defaultValue;
    } else if (notifyPort > 65535) {
      std::cerr << "Invalid value configured for " << configLabel <<": " 
         << notifyPort << " - must be < 65535" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  return notifyPort;
}

int main(int argc, char* argv[]){
  try{

    castor::stager::dbService::StagerMainDaemon stagerDaemon;

    /*******************************/
    /* thread pools for the stager */
    /*******************************/
    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("JobRequestSvcThread", 
        new castor::stager::dbService::JobRequestSvc(), 
          getConfigPort("JOBREQNOTIFYPORT", STAGER_JOBREQNOTIFYPORT)));
    
    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("PrepRequestSvcThread", 
        new castor::stager::dbService::PreRequestSvc(),
          getConfigPort("PREPREQNOTIFYPORT", STAGER_PREPREQNOTIFYPORT)));


    stagerDaemon.addThreadPool(
      new castor::server::SignalThreadPool("StageRequestSvcThread", 
        new castor::stager::dbService::StgRequestSvc(),
          getConfigPort("STAGEREQNOTIFYPORT", STAGER_STAGEREQNOTIFYPORT)));
     
    //stagerDaemon.addThreadPool(
    //  new castor::server::SignalThreadPool("QueryRequestSvcThread", 
    //    new castor::stager::dbService::QueryRequestSvcThread(),
    //      getConfigPort("QUERYREQNOTIFYPORT", STAGER_QUERYREQNOTIFYPORT)));
     
    //stagerDaemon.addThreadPool(
    //  new castor::server::SignalThreadPool("ErrorSvcThread", 
    //    new castor::stager::dbService::ErrorSvcThread(),
    //      getConfigPort("ERRORNOTIFYPORT", STAGER_ERRORNOTIFYPORT)));

    //stagerDaemon.addThreadPool(
    //  new castor::server::SignalThreadPool("jobSvcThread", 
    //    new castor::stager::dbService::JobSvcThread(),
    //      getConfigPort("JOBNOTIFYPORT", STAGER_JOBNOTIFYPORT)));

    //stagerDaemon.addThreadPool(
    //  new castor::server::SignalThreadPool("GCSvcThread", 
    //    new castor::stager::dbService::GCSvcThread(),
    //      getConfigPort("GCNOTIFYPORT", STAGER_GCNOTIFYPORT)));

    stagerDaemon.getThreadPool('J')->setNbThreads(10);
    stagerDaemon.getThreadPool('P')->setNbThreads(5);
    stagerDaemon.getThreadPool('S')->setNbThreads(5);
    //stagerDaemon.getThreadPool('Q')->setNbThreads(10);
    //stagerDaemon.getThreadPool('E')->setNbThreads(8);
    //stagerDaemon.getThreadPool('j')->setNbThreads(20);
    //stagerDaemon.getThreadPool('G')->setNbThreads(5);
    
    /* we need to call this function before setting the number of threads */
    stagerDaemon.parseCommandLine(argc, argv);

    stagerDaemon.start();  

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception: "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;

    // "Exception caught when starting Stager"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Code", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1, 2, params);
  } catch (...) {
    std::cerr << "Caught general exception!" << std::endl;
  }
  
  return 0;
}// end main


/******************************************************************************************/
/* constructor: initiallizes the DLF logging and set the default value to its attributes */
/****************************************************************************************/
castor::stager::dbService::StagerMainDaemon::StagerMainDaemon() throw(castor::exception::Exception)
  : castor::server::BaseDaemon("Stager") {
	
	castor::dlf::Message stagerMainMessages[]={
	  { 1, "Exception caught when starting Stager"},
	  {-1, ""}
	};
	
  dlfInit(stagerMainMessages);
}

/*************************************************************/
/* help method for the configuration (from the command line) */
/*************************************************************/
void castor::stager::dbService::StagerMainDaemon::help(std::string programName)
{
	  std::cout << "Usage: " << programName << " [options]\n"
	    "\n"
	    "where options can be:\n"
	    "\n"
	    "\t--Pthreads    or -P {integer >= 0}  \tNumber of threads for the Prepare requests\n"
	    "\n"
	    "Comments to: Castor.Support@cern.ch\n";
}



