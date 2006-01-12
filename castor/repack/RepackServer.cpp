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
 * @(#)$RCSfile: RepackServer.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/01/12 14:05:31 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm 
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/Services.hpp"

#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/SignalThreadPool.hpp"

#include "castor/io/ServerSocket.hpp"
#include "castor/BaseObject.hpp"
#include "castor/MsgSvc.hpp"
#include "h/stager_client_api_common.h"   // for stage_trace("..")

#include "castor/repack/RepackWorker.hpp"
#include "castor/repack/RepackServer.hpp"




//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {

   
    try {
      // new BaseDeamon as Server 
      castor::repack::RepackServer daemon("Repack");
      daemon.parseCommandLine(argc, argv);
      daemon.start();
      
      }// end try block
      catch (castor::exception::Exception e) {
      std::cerr << "Caught castor exception : "
                  << sstrerror(e.code()) << std::endl
                  << e.getMessage().str() << std::endl;
      }
      catch (...) {
      std::cerr << "Caught general exception!" << std::endl;
      }
      return 0;
}

//------------------------------------------------------------------------------
// RepackServer Constructor
// also initialises the logging facility
//------------------------------------------------------------------------------
castor::repack::RepackServer::RepackServer(const std::string serverName) : castor::server::BaseServer(serverName) 
{
  
  m_threadpoolname = "RepackWorker";
  
  castor::BaseObject::initLog(serverName, castor::SVC_STDMSG);
  // Initializes the DLF logging. This includes
  // registration of the predefined messages
   castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Database"},
     { 3, "Could not get Conversion Service for Streaming"},
     { 4, "Could not read Socket"},
     { 5, "New Request Arrival"},
     { 6, "Invalid Request Arrival"},
     { 7, "Unable to read Request object from socket"},
     {-1, ""}};
   castor::dlf::dlf_init((char *)serverName.c_str(), messages);
   
   // Create the ThreadPool 
   this->addThreadPool(
        new castor::server::BaseThreadPool(m_threadpoolname,
            new castor::repack::RepackWorker() 
        )
    ); 
   // We only need one thread at this moment 
   this->getThreadPool(m_threadpoolname[0])->setNbThreads(1);
   
   // don't forget to initialise the Pool !
   this->getThreadPool(m_threadpoolname[0])->init();
}



void castor::repack::RepackServer::start() throw (castor::exception::Exception)
{
  // BaseServer initialisation (foreground/background)
  try {
    castor::server::BaseServer::init();   
  }catch (castor::exception::Exception e) {
    // "Exception caught : ignored" message
    // Message is already done by BaseServer
    return;
  }
  stage_trace(3, "Starting Repack and wait for connections.. on port %d",CSP_REPACKSERVER_PORT);
  castor::io::ServerSocket sock(CSP_REPACKSERVER_PORT, true);  
  for (;;) {
      // Accept connections 
      castor::io::ServerSocket* s = sock.accept();
      // handle the command.
      castor::server::BaseThreadPool* temp = this->getThreadPool(m_threadpoolname[0]);   // id of the threadpool
      temp->threadAssign((void*)s);
  }
}

//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void castor::repack::RepackServer::help(std::string programName)
{
  std::cout << "Usage: " << programName << " [options]\n"
	  "\n"
	  "where options can be:\n"
	  "\n"
	  "\t--foreground   or -f                \tForeground\n"
	  "\t--help         or -h                \tThis help\n"
	  "\t--Sthreads     or -S {integer >= 0} \tNumber of RepackRequest handler threads\n"
	  "\n"
	  "Comments to: Castor.Support@cern.ch\n";
}

