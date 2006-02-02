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
 * @(#)$RCSfile: RepackServer.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2006/02/02 18:05:07 $ $Author: felixehm $
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

	char* port;
	const char* REPACK_PORT = "REPACK_PORT";
	int iport;
	if ( (port = getenv (REPACK_PORT)) != 0 ) {
	    char* dp = port;
	    iport = strtoul(port, &dp, 0);
	    if (*dp != 0) {
			std::cerr << "Bad port value in enviroment variable " << REPACK_PORT
			<< std::endl;
			return 1;
    	}
    	if ( iport > 65535 ){
    		std::cerr << "Given port no. in enviroment variable " << REPACK_PORT 
			<< "exceeds 65535 !"<< std::endl;
			return 1;
		}
	}
	else
		iport = CSP_REPACKSERVER_PORT;
	

   
    try {
      // new BaseDeamon as Server 
    castor::repack::RepackServer server;
    server.addThreadPool(
      new castor::server::SignalThreadPool("FileOrganizer", new castor::repack::FileOrganizer() ));

    server.addThreadPool(
      new castor::server::ListenerThreadPool("RepackWorker", new castor::repack::RepackWorker(), iport));
    
    // We only need one thread by default at this moment 
    server.getThreadPool('R')->setNbThreads(1);
    server.getThreadPool('F')->setNbThreads(1);
    
    server.parseCommandLine(argc, argv);
    server.start();
      }// end try block
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
 
}

castor::repack::RepackServer::~RepackServer() throw()
{
}





