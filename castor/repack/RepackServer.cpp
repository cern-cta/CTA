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
 * @(#)$RCSfile: RepackServer.cpp,v $ $Revision: 1.10 $ $Release$ $Date: 2006/03/03 17:14:03 $ $Author: felixehm $
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
		iport = castor::repack::CSP_REPACKSERVER_PORT;
	

	try {

    castor::repack::RepackServer server;
	
    server.addThreadPool(
      new castor::server::ListenerThreadPool("Worker", new castor::repack::RepackWorker(), iport));
	server.getThreadPool('W')->setNbThreads(1);
	
    server.addThreadPool(
      new castor::server::SignalThreadPool("Stager", new castor::repack::RepackFileStager() ));
	server.getThreadPool('S')->setNbThreads(1);

	server.addThreadPool(
      new castor::server::SignalThreadPool("Migrator", new castor::repack::RepackFileMigrator() ));
	server.getThreadPool('M')->setNbThreads(1);
	/*
	server.addThreadPool(
      new castor::server::SignalThreadPool("Cleaner", new castor::repack::RepackCleaner() ));
	server.getThreadPool('C')->setNbThreads(1);
    */
    
    server.parseCommandLine(argc, argv);
    server.start();
    } catch (castor::exception::Internal i){
    	std::cerr << "Caught castor internal exception : "
			<< sstrerror(i.code()) << std::endl
			<< i.getMessage().str() << std::endl;
    }
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
  // Initializes the DLF logging. This includes
  // defining the predefined messages
  
  castor::BaseObject::initLog("Repack", castor::SVC_STDMSG);
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New Request Arrival"},
     { 2, "Could not get Conversion Service for Database"},
     { 3, "Could not get Conversion Service for Streaming"},
     { 4, "Exception caught : server is stopping"},
     { 5, "Exception caught : ignored"},
     { 6, "Invalid Request"},
     { 7, "Unable to read Request from socket"},
     { 8, "Processing Request"},
     { 9, "Exception caught"},
     {10, "Sending reply to client"},		// remove ?
     {11, "Unable to send Ack to client"},
     {12, "DatabaseHelper: Request stored in DB"},
     {13, "DatabaseHelper: Unable to store Request in DB"},
     {14, "FileListHelper: Fetching files from Nameserver"},						// FileListHelper::getFileList()
     {15, "FileListHelper: Cannot get file pathname"},								// FileListHelper::getFilePathnames
     {16, "RepackWorker : No such Tape!"},											// RepackWorker:getTapeInfo()
   	 {17, "RepackWorker : Tape has unkown status, repack abort for this tape!"},	// RepackWorker:getTapeInfo()
   	 {18, "RepackWorker : Tape is marked as FREE, no repack to be done"},			// RepackWorker:getTapeInfo()
     {19, "RepackWorker : No such pool!"},											// RepackWorker:getPoolInfo()
     {20, "RepackWorker : Adding tapes for pool repacking!"},						// RepackWorker:getPoolInfo()
     {21, "RepackDaemon: Unable to stage files!"},								// RepackDaemon:stage_files
     {22, "RepackDaemon: New request for staging files"},			// RepackDaemon:run()
     {23, "RepackDaemon: Not enough space for this RepackRequest. Skipping..."},	// RepackDaemon:stage_files
     {24, "FileListHelper: Retrieved segs for SubRequest."},								// FileListHelper:getFileListSegs()
     {25, "RepackDaemon: Updating Request to STAGING and add its segs."},			// RepackDaemon:stage_files
     {26, "RepackDaemon: Staging files."},			// RepackDaemon:stage_files
     {27, "DatabaseHelper: Unable to update SubRequest!"},
     {28, "DatabaseHelper: Tape already in repack que!"},
     {29, "RepackDaemon: Getting Segs for SubRequest!"},
     {30, "DatabaseHelper: SubRequest updated!"},
     {31, "RepackWorker: Tape is good for repack!"},
     {99, "TODO::MESSAGE"},
     {-1, ""}};
  castor::dlf::dlf_init("Repack", messages); 
}

castor::repack::RepackServer::~RepackServer() throw()
{
}





