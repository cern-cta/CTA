/******************************************************************************
 *                      DiskCopyTransfer.cpp
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
 * @(#)$RCSfile: DiskCopyTransfer.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/26 14:54:55 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/job/diskcopy/DiskCopyTransfer.hpp"
#include "castor/job/diskcopy/MainThread.hpp"
#include "castor/server/SignalThreadPool.hpp"


//-----------------------------------------------------------------------------
// Main
//-----------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  try {
    castor::job::diskcopy::DiskCopyTransfer daemon;
    
    // We create a thread here to handle the main logic/operations of the disk
    // copy transfer mover. We use a thread as we'd like to benefit from the
    // signal handling of the BaseDaemon's start method
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("MainThread",
	new castor::job::diskcopy::MainThread(argc, argv), 0));
    daemon.getThreadPool('M')->setNbThreads(1);

    // Start daemon
    daemon.parseCommandLine(argc, argv);
    daemon.start();
    return 0;
    
  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception: "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;

    // "Exception caught when starting DiskCopyTransfer"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 2, params);
  } catch (...) {
    std::cerr << "Caught exception " << std::endl;
  }
  return 1;
}


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::job::diskcopy::DiskCopyTransfer::DiskCopyTransfer():
  castor::server::BaseDaemon("DiskCopy") {

  // Now with predefined messages
  castor::dlf::Message messages[] = {

    // General
    { 1,  "DiskCopy Transfer started" },
    { 2,  "Exception caught when starting DiskCopyTransfer" },

    // Constructor
    { 10, "Unable to get RemoteJobSvc, transfer terminated" },
    { 11, "Could not convert newly retrieved service into IJobSvc" },
    { 12, "Invalid DiskCopy/RetryInterval option, using default" },
    { 13, "Invalid DiskCopy/RetryAttempts option, using default" },
    { 14, "Failed to initialize mover" },
    { 15, "Failed to create sharedResource helper" },

    // Run
    { 20, "Failed to change uid and gid" },
    { 21, "Invalid Uniform Resource Indicator, cannot download resource file" },
    { 22, "Exceeded maximum number of attempts trying to download resource file" },
    { 23, "Exception caught trying to download resource file" },
    { 24, "Exception caught trying to getHostName, unable to determine which end of a disk2disk copy transfer is the destination" },
    { 25, "DiskCopyTransfer started" },
    { 26, "Exception caught trying to get information on destination and source disk copies" },
    { 27, "Failed to remotely execute disk2DiskCopyStart" },
    { 28, "Starting destination end of mover" },
    { 29, "Exception caught transfering file" },
    { 30, "Exception caught trying to finalize disk2disk copy transfer, transfer failed" },
    { 31, "Failed to remotely execute disk2DiskCopyDone" },
    { 32, "The content of the resource file is invalid" },
    { 39, "DiskCopy Transfer successful" },
    
    // Exit
    { 41, "DiskCopy Transfer failed" },
    { 42, "Exception caught trying to fail disk2DiskCopy transfer" },
    { 43, "Failed to remotely execute disk2DiskCopyFailed" },

    { -1, "" }};
  dlfInit(messages);
}
