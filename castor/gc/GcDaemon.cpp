/******************************************************************************
 *                      GcDaemon.cpp
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
 * @(#)GcDaemon.cpp,v 1.10 $Release$ 2005/03/31 15:17:24 sponcec3
 *
 * Garbage collector daemon handling the deletion of local files on a
 * filesystem. Makes remote calls to the stager to know what to delete and to
 * update the catalog
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include files
#include "castor/gc/GcDaemon.hpp"
#include "castor/gc/DeletionThread.hpp"
#include "castor/gc/SynchronizationThread.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "Cgetopt.h"
#include "getconfent.h"
#include <sys/time.h>
#include <string.h>


//-----------------------------------------------------------------------------
// main
//-----------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  try {
    castor::gc::GcDaemon daemon;

    // Randomize the start-up of the daemon between 1 and 15 minutes.
    char *value;
    int startDelay = 1;
    if ((value = getenv("GC_IMMEDIATESTART")) ||
	(value = getconfent("GC", "ImmediateStart", 0))) {
      if (!strcasecmp(value, "yes") || !strcmp(value, "1")) {
	startDelay = 0;
      }
    }
    if (startDelay){
      timeval tv;
      gettimeofday(&tv, NULL);
      srand(tv.tv_usec * tv.tv_sec);
      startDelay = 60 + (int) (900.0 * rand() / (RAND_MAX + 60.0));
    }

    // "Garbage Collector started successfully"
    castor::dlf::Param params[] =
      {castor::dlf::Param("StartDelay", startDelay)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 5, 1, params);

    // Create the deletion thread
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("Deletion",
	new castor::gc::DeletionThread(startDelay)));
    daemon.getThreadPool('D')->setNbThreads(1);

    // Create the Synchronization thread
    daemon.addThreadPool
      (new castor::server::SignalThreadPool
       ("Synchronization",
	new castor::gc::SynchronizationThread(startDelay)));
    daemon.getThreadPool('S')->setNbThreads(1);

    // Start daemon as the stager superuser
    daemon.parseCommandLine(argc, argv);
    daemon.runAsStagerSuperuser();
    daemon.start();
    return 0;

  } catch (castor::exception::Exception& e) {
    std::cerr << "Caught exception: "
	      << sstrerror(e.code()) << std::endl
	      << e.getMessage().str() << std::endl;

    // "Exception caught when starting Garbage Collector daemon"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Code", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 18, 2, params);
  } catch (...) {
    std::cerr << "Caught exception!" << std::endl;
  }

  return 1;
}


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::gc::GcDaemon::GcDaemon(): castor::server::BaseDaemon("gcd") {

  // Now with predefined messages
  castor::dlf::Message messages[] = {
    {  1, "Starting deletion thread" },
    {  2, "Could not get RemoteStagerSvc" },
    {  3, "Got a bad RemoteStagerSvc" },
    {  5, "Garbage Collector started successfully" },
    {  6, "Checking for garbage" },
    {  7, "Sleep interval changed" },
    {  8, "Error caught while looking for garbage files" },
    {  9, "Sleeping" },
    { 10, "Found files to garbage collect. Starting removal" },
    { 11, "Removed file successfully" },
    { 12, "Failed to remove file" },
    { 13, "Summary of files removed" },
    { 14, "Error caught while informing stager of the files deleted" },
    { 15, "No garbage files found" },
    { 16, "Error caught while informing stager of the failed deletions" },
    { 17, "Exception caught trying to getHostName" },
    { 17, "Exception caught when starting Garbage Collector daemon" },
    { 18, "Starting synchronization thread" },
    { 19, "Invalid GC/SyncInterval option, using default" },
    { 20, "Invalid GC/ChunkSize option, using default" },
    { 21, "New synchronization interval" },
    { 22, "New synchronization chunk size" },
    { 23, "Unable to retrieve mountpoints, giving up with synchronization" },
    { 24, "Could not list filesystem directories, giving up with filesystem's synchronisation" },
    { 25, "Could not list filesystem subdirectory, ignoring it for synchronization" },
    { 27, "Deleting local file which is no longer in the nameserver" },
    { 28, "Deletion of orphaned local file failed" },
    { 29, "Memory allocation failure" },
    { 30, "Synchronization configuration" },
    { 31, "Synchronizing files with nameserver and stager catalog" },
    { 32, "Error calling nameserver function Cns_bulkexist" },
    { 33, "Summary of files removed by stager synchronization" },
    { 35, "Summary of files removed by nameserver synchronization" },
    { 36, "Deleting local file which is no longer in the stager catalog" },
    { 37, "New chunk interval" },
    { 38, "Invalid GC/ChunkInterval option, using default" },
    { 39, "Ignoring filename that does not conform to castor naming conventions" },
    { 40, "Unexpected exception caught in synchronizeFiles" },
    { 41, "Failed to stat file" },
    { 42, "File scheduled for deletion as the file no longer exists in the nameserver but still exists in the stager" },
    { -1, "" }};
  dlfInit(messages);
}
