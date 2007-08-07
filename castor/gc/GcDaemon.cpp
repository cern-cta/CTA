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
 * Garbage collector daemon handling the deletion of local
 * files on a filesystem. Makes remote calls to the stager
 * to know what to delete and to update the catalog
 *
 * @author Sebastien Ponce
 *
 *****************************************************************************/

// Include Files
#include "castor/gc/GcDaemon.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "common.h"
#include "Cgetopt.h"
#include "Cinit.h"

#include <vector>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <stdlib.h>

#define  GCINTERVAL 300

/*** for test only
     #define  GCDATAFILE "/tmp/gcdata.lst"

     std::vector<castor::stager::GCLocalFile*>*
     ZselectFiles2Delete( std::string diskServer);
***/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::gc::GcDaemon::GcDaemon() : m_foreground(false), m_nodelay(false) {
  // gives a Cuuid to this server
  // XXX Interface to Cuuid has to be improved !
  // XXX its length has currently to be hardcoded
  // XXX wherever you use it !!!

  // Initializes Logging (old version)
  initLog("GC", SVC_DLFMSG);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::gc::GcDaemon::~GcDaemon() throw() {
  // hack to release specific allocated memory
  castor::Services* svcs = services();
  if (0 != svcs) {
    delete svcs;
  }
}

//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
int castor::gc::GcDaemon::start()
  throw (castor::exception::Exception) {

  // Switch to daemon mode
  int rc;
  if (!m_foreground) {
    if ((rc = Cinitdaemon ("GcDaemon", 0)) < 0) {
      return -1;
    }
  }

  // Initializes Logging
  castor::dlf::Message messages[] =
    {{ 0, " - "},  // Reserved for old version
     { 1, "Starting Garbage Collector Daemon"},
     { 2, "Could not get RemoteStagerSvc"},
     { 3, "Got a bad RemoteStagerSvc"},
     { 4, "Cannot get disk server name"},
     { 5, "Garbage Collector started successfully"},
     { 6, "Checking for garbage"},
     { 7, "Sleep interval changed"},
     { 8, "Error caught while looking for garbage files"},
     { 9, "Sleeping"},
     {10, "Found files to garbage. Starting removal"},
     {11, "Removed file successfully"},
     {12, "Failed to remove file"},
     {13, "Summary of files removed"},
     {14, "Error caught while informing stager of the files deleted"},
     {15, "No garbage files found"},
     {16, "Error caught while informing stager of the failed deletions"},
     {-1, ""}};
  castor::dlf::dlf_init("GC", messages);

  // "Starting Garbage Collector Daemon" message
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 1);

  // get RemoteGCSvc
  castor::IService* svc =
    castor::BaseObject::services()->
    service("RemoteGCSvc", castor::SVC_REMOTEGCSVC);
  if (0 == svc) {
    // "Could not get RemoteStagerSvc" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2);
    return -1;
  }
  castor::stager::IGCSvc *stgSvc =
    dynamic_cast<castor::stager::IGCSvc*>(svc);
  if (0 == stgSvc) {
    // "Got a bad RemoteStagerSvc" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("ID", svc->id()),
       castor::dlf::Param("Name", svc->name())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 2, params);
    return -1;
  }

  // Retrieve DiskServerName
  // First attempt is environment, second attempt using
  // castor.conf and finally gethostname
  // /etc/castor/castor.conf example: "GC  TARGET  machinename"
  std::string diskServerName;
  {
    char *p;
    if ( (p = getenv ("GC_TARGET")) || (p = getconfent ("GC", "TARGET", 0)) ) {
      diskServerName = p;
    } else { // get real hostname
      char cName[CA_MAXHOSTNAMELEN];
      if (gethostname(cName, CA_MAXHOSTNAMELEN) != 0) {
        // "Cannot get disk server name" message
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4);
        return (-1);
      }
      diskServerName = cName;
    }
  }

  // set GC sleep interval, sec.
  // /etc/castor/castor.conf example: "GC  INTERVAL  600"
  int gcinterval;
  {
    char *p;
    if ((p = getenv ("GC_INTERVAL")) || (p = getconfent ("GC", "INTERVAL", 0)))
      gcinterval = atoi(p);
    else
      gcinterval = GCINTERVAL;
  }

  // By default the first poll to the stager is deliberately offset by a random
  // interval between 1 and 15 minutes. This randomised delay should prevent all
  // GC's in an instance/setup from deleting files at the same time causing an
  // oscillation in incoming network traffic due to deletions.
  int delay = 0;
  if (!m_nodelay) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    srand(tv.tv_usec * tv.tv_sec);
    delay = 60 + (int) (900.0 * rand() / (RAND_MAX + 60.0));
  }

  // "Garbage Collector started successfully" message
  castor::dlf::Param params[] =
    {castor::dlf::Param("Machine", diskServerName),
     castor::dlf::Param("Sleep Interval", gcinterval),
     castor::dlf::Param("Startup Delay", delay)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 5, 3, params);

  // Sleep a bit
  if (delay) {
    sleep(delay);
  }
  
  // Main loop
  // XXX need to wake up on UDP with a time out ?
  // XXX To be discussed with Ben that has the code to do that
  for (;;) {
    // "Checking for garbage" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Machine", diskServerName)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 6, 1, params);

    // get new sleep interval if the environment has been changed
    {
      char *p;
      int  gcintervalnew;
      if ((p = getenv ("GC_INTERVAL")) || (p = getconfent ("GC", "INTERVAL", 0))) {
        gcintervalnew = atoi(p);
	if ( gcintervalnew != gcinterval) {
	  gcinterval = gcintervalnew;
	  // "Sleep interval changed" message
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("Machine", diskServerName),
	     castor::dlf::Param("Sleep Interval", gcinterval)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 7, 2, params);
	}
      }
    }

    // Retrieve list of files to delete
    std::vector<castor::stager::GCLocalFile*>* files2Delete = 0;
    try {
      files2Delete = stgSvc->selectFiles2Delete(diskServerName);
    } catch (castor::exception::Exception e) {
      // "Error caught while looking for garbage files" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 8, 1, params);
      // "Sleeping" message
      castor::dlf::Param params2[] =
        {castor::dlf::Param("Duration", gcinterval)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 1, params2);
      sleep(gcinterval);
      continue;
    }

    // Fork a child if there are files to delete
    if (0 < files2Delete->size()) {
      // "Found files to garbage. Starting removal" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Nb files", files2Delete->size())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 10, 1, params);

      // Loop over the files and delete them
      std::vector<u_signed64*> deletedFiles;
      std::vector<u_signed64*> failedFiles;
      u_signed64 gcremovedsize  = 0;
      long gcfilestotal   = 0;
      long gcfilesfailed  = 0;
      long gcfilesremoved = 0;
      for(std::vector<castor::stager::GCLocalFile*>::iterator it =
            files2Delete->begin();
          it != files2Delete->end();
          it++) {
	// Construct Cns invariant
	Cns_fileid fileId;
	fileId.fileid = (*it)->fileId();
	strncpy(fileId.server, (*it)->nsHost().c_str(), sizeof(fileId.server));	
        try {
          gcfilestotal++;
          u_signed64 gcfilesize = gcRemoveFilePath((*it)->fileName());
          gcremovedsize += gcfilesize;
          gcfilesremoved++;
          // "Removed file successfully" message
          castor::dlf::Param params[] =
            {castor::dlf::Param("File name", (*it)->fileName()),
             castor::dlf::Param("File size", gcfilesize)};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 11, 2, params, &fileId);
          // Add the file to the list of deleted ones
          u_signed64 *gcfileid = new u_signed64((*it)->diskCopyId());
          deletedFiles.push_back(gcfileid);
        } catch (castor::exception::Exception e) {
          gcfilesfailed++;
          // "Failed to remove file" message
          castor::dlf::Param params[] =
            {castor::dlf::Param("File name", (*it)->fileName()),
             castor::dlf::Param("Error", e.getMessage().str()),
             castor::dlf::Param("Origin", strerror(e.code()))};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 12, 3, params, &fileId);
          // Add the file to the list of failed ones
          u_signed64 *gcfileid = new u_signed64((*it)->diskCopyId());
          failedFiles.push_back(gcfileid);
        }
      } // end of delete files loop
      
      // log to DLF
      if (0 < gcfilestotal) {
        // "Summary of files removed" message
        castor::dlf::Param params[] =
          {castor::dlf::Param("Nb Files removed", gcfilesremoved),
           castor::dlf::Param("Nb Files failed", gcfilesfailed),
           castor::dlf::Param("Nb Files total", gcfilestotal),
           castor::dlf::Param("Space freed", gcremovedsize)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 13, 4, params);
      }
      // Inform stager of the deletion/failure
      if (deletedFiles.size() > 0) {
        try {
          stgSvc->filesDeleted(deletedFiles);
        } catch (castor::exception::Exception e) {
          // "Error caught while informing stager..." message
          castor::dlf::Param params[] =
            {castor::dlf::Param("Error", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 1, params);
        }
        // release memory
        for (std::vector<u_signed64*>::iterator it =
               deletedFiles.begin();
             it != deletedFiles.end();
             it++) {
          delete *it;
        }
      }
      if (failedFiles.size() > 0) {
        try {
          stgSvc->filesDeletionFailed(failedFiles);
        } catch (castor::exception::Exception e) {
          // "Error caught while informing stager..." message
          castor::dlf::Param params[] =
            {castor::dlf::Param("Error", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 1, params);
        }
        // release memory
        for (std::vector<u_signed64*>::iterator it =
               failedFiles.begin();
             it != failedFiles.end();
             it++) {
          delete *it;
        }
      }
    } else {
      // "No garbage files found" message
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 15);
    }
    // "Sleeping" message
    castor::dlf::Param params2[] =
      {castor::dlf::Param("Duration", gcinterval)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 1, params2);
    sleep(gcinterval);
  } // End of main loop
}

//------------------------------------------------------------------------------
// GCremoveFilePath
//------------------------------------------------------------------------------
u_signed64 castor::gc::GcDaemon::gcRemoveFilePath
(std::string gcfilepath)
  throw (castor::exception::Exception) {
  u_signed64 gcfilesize = gcGetFileSize(gcfilepath);
  if (unlink(gcfilepath.c_str()) < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "gcRemoveFilePath : Cannot unlink file " << gcfilepath;
    throw e;
  }
  return gcfilesize;
}

//------------------------------------------------------------------------------
// GCgetFileSize
//------------------------------------------------------------------------------
u_signed64 castor::gc::GcDaemon::gcGetFileSize
(std::string& gcfilepath)
  throw (castor::exception::Exception) {
  struct stat64  gcfiledata;
  if (::stat64(&gcfilepath[0], &gcfiledata) ) {
    castor::exception::Exception e(errno);
    e.getMessage() << "gcGetFileSize : Cannot stat file " << gcfilepath;
    throw e;
  }
  return gcfiledata.st_size;
}

//------------------------------------------------------------------------------
// GCparseCommandLine
//------------------------------------------------------------------------------
void castor::gc::GcDaemon::GCparseCommandLine(int argc, char *argv[]) {

  static struct Coptions longopts[] =
    {
      {"foreground", NO_ARGUMENT, NULL, 'f'},
      {"nodelay",    NO_ARGUMENT, NULL, 'n'},
      {NULL,         0,           NULL,  0 }
    };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fsn", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'n':
      m_nodelay = true;
      break;
    }
  }
}

//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  // First ignoring SIGPIPE and SIGXFSZ
  // to avoid crashing when a file is too big or
  // when the connection is lost with a client
#if ! defined(_WIN32)
  signal (SIGPIPE,SIG_IGN);
  signal (SIGXFSZ,SIG_IGN);
#endif

  try {
    castor::gc::GcDaemon server;
    server.GCparseCommandLine(argc, argv);
    std::cout << "Starting Garbage Collector Daemon... " << std::endl;
    server.start();
    std::cout << "\n-Garbage Collector Daemon Stopped." << std::endl;
    return 0;
  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  } catch (...) {
    std::cerr << "Caught exception!" << std::endl;
  }

  // shutdown the DLF interface
  dlf_shutdown(10);
}

