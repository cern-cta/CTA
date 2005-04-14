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
#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "common.h"
#include "Cgetopt.h"
#include "Cinit.h"

#include <vector>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdio.h>
//#include <string.h>
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
castor::gc::GcDaemon::GcDaemon() : m_foreground(false) {
  // gives a Cuuid to this server
  // XXX Interface to Cuuid has to be improved !
  // XXX its length has currently to be hardcoded
  // XXX wherever you use it !!!

  Cuuid_create(&m_uuid);
  // Initializes Logging
  initLog("GC", SVC_DLFMSG);
  // Use our uuid to log
  clog() << m_uuid;
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


  clog() << USAGE << " +++ Starting Garbage Collector Daemon... " << std::endl;
  // Switch to daemon mode
  int rc;
  if (!m_foreground) {
    if ((rc = Cinitdaemon ("GcDaemon", 0)) < 0) {
      return -1;
    }
  }

  // get RemoteStagerSvc
  castor::IService* svc =
    castor::BaseObject::services()->
    service("RemoteStagerSvc", castor::SVC_REMOTESTAGERSVC);
  if (0 == svc) {
    clog() << ERROR << "Could not get RemoteStagerSvc"
           << std::endl;
    return -1;
  }
  castor::stager::IStagerSvc *stgSvc =
    dynamic_cast<castor::stager::IStagerSvc*>(svc);
  if (0 == stgSvc) {
    clog() << "ERROR " << "Got a bad RemoteStagerSvc - id: "
           << svc->id() << " name: "
           << svc->name() << std::endl;
    return -1;
  }

  char *p;
  //  Retrieve DiskServerName
  char diskServerName[CA_MAXHOSTNAMELEN];
  char gcglobalFS[20] = "Global FileSystem";
  char *gctarget;
  // Get DiskServerName from /etc/castor/castor.conf:
  // example to serve global filesystem: "GC TARGET localhost"
  if ( (p = getenv ("GC_TARGET")) || (p = getconfent ("GC", "TARGET", 0)) ) {
    gctarget = strcpy(diskServerName, p);
    if ( (strcmp(gctarget, "localhost")) == 0) { gctarget = gcglobalFS; }
  } else { // get real hostname
    if (gethostname(diskServerName, CA_MAXHOSTNAMELEN) != 0) {
      clog() << ERROR << "Cannot get disk server name." << std::endl;
      return (-1);
    }
    gctarget = diskServerName;
  }

  int lhostlen = strlen( diskServerName );
  for (int i = 0; i < lhostlen; i++) {
    if (diskServerName[i] == '.') {diskServerName[i] = '\0'; break; }
  }
  lhostlen = strlen( diskServerName );


  // set GC sleep interval, sec.
  // /etc/castor/castor.conf example: "GC  INTERVAL  600"
  char gcint[16];
  int  gcinterval;
  if ((p = getenv ("GC_INTERVAL")) || (p = getconfent ("GC", "INTERVAL", 0)))
    gcinterval = atoi( strcpy (gcint, p) );
  else
    gcinterval = GCINTERVAL;

  // Log to DLF
  clog() << USAGE << "Garbage Collector started on " << gctarget
         << "; Sleep interval: " << gcinterval << " sec."
         << std::endl;



  // Main loop
  // XXX need to wake up on UDP with a time out ?
  // XXX To be discussed with Ben that has the code to do that

  for (;;) {
    int cid;
    clog() << DEBUG
           << " Checking garbage on " << diskServerName
	   << std::endl;

    // get new sleep interval if the environment has been changed
    int  gcintervalnew;
    if ((p = getenv ("GC_INTERVAL")) || (p = getconfent ("GC", "INTERVAL", 0)))
      gcintervalnew = atoi( strcpy (gcint, p) );
    else gcintervalnew = gcinterval;
    if ( gcintervalnew != gcinterval) {
      gcinterval = gcintervalnew;
      clog() << USAGE << " New sleep interval: " << gcinterval << " sec." << std::endl;
    }

    // Retrieve list of files to delete
    std::vector<castor::stager::GCLocalFile*>* files2Delete = 0;
    try {
      files2Delete = stgSvc->selectFiles2Delete(diskServerName);
    } catch (castor::exception::Exception e) {
      clog() << DEBUG << "Error caught while looking for garbage files."
             << " Sleeping  " << gcinterval << " sec..."
             << std::endl << e.getMessage().str();
      sleep(gcinterval);
      continue;
    }

    // Fork a child if there are files to delete
    if (0 < files2Delete->size()) {
      clog() << USAGE << files2Delete->size()
	     << " garbage files found. Starting removal..." << std::endl;

      cid = fork();
      if(cid < 0) {
        clog() << ERROR << " Fork child failed " << std::endl;
        sleep(gcinterval);
        continue;
      }
      if (0 == cid) { // if a am a child
        clog() << USAGE
               << " Removing Garbage files ..."
               << std::endl;
        // Loop over the files and delete them
        std::vector<u_signed64*> deletedFiles;
        u_signed64 gcremovedsize  = 0;
        long gcfilestotal   = 0;
        long gcfilesfailed  = 0;
        long gcfilesremoved = 0;
        for(std::vector<castor::stager::GCLocalFile*>::iterator it =
              files2Delete->begin();
            it != files2Delete->end();
            it++) {
	  try {
	    gcfilestotal++;
	    u_signed64 gcfilesize = gcRemoveFilePath((*it)->fileName());
            gcremovedsize =+ gcfilesize;
            gcfilesremoved++;
            clog() << DEBUG << "Removed " << (*it)->fileName() << ": "
                   << gcfilesize << " KB"
                   << std::endl;
            // Add the file to the list of deleted ones
            u_signed64 *gcfileid = new u_signed64((*it)->diskCopyId());
            deletedFiles.push_back(gcfileid);
	  } catch (castor::exception::Exception e) {
            gcfilesfailed++;
            clog() << ERROR << "Failed to remove "
		   << (*it)->fileName() << "\n"
		   << e.getMessage().str() << std::endl;
          }
        } // end of delete files loop

        // log to DLF
        if (0 < gcfilestotal) {
          gcremovedsize = gcremovedsize/1024;
          clog() << USAGE << "Files removed: "  << gcfilesremoved
                 << "; Files failed: " << gcfilesfailed
                 << "; Files total: "  << gcfilestotal
                 << "; Space freed: "  << gcremovedsize << " KB;"
                 << std::endl;
        }
        // Inform stager of the deletion
        try {
          stgSvc->filesDeleted(deletedFiles);
        } catch (castor::exception::Exception e) {
          clog() << ERROR << "Error caught while informing stager of the files deleted :\n"
                 << e.getMessage().str()
                 << "Files IDs : ";
          for (std::vector<u_signed64*>::iterator it = deletedFiles.begin();
               it != deletedFiles.end();
               it++) {
            clog() << **it << " ";
          }
          clog() << std::endl;
        }
        // release memory
        for (std::vector<u_signed64*>::iterator it =
               deletedFiles.begin();
             it != deletedFiles.end();
             it++) {
          delete *it;
        }
        exit(0); // end child
      }
    } else {
      clog() << DEBUG
	     << "No garbage files found." << std::endl;
    }
    clog() << DEBUG
           << "GC check finished on " << diskServerName
           << " - sleeping " << gcinterval << " sec..."
           << std::endl;
    
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
      {"foreground",         NO_ARGUMENT,        NULL,      'f'},
      {NULL,                 0,                  NULL,        0}
    };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fs", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
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
}
//-EOF--------------------------------------------------------------------------
