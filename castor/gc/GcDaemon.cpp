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
 * @(#)$RCSfile: GcDaemon.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/02/25 18:00:54 $ $Author: sponcec3 $
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
#include "Cgetopt.h"
#include "Cinit.h"
#include <vector>
#include <signal.h>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::gc::GcDaemon::GcDaemon() {
  // gives a Cuuid to this server
  // XXX Interface to Cuuid has to be improved !
  // XXX its length has currently to be hardcoded
  // XXX wherever you use it !!!
  Cuuid_create(&m_uuid);
  // Initializes Logging
  initLog("GC", SVC_DLFMSG);
  // Use ou uuid to log
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
    clog() << ERROR << "Got a bad RemoteStagerSvc. It's id was "
           << svc->id() << " and its name was "
           << svc->name() << std::endl;
    return -1;
  }

  // XXX Retrieve DiskServerName
  std::string diskServerName = "";

  // Main loop
  // XXX need to wake up on UDP with a time out ?
  // XXX To be discussed with Ben that has the code to do that
  for (;;) {

    // Log to DLF
    clog() << USAGE << "Starting GC on " << diskServerName << std::endl;

    // Retrieve list of files to delete
    std::vector<castor::stager::GCLocalFile>* files2Delete = 0;
    try {
      files2Delete = stgSvc->selectFiles2Delete(diskServerName);
    } catch (castor::exception::Exception e) {
      clog() << ERROR << "Unable to retrieve files to delete."
             << std::endl << e.getMessage().str();
      break;
    }

    // List of files deleted
    std::vector<u_signed64*> deletedFiles;

    // Loop over the files and delete them
    for(std::vector<castor::stager::GCLocalFile>::iterator it =
          files2Delete->begin();
        it != files2Delete->end();
        it++) {

      // XXX Implement the deletion of file it->fileName()

      // Log to DLF
      clog() << DEBUG << "Deleted file " << it->fileName() << std::endl;

      // Add the file to the list of deleted ones
      deletedFiles.push_back(&(it->diskCopyId()));
    }

    // Inform stager of the deletion
    try {
      stgSvc->filesDeleted(deletedFiles);
    } catch (castor::exception::Exception e) {
      clog() << ERROR << "Unable to inform stager of the files deleted :"
             << std::endl << e.getMessage().str()
             << std::endl << "Here is the list of ids : ";
      for (std::vector<u_signed64*>::iterator it = deletedFiles.begin();
           it != deletedFiles.end();
           it++) {
        clog() << **it << " ";
      }
    }

    // Log to DLF
    clog() << USAGE << "End of GC on " << diskServerName << std::endl;

  } // End of main loop

}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::gc::GcDaemon::parseCommandLine(int argc, char *argv[]) {

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
    server.parseCommandLine(argc, argv);
    std::cout << "Starting Garbage Collector Daemon" << std::endl;
    server.start();
    return 0;
  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;
  } catch (...) {
    std::cerr << "Caught exception!" << std::endl;
  }
}
