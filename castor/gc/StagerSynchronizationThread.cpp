/******************************************************************************
 *                      StagerSynchronizationThread.cpp
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
 * @(#)$RCSfile: StagerSynchronizationThread.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/02/29 13:01:07 $ $Author: mmartins $
 *
 * Synchronization thread used to check periodically whether files need to be deleted
 *
 * @author castor dev team
 *****************************************************************************/

// Include files
#include "castor/gc/StagerSynchronizationThread.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "castor/System.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "getconfent.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <map>

// Definitions
#define DEFAULT_STG_GCINTERVAL 300
#define DEFAULT_STG_SYNCINTERVAL 10
#define DEFAULT_STG_CHUNKSIZE 1000


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::gc::StagerSynchronizationThread::StagerSynchronizationThread() {
  
  syncIntervalConf = "STG_SYNCINTERVAL";
  syncInterval = DEFAULT_STG_GCSYNCINTERVAL;
  chunkSizeConf = "STG_CHUNKSIZE";
  chunkSize = DEFAULT_STG_CHUNKSIZE;
  
  type = syncStager;
  

}

//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::gc::StagerSynchronizationThread::run(void *param) {  

  // "Starting Garbage Collector Daemon" message
  castor::dlf::Param param[] =
    { castor::dlf::Param("GC SynchronizationThread:","Stager")};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 18, 1, param);
  
  // Get the synchronization interval and chunk size
  unsigned int syncInterval = DEFAULT_STG_SYNCINTERVAL;
  unsigned int chunkSize = DEFAULT_STG_CHUNKSIZE;
  readConfigFile(true);

  // Endless loop
  for (;;) {
    // get the synchronization interval and chunk size
    // these may have changed since last iteration
    readConfigFile();

    // Get the list of filesystem to be checked
    char** fs;
    int nbFs;
    if (getconfent_multi
        ("RmNode", "MountPoints", 1, &fs, &nbFs) < 0) {
      // "Unable to retrieve mountPoints, giving up with synchronization"
      // valid logging
      castor::dlf::Param param[] =
	{ castor::dlf::Param("GC SynchronizationThread:","Stager")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 23, 2, param);
      sleep(syncInterval);
      continue;
    }
    // initialize random number generator
    srand(time(0));
    // Loop over the fileSystems starting in a random place
    int fsIt = (int) (nbFs * (rand() / (RAND_MAX + 1.0)));
    for (int i = 0; i < nbFs; i++) {
      // list the filesystem directories in random order
      std::vector<std::string> directories;
      DIR *dirs = opendir(fs[fsIt]);
      if (0 == dirs) {
        // "Could not list filesystem directories"
	// valid logging
	castor::dlf::Param params[] =
	  { castor::dlf::Param("GC SynchronizationThread:","Stager"),
	    castor::dlf::Param("FileSystem", fs[fsIt]),
	    castor::dlf::Param("Error", strerror(errno))};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 24, 3, params);
        continue;
      }
      struct dirent *dir;
      while ((dir = readdir(dirs))) {
	/******/
	struct stat file;
	std::ostringstream filepath;
	filepath << fs[fsIt] << dir->d_name;
	if (stat(filepath.str().c_str(), &file) < 0) {
	  continue;
	} else if (!(file.st_mode & S_IFDIR)) {
	  continue;  /* not a file */
	}
	int offset = (int) ((1 + directories.size()) *
			    (rand() / (RAND_MAX + 1.0)));
	directories.insert
	  (directories.begin() + offset,
	   filepath.str().c_str());

      }
      closedir(dirs);
      // Loop over the directories
      for (std::vector<std::string>::const_iterator it =castor::dlf::Param("nbFilesClean", delFileIds.size()-orphans.size()),
             directories.begin();
           it != directories.end();
           it++) {
        // Loop over files inside a directory
        DIR *files = opendir(it->c_str());
        if (0 == files) {
          // "Could not list filesystem subdirectory"
	  // valid logging
          castor::dlf::Param params[] =
            { castor::dlf::Param("GC SynchronizationThread:","Stager"),
	      castor::dlf::Param("FileSystem", fs[fsIt]),
	      castor::dlf::Param("Directory", *it),
	      castor::dlf::Param("Error", strerror(errno))};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 25, 4, params);
          continue;
        }
        // list files in this directory
        struct dirent *file;
        std::map<std::string, std::vector<u_signed64> > diksCopyIds;
        std::map<std::string, std::map<u_signed64, std::string> > paths;
        while ((file = readdir(files))) {
          // ignore non regular files
	  /******/
	  struct stat filebuf;
	  std::string filepath (*it+"/"+file->d_name);
	  if (stat(filepath.c_str(), &filebuf) < 0) {
	    continue;
	  } else if (!(filebuf.st_mode & S_IFREG)) {
	    continue;  /* not a file */
	  }
          try {
            std::pair<std::string, u_signed64> fid =
              diskCopyIdFromFileName(file->d_name);
            diksCopyIds[fid.first].push_back(fid.second);
            paths[fid.first][fid.second] = *it+"/"+file->d_name;
            // in case of large number of files, synchronize a first chunk
            if (diksCopyIds[fid.first].size() >= chunkSize) {
              // Synchronizing files with Stager
              castor::dlf::Param params[] =
                { castor::dlf::Param("GC SynchronizationThread:","Stager"),
		  castor::dlf::Param("Directory", *it),
		  castor::dlf::Param("NbFiles", diksCopyIds[fid.first].size()),
		  castor::dlf::Param("Stager", fid.first)};
              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 40, 4, params);
              synchronizeFiles(diksCopyIds[fid.first], paths[fid.first]);
              diksCopyIds[fid.first].clear();
              paths[fid.first].clear();
	      sleep(syncInterval);
            }
          } catch (castor::exception::Exception e) {
            // ignore files badly named, they are probably not ours
          }
        }
        // synchronize all files for all nameservers
        for (std::map<std::string, std::vector<u_signed64> >::const_iterator it2 =
               diksCopyIds.begin();
             it2 != diksCopyIds.end();
             it2++) {
          if (it2->second.size() > 0) {
            // Synchronizing files with Stager
            castor::dlf::Param params[] =
              { castor::dlf::Param("GC SynchronizationThread:","Stager"),
		castor::dlf::Param("Directory", *it),
		castor::dlf::Param("NbFiles", it2->second.size()),
		castor::dlf::Param("Stager", it2->first)};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 40, 4, params);
            synchronizeFiles(it2->second, paths[it2->first]);
	    sleep(syncInterval);
          }
        }
        closedir(files);
      }
      free(fs[fsIt]);
      fsIt = (fsIt + 1) % nbFs;
    }
    free(fs);
    sleep(syncInterval);
  }
}

//-----------------------------------------------------------------------------
// diskCopyIdFromFileName
//-----------------------------------------------------------------------------
std::pair<std::string, u_signed64>
castor::gc::StagerSynchronizationThread::diskCopyIdFromFileName
(char *fileName) throw (castor::exception::Exception) {
  // the filename should start with the numerical fileid
  u_signed64 fid = atoll(fileName);
  if (0 >= fid) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse fileName : '"
                   << fileName << "'";
    throw e;
  }
  // then we should have diskCopyId
  char* p = index(fileName, '.');
  int len = p - 1;
  if (len <= 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse fileName : '"
                   << fileName << "'";
    throw e;
  }
  return std::pair<std::string, u_signed64>
    (std::string(p+1, len), fid);
}







//-----------------------------------------------------------------------------
// synchronizeFiles
//-----------------------------------------------------------------------------
void castor::gc::StagerSynchronizationThread::synchronizeFiles
( const std::vector<u_signed64> &diksCopyIds,
  const std::map<u_signed64, std::string> &paths) throw() {
  
 

  // Put the returned deleted files into a vector
  std::vector<u_signed64> delDiskCopyIds;
  for (std::vector<u_signed64>::const_iterator it = diskCopyIds.begin();
       it != diskCopyIds.end();
       it+) {
    delDiskCopyIds.push_back(diskCopyIds.find(*it));
  }
 

  // Get RemoteGCSvc
  castor::IService* svc =
    castor::BaseObject::services()->
    service("RemoteGCSvc", castor::SVC_REMOTEGCSVC);
  castor::stager::IGCSvc* gcSvc =
    dynamic_cast<castor::stager::IGCSvc*>(svc);
  if (0 == gcSvc) {
    // "Could not get RemoteStagerSvc" message
    // valid for the stager
    castor::dlf::Param param[]=
      { castor::dlf::Param("GC SynchronizationThread:", "Stager")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, param);
    return;
  }



  // get the files which dont exist anymore in the stager
  std::vector<u_signed64> orphans =
    gcSvc->stgFilesDeleted(delDiskCopyIds);
  // "Getting files which dont exist anymore in the Stager"
  castor::dlf::Param params[] =
    { castor::dlf::Param("GC SynchronizationThread:","Stager"),
      castor::dlf::Param("nbOrphanFiles", orphans.size())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 41, 2, params);
 

  if(orphans.size() == 0){
    return;
  }
  // remove local files that are orphaned
  struct Cns_fileid fid;
  strncpy(fid.server, "noNameServerHost",CA_MAXHOSTNAMELEN+1 );
  for (std::vector<u_signed64>::const_iterator it = orphans.begin();
       it != orphans.end();
       it++) {
    fid.fileid = *it;
    // "Deleting local file which is not in the Stager"
    castor::dlf::Param params[] =
      { castor::dlf::Param("GC SynchronizationThread:","Stager"),
	castor::dlf::Param("fileName", paths.find(*it)->second)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 42, 2, params, &fid);
    if (unlink(paths.find(*it)->second.c_str()) < 0) {
      // "Failed Deletion of orphan file in the Stager"
      castor::dlf::Param params[] =
	{ castor::dlf::Param("GC SynchronizationThread:","Stager"),
	  castor::dlf::Param("FileName", paths.find(*it)->second),
	  castor::dlf::Param("Error", strerror(errno))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 43, 3, params, &fid);
    }
  }
}
