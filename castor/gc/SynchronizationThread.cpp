/******************************************************************************
 *                      SynchronizationThread.cpp
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
 * @(#)$RCSfile: SynchronizationThread.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2008/03/03 13:26:03 $ $Author: waldron $
 *
 * Synchronization thread used to check periodically whether files need to be
 * deleted
 *
 * @author castor dev team
 *****************************************************************************/

// Include files
#include "castor/gc/SynchronizationThread.hpp"
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
#define DEFAULT_GCINTERVAL   300
#define DEFAULT_SYNCINTERVAL 10
#define DEFAULT_CHUNKSIZE    1000


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::gc::SynchronizationThread::SynchronizationThread() {};


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::gc::SynchronizationThread::run(void *param) {

  // "Starting Garbage Collector Daemon"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 18);

  // Get the synchronization interval and chunk size
  unsigned int syncInterval = DEFAULT_SYNCINTERVAL;
  unsigned int chunkSize = DEFAULT_CHUNKSIZE;
  readConfigFile(&syncInterval, &chunkSize, true);

  // Endless loop
  for (;;) {

    // Get the synchronization interval and chunk size these may have changed
    // since the last iteration
    readConfigFile(&syncInterval, &chunkSize);

    // Get the list of filesystem to be checked
    char** fs;
    int nbFs;
    if (getconfent_multi("RmNode", "MountPoints", 1, &fs, &nbFs) < 0) {
      // "Unable to retrieve mountpoints, giving up with synchronization"
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 23);
      sleep(syncInterval);
      continue;
    }

    // Initialize random number generator
    srand(time(0));

    // Loop over the fileSystems starting in a random place
    int fsIt = (int) (nbFs * (rand() / (RAND_MAX + 1.0)));
    for (int i = 0; i < nbFs; i++) {

      // List the filesystem directories in random order
      std::vector<std::string> directories;
      DIR *dirs = opendir(fs[fsIt]);
      if (0 == dirs) {
        // "Could not list filesystem directories"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("FileSystem", fs[fsIt]),
           castor::dlf::Param("Error", strerror(errno))};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 24, 2, params);
        continue;
      }
      struct dirent *dir;
      while ((dir = readdir(dirs))) {
	struct stat file;
	std::ostringstream filepath;
	filepath << fs[fsIt] << dir->d_name;
	if (stat(filepath.str().c_str(), &file) < 0) {
	  continue;
	} else if (!(file.st_mode & S_IFDIR)) {
	  continue;  // not a directory
	} else if (!strcmp(dir->d_name, ".") || !strcmp(dir->d_name, "..")) {
	  continue;
	} else if (strspn(dir->d_name, "0123456789") != strlen(dir->d_name)
		   || (strlen(dir->d_name) != 2)) {
	  continue;  // not a numbered directory name between 00 and 99
	}
	int offset = (int) ((1 + directories.size()) *
			    (rand() / (RAND_MAX + 1.0)));
	directories.insert
	  (directories.begin() + offset, filepath.str().c_str());
      }
      closedir(dirs);

      // Loop over the directories
      for (std::vector<std::string>::const_iterator it =
             directories.begin();
           it != directories.end();
           it++) {

        // Loop over files inside a directory
        DIR *files = opendir(it->c_str());
        if (0 == files) {
          // "Could not list filesystem subdirectory"
          castor::dlf::Param params[] =
            {castor::dlf::Param("FileSystem", fs[fsIt]),
             castor::dlf::Param("Directory", *it),
             castor::dlf::Param("Error", strerror(errno))};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 25, 3, params);
          continue;
        }
        // List files in this directory
        struct dirent *file;
        std::map<std::string, std::vector<u_signed64> > diskCopyIds;
        std::map<std::string, std::map<u_signed64, std::string> > paths;
        while ((file = readdir(files))) {

          // Ignore non regular files
	  struct stat filebuf;
	  std::string filepath (*it + "/" + file->d_name);
	  if (stat(filepath.c_str(), &filebuf) < 0) {
	    continue;
	  } else if (!(filebuf.st_mode & S_IFREG)) {
	    continue;  // not a file
	  }
          try {
            std::pair<std::string, u_signed64> fid =
              diskCopyIdFromFileName(file->d_name);
            diskCopyIds[fid.first].push_back(fid.second);
            paths[fid.first][fid.second] = *it + "/" + file->d_name;

	    // In the case of a large number of files, synchronize them in
	    // chunks so to not overwhelming central services
	    if (diskCopyIds[fid.first].size() >= chunkSize) {

	      // "Synchronizing files with nameserver and stager catalog"
              castor::dlf::Param params[] =
                {castor::dlf::Param("Directory", *it),
                 castor::dlf::Param("NbFiles", diskCopyIds[fid.first].size()),
                 castor::dlf::Param("NameServer", fid.first)};
              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 31, 3, params);

	      synchronizeFiles(fid.first, diskCopyIds[fid.first], paths[fid.first]);
              diskCopyIds[fid.first].clear();
              paths[fid.first].clear();
	      sleep(syncInterval);
	    }
          } catch (castor::exception::Exception e) {
            // Ignore files badly named, they are probably not ours
          }
        }

        // Synchronize the remaining files not yet checked
        for (std::map<std::string, std::vector<u_signed64> >::const_iterator it2 =
               diskCopyIds.begin();
             it2 != diskCopyIds.end();
             it2++) {
          if (it2->second.size() > 0) {
	    // "Synchronizing files with nameserver and stager catalog"
            castor::dlf::Param params[] =
              {castor::dlf::Param("Directory", *it),
               castor::dlf::Param("NbFiles", it2->second.size()),
               castor::dlf::Param("NameServer", it2->first)};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 31, 3, params);

	    synchronizeFiles(it2->first, it2->second, paths[it2->first]);
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
// ReadConfigFile
//-----------------------------------------------------------------------------
void castor::gc::SynchronizationThread::readConfigFile
(unsigned int *syncInterval, unsigned int * chunkSize, bool firstTime)
  throw() {

  // Synchronization interval
  char* value;
  int intervalnew;
  if ((value = getenv("GC_SYNCINTERVAL")) ||
      (value = getconfent("GC", "SyncInterval", 0))) {
    intervalnew = atoi(value);
    if (intervalnew >= 0) {
      if ((unsigned int)intervalnew != *syncInterval) {
	*syncInterval = intervalnew;
	if (!firstTime) {
	  // "New synchronization interval"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("Interval", *syncInterval)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 21, 1, params);
	}
      }
    } else {
      // "Invalid GC/SyncInterval option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", *syncInterval)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 19, 1, params);
    }
  }

  // Chunk size
  int chunkSizenew;
  if ((value = getenv("GC_CHUNKSIZE")) ||
      (value = getconfent("GC", "ChunkSize", 0))) {
    chunkSizenew = atoi(value);
    if (chunkSizenew >= 0) {
      if (*chunkSize != (unsigned int)chunkSizenew) {
	*chunkSize = (unsigned int)chunkSizenew;
	if (!firstTime) {
	  // "New synchronization chunk size"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("ChunkSize", *chunkSize)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 22, 1, params);
	}
      }
    } else {
      // "Invalid GC/ChunkSize option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", *chunkSize)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 20, 1, params);
    }
  }

  // Logging at start time
  if (firstTime) {
    // "Synchronization configuration"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Interval", *syncInterval),
       castor::dlf::Param("ChunkSize", *chunkSize)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 30, 2, params);
  }
}


//-----------------------------------------------------------------------------
// DiskCopyIdFromFileName
//-----------------------------------------------------------------------------
std::pair<std::string, u_signed64>
castor::gc::SynchronizationThread::diskCopyIdFromFileName(std::string fileName) 
  throw (castor::exception::Exception) {
  
  // Locate the beginning of the nameserver host in the filename, this gives
  // us the fileid
  std::string::size_type p = fileName.find('@', 0);
  if (p == std::string::npos) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse filename : '" << fileName << "'";
    throw e;
  }

  // The fileid should only contain numbers and cannot be empty
  std::string fidStr = fileName.substr(0, p);
  if ((strspn(fidStr.c_str(), "0123456789") != fidStr.length()) ||
      (fidStr.length() <= 0)) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse filename : '" << fileName << "'";
    throw e;
  }
  
  // Now extract the nameserver host, everything up to the '.'
  std::string::size_type q = fileName.find('.', p + 1);
  if (q == std::string::npos) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse filename : '" << fileName << "'";
    throw e;
  }
  std::string nsHost = fileName.substr(p + 1, q - p -1);

  // Now the .diskcopyid, again only numbers and cannot be empty
  std::string dcIdStr = fileName.substr(q + 1, fileName.length());
  if ((strspn(dcIdStr.c_str(), "0123456789") != dcIdStr.length()) ||
      (dcIdStr.length() <= 0)) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse filename : '" << fileName << "'";
    throw e;      
  }
  u_signed64 dcId = atoll(dcIdStr.c_str());

  return std::pair<std::string, u_signed64>(nsHost, dcId);
}


//-----------------------------------------------------------------------------
// FileCopyIdFromFilePath
//-----------------------------------------------------------------------------
u_signed64
castor::gc::SynchronizationThread::fileIdFromFilePath(std::string filePath) 
  throw (castor::exception::Exception) {

  // Extract the filename
  std::string::size_type f = filePath.find_last_of('/', filePath.length());
  if (f == std::string::npos) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse filepath : '" << filePath << "'";
    throw e;
  }
  std::string fileName = filePath.substr(f + 1, filePath.length());

  // Locate the beginning of the nameserver host in the filename, this gives
  // us the fileid
  std::string::size_type p = fileName.find('@', 0);
  if (p == std::string::npos) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse filename : '" << fileName << "'";
    throw e;
  }

  // The fileid should only contain numbers and cannot be empty
  std::string fidStr = fileName.substr(0, p);
  if ((strspn(fidStr.c_str(), "0123456789") != fidStr.length()) ||
      (fidStr.length() <= 0)) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to parse filename : '" << fileName << "'";
    throw e;
  }

  return atoll(fidStr.c_str());
}


//-----------------------------------------------------------------------------
// synchronizeFiles
//-----------------------------------------------------------------------------
void castor::gc::SynchronizationThread::synchronizeFiles
(std::string nameServer,
 const std::vector<u_signed64> &diskCopyIds,
 const std::map<u_signed64, std::string> &paths) throw() {

  // Make a copy of the disk copy id and file path containers so that they can 
  // be modified safely
  std::vector<u_signed64> dcIds = diskCopyIds;
  std::map<u_signed64, std::string> filePaths = paths;

  // Get RemoteGCSvc
  castor::IService* svc =
    castor::BaseObject::services()->
    service("RemoteGCSvc", castor::SVC_REMOTEGCSVC);
  if (0 == svc) {
    // "Could not get RemoteStagerSvc"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "SynchronizationThread::synchronizeFiles")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params);
    return;
  }

  castor::stager::IGCSvc *gcSvc =
    dynamic_cast<castor::stager::IGCSvc*>(svc);
  if (0 == gcSvc) {
    // "Got a bad RemoteStagerSvc"
    castor::dlf::Param params[] =
      {castor::dlf::Param("ID", svc->id()),
       castor::dlf::Param("Name", svc->name()),
       castor::dlf::Param("Function", "SynchronizationThread::synchronizeFiles")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 3, params);
    return;
  }

  // Synchronize the diskcopys with the stager catalog
  std::vector<u_signed64> orphans = gcSvc->stgFilesDeleted(dcIds);

  // Remove orphaned files
  Cns_fileid fileId;
  memset(&fileId, 0, sizeof(fileId));
  for (std::vector<u_signed64>::const_iterator it = orphans.begin();
       it != orphans.end();
       it++) {
    fileId.fileid = fileIdFromFilePath(filePaths.find(*it)->second);
    
    // "Deleting local file which is no longer in the stager catalog"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Filename", filePaths.find(*it)->second)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 36, 1, params, &fileId);
    
    if (unlink(filePaths.find(*it)->second.c_str()) < 0) {
      if (errno != ENOENT) {
	// "Deletion of orphaned local file failed"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Filename", filePaths.find(*it)->second),
	   castor::dlf::Param("Error", strerror(errno))};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28, 2, params, &fileId);
      }
    }

    // Remove the file from the file paths map
    filePaths.erase(filePaths.find(*it));
  }

  // "Summary of files removed by stager synchronization"
  if (orphans.size()) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("NbOrphanFiles", orphans.size())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_MONITORING, 33, 1, params);
  }

  // During the unlinking of the files in the diskserver to stager catalog
  // synchronization we removed the orphaned files from the file path map. As
  // a result the file path map now only contains those files which need to 
  // be checked against the nameserver
  if (filePaths.size() == 0) {
    return;
  }

  // Create an array of 64bit unsigned integers to pass to the Cns_bulkexist
  // call
  u_signed64 *cnsFileIds = (u_signed64 *) malloc(filePaths.size() * sizeof(u_signed64));
  if (cnsFileIds == 0){
    // "Memory allocation failure"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "synchronizeFiles"),
       castor::dlf::Param("Message", strerror(errno)) };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 29, 2, params);   
  }

  // Populate the array with fileids
  int i = 0;
  std::map<u_signed64, std::string> cnsFilePaths;
  for (std::map<u_signed64, std::string>::iterator it = filePaths.begin();
       it != filePaths.end();
       it++, i++) {
    u_signed64 fid = fileIdFromFilePath(it->second);
    cnsFilePaths[fid] = it->second;
    cnsFileIds[i] = fid;
  }

  // Call the nameserver to determine which files have been deleted
  int nbFids = cnsFilePaths.size();
  int rc = Cns_bulkexist(nameServer.c_str(), cnsFileIds, &nbFids);
  if (rc != 0) {
    // "Error calling nameserver function Cns_bulkexist"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Nameserver", nameServer),
       castor::dlf::Param("Message", sstrerror(serrno))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 32, 2, params);
    free(cnsFileIds);
    return;
  }

  // No files to be deleted
  if (nbFids == 0) {
    free(cnsFileIds);
    return;
  }

  // Put the returned deleted files into a vector
  std::vector<u_signed64> delFileIds;
  for (i = 0; i < nbFids; i++) {
    delFileIds.push_back(cnsFileIds[i]);
  }
  free(cnsFileIds);

  // Notify the stager to the deletion of the orphaned files
  orphans = gcSvc->nsFilesDeleted(delFileIds, nameServer);

  // Remove orphaned files
  strncpy(fileId.server, nameServer.c_str(), sizeof(fileId.server));
  for (std::vector<u_signed64>::const_iterator it = orphans.begin();
       it != orphans.end();
       it++) {
    fileId.fileid = fileIdFromFilePath(cnsFilePaths.find(*it)->second);
    
    // "Deleting local file which is no longer in the nameserver"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Filename", cnsFilePaths.find(*it)->second)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 27, 1, params, &fileId);
    
    if (unlink(cnsFilePaths.find(*it)->second.c_str()) < 0) {
      if (errno != ENOENT) {
	// "Deletion of orphaned local file failed"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Filename", cnsFilePaths.find(*it)->second),
	   castor::dlf::Param("Error", strerror(errno))};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28, 2, params, &fileId);
      }
    }

    printf("unlink: %s\n", cnsFilePaths.find(*it)->second.c_str());
  }

  // "Summary of files removed by nameserver synchronization"
  if (orphans.size()) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("NbOrphanFiles", orphans.size())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_MONITORING, 35, 1, params);
  }
}
