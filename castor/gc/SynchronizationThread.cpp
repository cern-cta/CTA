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
 * @(#)$RCSfile: SynchronizationThread.cpp,v $ $Revision: 1.22 $ $Release$ $Date: 2009/08/18 09:42:51 $ $Author: waldron $
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
#define DEFAULT_SYNCINTERVAL       1800
#define DEFAULT_CHUNKINTERVAL      120
#define DEFAULT_CHUNKSIZE          2000
#define DEFAULT_DISABLESTAGERSYNC  false


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::gc::SynchronizationThread::SynchronizationThread(int startDelay) :
  m_startDelay(startDelay) { };


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::gc::SynchronizationThread::run(void *param) {

  // "Starting synchronization thread"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 18);
  sleep(m_startDelay);

  // Get the synchronization interval and chunk size
  unsigned int syncInterval = DEFAULT_SYNCINTERVAL;
  unsigned int chunkInterval = DEFAULT_CHUNKINTERVAL;
  unsigned int chunkSize = DEFAULT_CHUNKSIZE;
  bool disableStagerSync = DEFAULT_DISABLESTAGERSYNC;
  readConfigFile(&syncInterval, &chunkInterval, &chunkSize,
                 &disableStagerSync, true);

  // Endless loop
  for (;;) {

    // Get the synchronization interval and chunk size these may have changed
    // since the last iteration
    readConfigFile(&syncInterval, &chunkInterval,
                   &chunkSize, &disableStagerSync);
    if (syncInterval <= 0) {
      sleep(300);
      return;
    }

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

    std::map<std::string, std::vector<u_signed64> > diskCopyIds;
    std::map<std::string, std::map<u_signed64, std::string> > paths;

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
	struct stat64 file;
	std::ostringstream filepath;
	filepath << fs[fsIt] << dir->d_name;
	if (stat64(filepath.str().c_str(), &file) < 0) {
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
	while ((file = readdir(files))) {

          // Ignore non regular files
	  struct stat64 filebuf;
	  std::string filepath (*it + "/" + file->d_name);
	  if (stat64(filepath.c_str(), &filebuf) < 0) {
	    continue;
	  } else if (!(filebuf.st_mode & S_IFREG)) {
	    continue;  // not a file
	  }

	  // Extract the nameserver host and diskcopy id from the filename
	  std::pair<std::string, u_signed64> fid;
	  try {
	    fid = diskCopyIdFromFileName(file->d_name);
	  } catch (castor::exception::Exception e) {
	    // "Ignoring filename that does not conform to castor naming
	    // conventions"
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Filename", file->d_name)};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 39, 1, params);
	    continue;
	  }

          try {
	    diskCopyIds[fid.first].push_back(fid.second);
            paths[fid.first][fid.second] = *it + "/" + file->d_name;

	    // In the case of a large number of files, synchronize them in
	    // chunks so to not overwhelming central services
	    if (diskCopyIds[fid.first].size() >= chunkSize) {

	      // "Synchronizing files with nameserver and stager catalog"
              castor::dlf::Param params[] =
                {castor::dlf::Param("NbFiles", diskCopyIds[fid.first].size()),
                 castor::dlf::Param("Nameserver", fid.first)};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 31, 2, params);

	      synchronizeFiles(fid.first, diskCopyIds[fid.first],
                               paths[fid.first], disableStagerSync);
	      diskCopyIds[fid.first].clear();
	      paths[fid.first].clear();
	      sleep(chunkInterval);
	    }
	  } catch (castor::exception::Exception e) {
	    // "Unexpected exception caught in synchronizeFiles"
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 40, 0, 0);
	    sleep(chunkInterval);
	  }
	}
	closedir(files);
      }
      free(fs[fsIt]);
      fsIt = (fsIt + 1) % nbFs;
    }

    // Synchronize the remaining files not yet checked
    for (std::map<std::string, std::vector<u_signed64> >::const_iterator it2 =
	   diskCopyIds.begin();
	 it2 != diskCopyIds.end();
	 it2++) {
      try {
	if (it2->second.size() > 0) {
	  // "Synchronizing files with nameserver and stager catalog"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("NbFiles", it2->second.size()),
	     castor::dlf::Param("Nameserver", it2->first)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 31, 2, params);

	  synchronizeFiles(it2->first, it2->second,
                           paths[it2->first], disableStagerSync);
	  sleep(chunkInterval);
	}
      } catch (castor::exception::Exception e) {
	// "Unexpected exception caught in synchronizeFiles"
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 40, 0, 0);
	sleep(chunkInterval);
      }
    }

    free(fs);
    sleep(syncInterval);
  }
}


//-----------------------------------------------------------------------------
// ReadConfigFile
//-----------------------------------------------------------------------------
void castor::gc::SynchronizationThread::readConfigFile
(unsigned int *syncInterval,
 unsigned int *chunkInterval,
 unsigned int *chunkSize,
 bool *disableStagerSync,
 bool firstTime)
  throw(castor::exception::Exception) {

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
      *syncInterval = DEFAULT_SYNCINTERVAL;
      // "Invalid GC/SyncInterval option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", *syncInterval)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 19, 1, params);
    }
  }

  // Chunk interval
  if ((value = getenv("GC_CHUNKINTERVAL")) ||
      (value = getconfent("GC", "ChunkInterval", 0))) {
    intervalnew = atoi(value);
    if (intervalnew >= 0) {
      if ((unsigned int)intervalnew != *chunkInterval) {
	*chunkInterval = intervalnew;
	if (!firstTime) {
	  // "New chunk interval"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("Interval", *chunkInterval)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 37, 1, params);
	}
      }
    } else {
      *chunkInterval = DEFAULT_CHUNKINTERVAL;
      // "Invalid GC/ChunkInterval option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", *chunkInterval)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 38, 1, params);
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
      *chunkSize = DEFAULT_CHUNKSIZE;
      // "Invalid GC/ChunkSize option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", *chunkSize)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 20, 1, params);
    }
  }

  // Disabling of stager synchronization
  if ((value = getenv("GC_DISABLESTAGERSYNC")) ||
      (value = getconfent("GC", "DisableStagerSync", 0))) {
    *disableStagerSync = DEFAULT_DISABLESTAGERSYNC;
    if (!strcasecmp(value, "yes")) {
      *disableStagerSync = true;
    } else if (strcasecmp(value, "no")) {
      castor::exception::Exception e(EINVAL);
      e.getMessage() << "Invalid option for DisableStagerSync: '" << value
                     << "' - must be 'yes' or 'no'" << std::endl;
      throw e;
    }
  }

  // Logging at start time
  if (firstTime) {
    // "Synchronization configuration"
    castor::dlf::Param params[] =
      {castor::dlf::Param("SyncInterval", *syncInterval),
       castor::dlf::Param("ChunkInterval", *chunkInterval),
       castor::dlf::Param("ChunkSize", *chunkSize)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 30, 3, params);
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

  // Now extract the nameserver host, everything up to the last '.'
  std::string::size_type q = fileName.find_last_of('.', fileName.length());
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
// FileIdFromFilePath
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
 const std::map<u_signed64, std::string> &paths,
 bool disableStagerSync) throw() {

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

  Cns_fileid fileId;
  memset(&fileId, 0, sizeof(fileId));
  strncpy(fileId.server, nameServer.c_str(), sizeof(fileId.server));
  u_signed64 nbOrphanFiles = 0;
  u_signed64 spaceFreed = 0;
  std::vector<u_signed64> orphans;

  // Synchronize the diskcopys with the stager catalog if needed
  if (!disableStagerSync) {
    orphans = gcSvc->stgFilesDeleted(dcIds, nameServer);

  // Remove orphaned files
    for (std::vector<u_signed64>::const_iterator it = orphans.begin();
         it != orphans.end();
         it++) {
      fileId.fileid = fileIdFromFilePath(filePaths.find(*it)->second);

      // Get information about the file before unlinking
      struct stat64 fileinfo;
      if (stat64(filePaths.find(*it)->second.c_str(), &fileinfo) < 0) {
        if (errno != ENOENT) {
          // "Failed to stat file"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Filename", filePaths.find(*it)->second),
             castor::dlf::Param("Error", strerror(errno))};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 41, 2, params, &fileId);
        }
      }

      if (unlink(filePaths.find(*it)->second.c_str()) < 0) {
        if (errno != ENOENT) {
          // "Deletion of orphaned local file failed"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Filename", filePaths.find(*it)->second),
             castor::dlf::Param("Error", strerror(errno))};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28, 2, params, &fileId);
        }
      } else {

        // "Deleting local file which is no longer in the stager catalog"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Filename", filePaths.find(*it)->second),
           castor::dlf::Param("FileSize", (u_signed64)fileinfo.st_size),
           castor::dlf::Param("FileAge", time(NULL) - fileinfo.st_ctime)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 36, 3, params, &fileId);
        spaceFreed += fileinfo.st_size;
        nbOrphanFiles++;
      }

      // Remove the file from the file paths map
      filePaths.erase(filePaths.find(*it));
    }

    // "Summary of files removed by stager synchronization"
    if (nbOrphanFiles) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("NbOrphanFiles", nbOrphanFiles),
         castor::dlf::Param("SpaceFreed", spaceFreed)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 33, 2, params);
    }

    // During the unlinking of the files in the diskserver to stager catalog
    // synchronization we removed the orphaned files from the file path map. As
    // a result the file path map now only contains those files which need to
    // be checked against the nameserver
    if (filePaths.size() == 0) {
      return;
    }

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
  spaceFreed = 0, nbOrphanFiles = 0;
  for (std::vector<u_signed64>::const_iterator it = orphans.begin();
       it != orphans.end();
       it++) {
    fileId.fileid = fileIdFromFilePath(cnsFilePaths.find(*it)->second);

    // Get information about the file before unlinking
    struct stat64 fileinfo;
    if (stat64(cnsFilePaths.find(*it)->second.c_str(), &fileinfo) < 0) {
      if (errno != ENOENT) {
	// "Failed to stat file"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Filename", cnsFilePaths.find(*it)->second),
	   castor::dlf::Param("Error", strerror(errno))};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 41, 2, params, &fileId);
      }
    }

    if (unlink(cnsFilePaths.find(*it)->second.c_str()) < 0) {
      if (errno != ENOENT) {
	// "Deletion of orphaned local file failed"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Filename", cnsFilePaths.find(*it)->second),
	   castor::dlf::Param("Error", strerror(errno))};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28, 2, params, &fileId);
      }
    } else {
      // "Deleting local file which is no longer in the nameserver"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Filename", cnsFilePaths.find(*it)->second),
	 castor::dlf::Param("FileSize", (u_signed64)fileinfo.st_size),
	 castor::dlf::Param("FileAge", time(NULL) - fileinfo.st_ctime)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 27, 3, params, &fileId);
      spaceFreed += fileinfo.st_size;
      nbOrphanFiles++;
    }
  }

  // "Summary of files removed by nameserver synchronization"
  if (nbOrphanFiles) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("NbOrphanFiles", nbOrphanFiles),
       castor::dlf::Param("SpaceFreed", spaceFreed)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 35, 2, params);
  }

  // List all files that will be scheduled for later deletion using the normal
  // selectFile2Delete logic
  for (std::vector<u_signed64>::const_iterator it = delFileIds.begin();
       it != delFileIds.end();
       it++) {

    // Exclude those files which we already removed above i.e. those which
    // aren't in the stager catalogue
    std::vector<u_signed64>::const_iterator it2 =
      std::find(orphans.begin(), orphans.end(), *it);
    if (it2 != orphans.end()) {
      continue;
    }
    fileId.fileid = fileIdFromFilePath(cnsFilePaths.find(*it)->second);

    // "File scheduled for deletion as the file no longer exists in the
    // nameserver but still exists in the stager"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Filename", cnsFilePaths.find(*it)->second)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 42, 1, params, &fileId);

  }
}
