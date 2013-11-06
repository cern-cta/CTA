/******************************************************************************
 *                      DeletionThread.cpp
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
 *
 * Deletion thread used to check periodically whether files need to be deleted
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/gc/DeletionThread.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/System.hpp"
#include "getconfent.h"

#include <vector>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <stdlib.h>

// Definitions
#define DEFAULT_GCINTERVAL 300


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::gc::DeletionThread::DeletionThread(int startDelay):
  m_interval(DEFAULT_GCINTERVAL),
  m_startDelay(startDelay) {};


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::gc::DeletionThread::run(void*) {

  // "Starting deletion thread"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 1);
  sleep(m_startDelay);

  // Get RemoteGCSvc
  castor::IService* svc =
    castor::BaseObject::services()->
    service("RemoteGCSvc", castor::SVC_REMOTEGCSVC);
  if (0 == svc) {
    // "Could not get RemoteStagerSvc"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "DeletionThread::run")};
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
       castor::dlf::Param("Function", "DeletionThread::run")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 3, params);
    return;
  }

  // Determine the name of the diskserver
  try {
    m_diskServerName = castor::System::getHostName();
  } catch (castor::exception::Exception& e) {
    // "Exception caught trying to getHostName"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Code", strerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17, 2, params);
    return;
  }

  // Get the GC Interval
  char *value = 0;
  if ((value = getenv("GC_INTERVAL")) ||
      (value = getconfent("GC", "Interval", 0))) {
    m_interval = atoi(value);
  }

  // Endless loop
  for (;;) {
    // "Checking for garbage"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 6);

    // Get new sleep interval if the environment has been changed
    int intervalnew;
    if ((value = getenv("GC_INTERVAL")) ||
	(value = getconfent("GC", "Interval", 0))) {
      intervalnew = atoi(value);
      if (intervalnew != m_interval) {
	m_interval = intervalnew;

	// "Sleep interval changed"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Interval", m_interval)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 7, 1, params);
      }
    }

    // Retrieve list of files to delete
    std::vector<castor::stager::GCLocalFile*>* files2Delete = 0;
    try {
      files2Delete = gcSvc->selectFiles2Delete(m_diskServerName);
    } catch (castor::exception::Exception& e) {

      // "Error caught while looking for garbage files"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", sstrerror(e.code())),
	 castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 8, 2, params);

      // "Sleeping"
      castor::dlf::Param params2[] =
        {castor::dlf::Param("Duration", m_interval)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 1, params2);
      sleep(m_interval);
      continue;
    }

    // Delete files
    if (0 < files2Delete->size()) {

      // "Found files to garbage collect. Starting removal"
      castor::dlf::Param params[] =
        {castor::dlf::Param("NbFiles", files2Delete->size())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 10, 1, params);

      // Loop over the files and delete them
      std::vector<u_signed64*> deletedFiles;
      std::vector<u_signed64*> failedFiles;
      u_signed64 removedsize  = 0;
      long filestotal   = 0;
      long filesfailed  = 0;
      long filesremoved = 0;
      for(std::vector<castor::stager::GCLocalFile*>::iterator it =
            files2Delete->begin();
          it != files2Delete->end();
          it++) {

	// Construct Cns invariant
	Cns_fileid fileId;
	memset(&fileId, '\0', sizeof(fileId));
	fileId.fileid = (*it)->fileId();
	strncpy(fileId.server, (*it)->nsHost().c_str(), sizeof(fileId.server));

        try {
          filestotal++;
	  u_signed64 filesize;
	  u_signed64 fileage;
	  gcRemoveFilePath((*it)->fileName(), filesize, fileage);
          removedsize += filesize;
          filesremoved++;

          // "Removed file successfully"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Filename", (*it)->fileName()),
             castor::dlf::Param("FileSize", filesize),
	     castor::dlf::Param("FileAge", fileage),
	     castor::dlf::Param("LastAccessTime", (*it)->lastAccessTime()),
	     castor::dlf::Param("NbAccesses", (*it)->nbAccesses()),
	     castor::dlf::Param("GcWeight", (*it)->gcWeight()),
	     castor::dlf::Param("GcType", (*it)->gcTriggeredBy()),
	     castor::dlf::Param("SvcClass", (*it)->svcClassName())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 11, 8, params, &fileId);

	  // Add the file to the list of deleted ones
          u_signed64 *fileid = new u_signed64((*it)->diskCopyId());
          deletedFiles.push_back(fileid);
        } catch (castor::exception::Exception& e) {
          filesfailed++;
          // "Failed to remove file"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Filename", (*it)->fileName()),
             castor::dlf::Param("Message", e.getMessage().str()),
             castor::dlf::Param("Type", strerror(e.code()))};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 12, 3, params, &fileId);

	  // Add the file to the list of failed ones
          u_signed64 *fileid = new u_signed64((*it)->diskCopyId());
          failedFiles.push_back(fileid);
        }
      } // End of delete files loop

      // Log to DLF
      if (0 < filestotal) {

        // "Summary of files removed"
        castor::dlf::Param params[] =
          {castor::dlf::Param("FilesRemoved", filesremoved),
           castor::dlf::Param("FailedFiles", filesfailed),
           castor::dlf::Param("TotalFiles", filestotal),
           castor::dlf::Param("SpaceFreed", removedsize)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 13, 4, params);
      }

      // Inform stager of the deletion/failure
      if (deletedFiles.size() > 0) {
        try {
          gcSvc->filesDeleted(deletedFiles);
        } catch (castor::exception::Exception& e) {
          // "Error caught while informing stager..."
          castor::dlf::Param params[] =
            {castor::dlf::Param("Code", sstrerror(e.code())),
	     castor::dlf::Param("Error", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 2, params);
        }
        // Release memory
        for (std::vector<u_signed64*>::iterator it =
               deletedFiles.begin();
             it != deletedFiles.end();
             it++) {
          delete *it;
        }
      }
      if (failedFiles.size() > 0) {
        try {
          gcSvc->filesDeletionFailed(failedFiles);
        } catch (castor::exception::Exception& e) {
          // "Error caught while informing stager..."
          castor::dlf::Param params[] =
            {castor::dlf::Param("Code", sstrerror(e.code())),
	     castor::dlf::Param("Error", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 2, params);
        }
        // Release memory
        for (std::vector<u_signed64*>::iterator it =
               failedFiles.begin();
             it != failedFiles.end();
             it++) {
          delete *it;
        }
      }
    } else {
      // "No garbage files found"
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 15);
    }

    // "Sleeping"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("Duration", m_interval)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 1, params2);
    sleep(m_interval);
  } // End of loop
}


//-----------------------------------------------------------------------------
// gcRemoveFilePath
//-----------------------------------------------------------------------------
void castor::gc::DeletionThread::gcRemoveFilePath
(std::string filepath, u_signed64 &filesize, u_signed64 &fileage)
  throw (castor::exception::Exception) {
  struct stat64 fileinfo;
  if (::stat64(filepath.c_str(), &fileinfo) ) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to stat file " << filepath;
    throw e;
  }
  filesize = fileinfo.st_size;
  fileage  = time(NULL) - fileinfo.st_ctime;
  if (unlink(filepath.c_str()) < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to unlink file " << filepath;
    throw e;
  }
}
