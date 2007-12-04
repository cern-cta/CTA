/******************************************************************************
 *                      UpdateThread.cpp
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
 * @(#)$RCSfile: UpdateThread.cpp,v $ $Author: waldron $
 *
 * The Update thread of the RmMaster daemon. It receives updates from the
 * stager about openings and closings of streams and updates the memory
 * representation accordingly
 *
 * @author Giulia Taurelli
 *****************************************************************************/

// Include files
#include "castor/monitoring/rmmaster/UpdateThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/monitoring/StreamReport.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/Constants.hpp"
#include "castor/System.hpp"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::UpdateThread::UpdateThread
(castor::monitoring::ClusterStatus* clusterStatus) :
  m_clusterStatus(clusterStatus) {
  // "Update thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 34, 0, 0);
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::monitoring::rmmaster::UpdateThread::run(void* par) throw() {

  try {

    // "Update thread running"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 19, 0, 0);

    // Get the report
    castor::IObject* obj = (castor::IObject*) par;

    // handle it
    try {
      if (OBJ_StreamReport == obj->type()) {
        castor::monitoring::StreamReport* sr =
          dynamic_cast<castor::monitoring::StreamReport*>(obj);
        handleStreamReport(sr);
      } else {

        // "Received unknown object from socket"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Type", obj->type())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 1, params);
      }
    } catch (castor::exception::Exception e) {

      // "Caught exception in UpdateThread"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", sstrerror(e.code())),
	 castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 20, 2, params);
    }
    // Cleanup
    delete obj;
  } catch (...) {
    // "General exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "UpdateThread::run")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17, 1, params);
  }
}


//-----------------------------------------------------------------------------
// HandleStreamReport
//-----------------------------------------------------------------------------
void castor::monitoring::rmmaster::UpdateThread::handleStreamReport
(castor::monitoring::StreamReport* report)
  throw (castor::exception::Exception) {

  // If operating in slave mode do nothing!
  try {
    std::string masterName =
      castor::monitoring::rmmaster::LSFSingleton::getInstance()->
      getLSFMasterName();
    std::string hostName = castor::System::getHostName();
    if (masterName != hostName) {
      return;
    }
  } catch (castor::exception::Exception e) {

    // "Failed to determine the hostname of the LSF master"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Function", "UpdateThread::run")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 46, 3, params);
    return;
  }

  // Cast normal string into sharedMemory one in order to be able to search for
  // it in the ClusterStatus map. This is safe because the difference in the
  // types is only the allocator and because the 2 allocators have identical
  // members
  std::string machineName = report->diskServerName();
  castor::monitoring::SharedMemoryString *smMachineName =
    (castor::monitoring::SharedMemoryString*)(&machineName);
  // Find the diskServer
  castor::monitoring::ClusterStatus::iterator it =
    m_clusterStatus->find(*smMachineName);
  if (it == m_clusterStatus->end()) {
    // DiskServer does not yet exist, ignore report
    return;
  }

  // Cast normal string into sharedMemory one in order to be able to search for
  // it in the DiskServerStatus map. This is safe because the difference in the
  // types is only the allocator and because the 2 allocators have identical
  // members
  std::string mountPoint = report->mountPoint();
  castor::monitoring::SharedMemoryString *smMountPoint =
    (castor::monitoring::SharedMemoryString*)(&mountPoint);
  // Find the FileSystem
  castor::monitoring::DiskServerStatus::iterator it2 =
    it->second.find(*smMountPoint);
  if (it2 == it->second.end()) {
    // FileSystem does not yet exist, ignore report
    return;
  }

  // Update FileSystem's number of streams
  int delta = report->created() ? 1 : -1;
  switch (report->direction()) {
  case castor::monitoring::STREAMDIRECTION_READ:
    it2->second.setNbReadStreams(it2->second.nbReadStreams() + delta);
    break;
  case castor::monitoring::STREAMDIRECTION_WRITE:
    it2->second.setNbWriteStreams(it2->second.nbWriteStreams() + delta);
    break;
  case castor::monitoring::STREAMDIRECTION_READWRITE:
    it2->second.setNbReadWriteStreams(it2->second.nbReadWriteStreams() + delta);
    break;
  }
}
