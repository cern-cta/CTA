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
 * @(#)$RCSfile: UpdateThread.cpp,v $ $Author: sponcec3 $
 *
 * The Update thread of the RmMaster daemon.
 * It receives updates from the stager about openings and closings
 * of streams and updates the memory representation accordingly
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/monitoring/rmmaster/UpdateThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/monitoring/StreamReport.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/Constants.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmmaster::UpdateThread::UpdateThread
(castor::monitoring::ClusterStatus* clusterStatus) :
  m_clusterStatus(clusterStatus) {
  // "Update thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 10);
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::UpdateThread::run(void* par) throw() {
  try { // no exception should go out
    // "Thread Update started"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 19);
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
      // "Caught exception in UpdateThread" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 20, 1, params);
    }
    // free memory
    delete obj;
  } catch (...) {
    // "General exception caught"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17);
  }
}

//------------------------------------------------------------------------------
// handleStreamReport
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::UpdateThread::handleStreamReport
(castor::monitoring::StreamReport* report)
  throw (castor::exception::Exception) {
  // Find the diskServer
  const castor::monitoring::SharedMemoryString
    smMachineName(report->diskServerName().c_str());
  castor::monitoring::ClusterStatus::iterator it =
    m_clusterStatus->find(smMachineName);
  if (it == m_clusterStatus->end()) {
    // DiskServer does not yet exist, ignore report
    return;
  }
  // Find the FileSystem
  const castor::monitoring::SharedMemoryString smMountPoint
    (report->mountPoint().c_str());
  castor::monitoring::DiskServerStatus::iterator it2 =
    it->second.find(smMountPoint);
  if (it2 == it->second.end()) {
    // FileSystem does not yet exist, ignore report
    return;
  }
  // Update FileSystem nb of streams
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
