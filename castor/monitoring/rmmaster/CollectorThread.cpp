/******************************************************************************
 *                      CollectorThread.cpp
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
 * @(#)$RCSfile$ $Author$
 *
 * The Collector thread of the RmMaster daemon.
 * It collects the data from the different nodes and updates a shared
 * memory representation of it
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/monitoring/rmmaster/CollectorThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/monitoring/DiskServerMetricsReport.hpp"
#include "castor/monitoring/FileSystemMetricsReport.hpp"
#include "castor/monitoring/DiskServerStateReport.hpp"
#include "castor/monitoring/FileSystemStateReport.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/MessageAck.hpp"
#include "castor/Constants.hpp"
#include <time.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmmaster::CollectorThread::CollectorThread
(castor::monitoring::ClusterStatus* clusterStatus) :
  m_clusterStatus(clusterStatus) {
  // "Collector thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 10);
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::run(void* par) throw() {
  try { // no exception should go out
    // "Thread Collector started"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9);
    // Get the server socket
    castor::io::ServerSocket* sock = (castor::io::ServerSocket*) par;
    // prepare the ackowledgement
    castor::MessageAck ack;
    ack.setStatus(true);
    // get the incoming object
    castor::IObject* obj = 0;
    try {
      obj = sock->readObject();
    } catch (castor::exception::Exception e) {
      // "Unable to read Object from socket" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 12, 1, params);
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      std::ostringstream stst;
      stst << "Unable to read Request object from socket."
           << std::endl << e.getMessage().str();
      ack.setErrorMessage(stst.str());
    }
    // handle it
    if (ack.status()) {
      try {
        if (OBJ_DiskServerStateReport == obj->type()) {
          castor::monitoring::DiskServerStateReport* dss =
            dynamic_cast<castor::monitoring::DiskServerStateReport*>(obj);
          handleStateUpdate(dss);
        } else if (OBJ_DiskServerMetricsReport == obj->type()) {
          castor::monitoring::DiskServerMetricsReport* dss =
            dynamic_cast<castor::monitoring::DiskServerMetricsReport*>(obj);
          handleMetricsUpdate(dss);
        } else {
          // "Received unknown object from socket"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Type", obj->type())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 1, params);
          ack.setStatus(false);
          ack.setErrorCode(EINVAL);
          std::ostringstream stst;
          stst << "Received unknown object from socket."
               << std::endl << "Type was " << obj->type();
          ack.setErrorMessage(stst.str());
        }
      } catch (castor::exception::Exception e) {
        // "Caught exception in CollectorThread" message
        castor::dlf::Param params[] =
          {castor::dlf::Param("Message", e.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 12, 1, params);
        ack.setStatus(false);
        ack.setErrorCode(e.code());
        std::ostringstream stst;
        stst << "Caught exception in CollectorThread"
             << std::endl << e.getMessage().str();
        ack.setErrorMessage(stst.str());
      }
    }
    // "Sending acknowledgement to client" message
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 15);
    try {
      sock->sendObject(ack);
    } catch (castor::exception::Exception e) {
      // "Unable to send Ack to client" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Standard Message", sstrerror(e.code())),
         castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 2, params);
    }
  } catch (...) {
    // "General exception caught"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17);
  }
}

//------------------------------------------------------------------------------
// handleStateUpdate
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::handleStateUpdate
(castor::monitoring::DiskServerStateReport* state)
  throw (castor::exception::Exception) {
  // Take care of DiskServer creation
  const castor::monitoring::SharedMemoryString
    smMachineName(state->name().c_str());
  castor::monitoring::ClusterStatus::iterator it =
    m_clusterStatus->find(smMachineName);
  if (it == m_clusterStatus->end()) {
    it = m_clusterStatus->insert
      (std::make_pair(smMachineName,
                      castor::monitoring::DiskServerStatus())).first;
  }
  // update DiskServer status
  it->second.setRam(state->ram());
  it->second.setMemory(state->memory());
  it->second.setSwap(state->swap());
  // Update status if needed
  if (it->second.adminStatus() != ADMIN_FORCE ||
      state->adminStatus() != ADMIN_NONE) {
    it->second.setStatus(state->status());
    if (state->adminStatus() == ADMIN_FORCE) {
      it->second.setAdminStatus(ADMIN_FORCE);
    } else {
      it->second.setAdminStatus(ADMIN_NONE);
    }
  }
  // Update lastUpdate
  it->second.setLastStateUpdate(time(0));
  // Update FileSystems
  for (std::vector<castor::monitoring::FileSystemStateReport*>::const_iterator
         itFs = state->FileSystemStatesReports().begin();
       itFs != state->FileSystemStatesReports().end();
       itFs++) {
    // Take care of the FileSystem creation
    const castor::monitoring::SharedMemoryString smMountPoint
      ((*itFs)->mountPoint().c_str());
    castor::monitoring::DiskServerStatus::iterator it2 =
      it->second.find(smMountPoint);
    if (it2 == it->second.end()) {
      it2 = it->second.insert
        (std::make_pair
         (smMountPoint,
          castor::monitoring::FileSystemStatus())).first;
    }
    // Update FileSystem status
    it2->second.setSpace((*itFs)->space());
    // Update status if needed
    if (it2->second.adminStatus() != ADMIN_FORCE ||
        (*itFs)->adminStatus() != ADMIN_NONE) {
      it2->second.setStatus((*itFs)->status());
      if ((*itFs)->adminStatus() == ADMIN_FORCE) {
        it2->second.setAdminStatus(ADMIN_FORCE);
      } else {
        it2->second.setAdminStatus(ADMIN_NONE);
      }
    }
    // Update lastUpdate
    it2->second.setLastStateUpdate(time(0));
  }
}

//------------------------------------------------------------------------------
// handleMetricsUpdate
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::handleMetricsUpdate
(castor::monitoring::DiskServerMetricsReport* metrics)
  throw (castor::exception::Exception) {
  // Take care of DiskServer creation
  const castor::monitoring::SharedMemoryString
    smMachineName(metrics->name().c_str());
  castor::monitoring::ClusterStatus::iterator it =
    m_clusterStatus->find(smMachineName);
  if (it == m_clusterStatus->end()) {
    it = m_clusterStatus->insert
      (std::make_pair(smMachineName,
                      castor::monitoring::DiskServerStatus())).first;
  }
  // update DiskServer metrics
  it->second.setFreeRam(metrics->freeRam());
  it->second.setFreeMemory(metrics->freeMemory());
  it->second.setFreeSwap(metrics->freeSwap());
  it->second.setLoad(metrics->load());
  // Update lastUpdate
  it->second.setLastMetricsUpdate(time(0));
  // Update FileSystems
  for (std::vector<castor::monitoring::FileSystemMetricsReport*>::const_iterator
         itFs = metrics->fileSystemMetricsReports().begin();
       itFs != metrics->fileSystemMetricsReports().end();
       itFs++) {
    // Take care of the FileSystem creation
    const castor::monitoring::SharedMemoryString smMountPoint
      ((*itFs)->mountPoint().c_str());
    castor::monitoring::DiskServerStatus::iterator it2 =
      it->second.find(smMountPoint);
    if (it2 == it->second.end()) {
      it2 = it->second.insert
        (std::make_pair
         (smMountPoint,
          castor::monitoring::FileSystemStatus())).first;
    }
    // Update FileSystem metrics
    it2->second.setReadRate((*itFs)->readRate());
    it2->second.setWriteRate((*itFs)->writeRate());
    it2->second.setReadStreams((*itFs)->readStreams());
    it2->second.setWriteStreams((*itFs)->writeStreams());
    it2->second.setReadWriteStreams((*itFs)->readWriteStreams());
    it2->second.setFreeSpace((*itFs)->freeSpace());
    // Update lastUpdate
    it2->second.setLastMetricsUpdate(time(0));
  }
}
