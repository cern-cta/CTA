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
 * @author castor-dev team
 *****************************************************************************/

#include "castor/monitoring/rmmaster/CollectorThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/monitoring/DiskServerMetricsReport.hpp"
#include "castor/monitoring/FileSystemMetricsReport.hpp"
#include "castor/monitoring/DiskServerStateReport.hpp"
#include "castor/monitoring/FileSystemStateReport.hpp"
#include "castor/monitoring/admin/DiskServerAdminReport.hpp"
#include "castor/monitoring/admin/FileSystemAdminReport.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/MessageAck.hpp"
#include "castor/Constants.hpp"
#include <time.h>
#include <errno.h>

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
        } else if (OBJ_DiskServerAdminReport == obj->type()) {
          castor::monitoring::admin::DiskServerAdminReport* dss =
            dynamic_cast<castor::monitoring::admin::DiskServerAdminReport*>(obj);
          handleDiskServerAdminUpdate(dss);
        } else if (OBJ_FileSystemAdminReport == obj->type()) {
          castor::monitoring::admin::FileSystemAdminReport* dss =
            dynamic_cast<castor::monitoring::admin::FileSystemAdminReport*>(obj);
          handleFileSystemAdminUpdate(dss);
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
        stst << e.getMessage().str();
        ack.setErrorMessage(stst.str());
      }
    }
    // free memory
    if (0 != obj) delete obj;
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
    // closing and deleting socket
    delete sock;
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
  // Throw away reports with no name cause the build of
  // a shared memory string fails for empty strings. This
  // is due to an optimization inside the string implementation
  // that tries to delay memory allocation when not needed
  // (typically for empty strings). This could be avoided
  // by recompiling libstdc++ with -D_GLIBCXX_FULLY_DYNAMIC_STRING
  // but the default version distributed does not have this
  // The consequence of accepting this is a seg fault in any
  // process attempting to read it (other than the one that
  // created the empty string).
  if (state->name().size() == 0) {
    // "Ignored state report for machine with empty name"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 21);
    return;
  }
  // Take care of DiskServer creation
  castor::monitoring::ClusterStatus::iterator it;
  if (!getOrCreateDiskServer(state->name(), it)) {
    return;
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
    // Throw away reports from filesystems with no name cause the build of
    // a shared memory string fails for empty strings. This
    // is due to an optimization inside the string implementation
    // that tries to delay memory allocation when not needed
    // (typically for empty strings). This could be avoided
    // by recompiling libstdc++ with -D_GLIBCXX_FULLY_DYNAMIC_STRING
    // but the default version distributed does not have this
    // The consequence of accepting this is a seg fault in any
    // process attempting to read it (other than the one that
    // created the empty string).
    if ((*itFs)->mountPoint().size() == 0) {
      // "Ignored state report for filesystem with empty name"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Machine", state->name())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 27, 1, params);
      continue;
    }
    // Take care of the FileSystem creation
    castor::monitoring::DiskServerStatus::iterator it2;
    if (!getOrCreateFileSystem(it->second,
			       (*itFs)->mountPoint(),
			       it2)) {
      return;
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
// getOrCreateDiskServer
//------------------------------------------------------------------------------
bool
castor::monitoring::rmmaster::CollectorThread::getOrCreateDiskServer
(std::string name,
 castor::monitoring::ClusterStatus::iterator& it) throw() {
  // cast normal string into sharedMemory one in order to be able
  // to search for it in the ClusterStatus map.
  // This is safe because the difference in the types is only
  // the allocator and because the 2 allocators have identical
  // members
  castor::monitoring::SharedMemoryString *smName =
    (castor::monitoring::SharedMemoryString*)(&name);
  it = m_clusterStatus->find(*smName);
  if (it == m_clusterStatus->end()) {
    try {
      // here we really have to create a shared memory string,
      // the previous cast won't work since we need to allocate memory
      const castor::monitoring::SharedMemoryString smName2(name.c_str());
      it = m_clusterStatus->insert
	(std::make_pair(smName2,
			castor::monitoring::DiskServerStatus())).first;
    } catch (std::exception e) {
      // "Unable to allocate SharedMemoryString"
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28);
      // not enough shared memory... let's give up
      return false;
    }
  }
  return true;
}

//------------------------------------------------------------------------------
// getOrCreateFileSystem
//------------------------------------------------------------------------------
bool
castor::monitoring::rmmaster::CollectorThread::getOrCreateFileSystem
(castor::monitoring::DiskServerStatus& dss,
 std::string mountPoint,
 castor::monitoring::DiskServerStatus::iterator& it2) throw() {
  // cast normal string into sharedMemory one in order to be able
  // to search for it in the DiskServerStatus map.
  // This is safe because the difference in the types is only
  // the allocator and because the 2 allocators have identical
  // members
  castor::monitoring::SharedMemoryString *smMountPoint =
    (castor::monitoring::SharedMemoryString*)(&mountPoint);
  it2 = dss.find(*smMountPoint);
  if (it2 == dss.end()) {
    try {
      // here we really have to create a shared memory string,
      // the previous cast won't work
      const castor::monitoring::SharedMemoryString smMountPoint2
	(mountPoint.c_str());
      it2 = dss.insert(std::make_pair
		       (smMountPoint2,
			castor::monitoring::FileSystemStatus())).first;
    } catch (std::exception e) {
      // "Unable to allocate SharedMemoryString"
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28);
      // not enough shared memory... let's give up
      return false;
    }
  }
  return true;
}

//------------------------------------------------------------------------------
// handleMetricsUpdate
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::handleMetricsUpdate
(castor::monitoring::DiskServerMetricsReport* metrics)
  throw (castor::exception::Exception) {
  // Throw away reports with no name cause the build of
  // a shared memory string fails for empty strings. This
  // is due to an optimization inside the string implementation
  // that tries to delay memory allocation when not needed
  // (typically for empty strings). This could be avoided
  // by recompiling libstdc++ with -D_GLIBCXX_FULLY_DYNAMIC_STRING
  // but the default version distributed does not have this
  // The consequence of accepting this is a seg fault in any
  // process attempting to read it (other than the one that
  // created the empty string).
  if (metrics->name().size() == 0) {
    // "Ignored metrics report for machine with empty name"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 22);
    return;
  }
  // Take care of DiskServer creation
  castor::monitoring::ClusterStatus::iterator it;
  if (!getOrCreateDiskServer(metrics->name(), it)) {
    return;
  }
  // update DiskServer metrics
  it->second.setFreeRam(metrics->freeRam());
  it->second.setFreeMemory(metrics->freeMemory());
  it->second.setFreeSwap(metrics->freeSwap());
  it->second.setLoad(metrics->load() + it->second.deltaLoad());
  it->second.setDeltaLoad(0);
  // Update lastUpdate
  it->second.setLastMetricsUpdate(time(0));
  // Update FileSystems
  for (std::vector<castor::monitoring::FileSystemMetricsReport*>::const_iterator
         itFs = metrics->fileSystemMetricsReports().begin();
       itFs != metrics->fileSystemMetricsReports().end();
       itFs++) {
    // Throw away reports from filesystems with no name cause the build of
    // a shared memory string fails for empty strings. This
    // is due to an optimization inside the string implementation
    // that tries to delay memory allocation when not needed
    // (typically for empty strings). This could be avoided
    // by recompiling libstdc++ with -D_GLIBCXX_FULLY_DYNAMIC_STRING
    // but the default version distributed does not have this
    // The consequence of accepting this is a seg fault in any
    // process attempting to read it (other than the one that
    // created the empty string).
    if ((*itFs)->mountPoint().size() == 0) {
      // "Ignored metrics report for filesystem with empty name"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Machine", metrics->name())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 26, 1, params);
      continue;
    }
    // Take care of the FileSystem creation
    castor::monitoring::DiskServerStatus::iterator it2;
    if (!getOrCreateFileSystem(it->second,
			       (*itFs)->mountPoint(),
			       it2)) {
      return;
    }
    // Update FileSystem metrics
    it2->second.setReadRate((*itFs)->readRate() + it2->second.deltaReadRate());
    it2->second.setDeltaReadRate(0);
    it2->second.setWriteRate((*itFs)->writeRate() + it2->second.deltaWriteRate());
    it2->second.setDeltaWriteRate(0);
    it2->second.setNbReadStreams((*itFs)->nbReadStreams() + it2->second.deltaNbReadStreams());
    it2->second.setDeltaNbReadStreams(0);
    it2->second.setNbWriteStreams((*itFs)->nbWriteStreams() + it2->second.deltaNbWriteStreams());
    it2->second.setDeltaNbWriteStreams(0);
    it2->second.setNbReadWriteStreams((*itFs)->nbReadWriteStreams() + it2->second.deltaNbReadWriteStreams());
    it2->second.setDeltaNbReadWriteStreams(0);
    it2->second.setFreeSpace((*itFs)->freeSpace() + it2->second.deltaFreeSpace());
    it2->second.setDeltaFreeSpace(0);
    // Update lastUpdate
    it2->second.setLastMetricsUpdate(time(0));
  }
}

//------------------------------------------------------------------------------
// handleDiskServerAdminUpdate
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::handleDiskServerAdminUpdate
(castor::monitoring::admin::DiskServerAdminReport* admin)
  throw (castor::exception::Exception) {
  // Throw away reports with no name cause the build of
  // a shared memory string fails for empty strings. This
  // is due to an optimization inside the string implementation
  // that tries to delay memory allocation when not needed
  // (typically for empty strings). This could be avoided
  // by recompiling libstdc++ with -D_GLIBCXX_FULLY_DYNAMIC_STRING
  // but the default version distributed does not have this
  // The consequence of accepting this is a seg fault in any
  // process attempting to read it (other than the one that
  // created the empty string).
  if (admin->diskServerName().size() == 0) {
    // "Ignored admin diskserver report for machine with empty name"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 23);
    return;
  }
  // cast normal string into sharedMemory one in order to be able
  // to search for it in the ClusterStatus map.
  // This is safe because the difference in the types is only
  // the allocator and because the 2 allocators have identical
  // members
  std::string machineName = admin->diskServerName();
  castor::monitoring::SharedMemoryString *smMachineName =
    (castor::monitoring::SharedMemoryString*)(&machineName);
  // Take care of DiskServer creation
  castor::monitoring::ClusterStatus::iterator it =
    m_clusterStatus->find(*smMachineName);
  if (it == m_clusterStatus->end()) {
    // "Ignored admin diskServer report for unknown machine"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Machine name", admin->diskServerName())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 29, 1, params);
    // inform user via the exception mechanism
    castor::exception::NoEntry e;
    e.getMessage() << "Unknown machine '"
		   << admin->diskServerName()
		   << "'. Please check the name and provide the domain.";
    throw e;
  }
  // Update status if needed
  if (it->second.adminStatus() != ADMIN_FORCE ||
      admin->adminStatus() != ADMIN_NONE) {
    it->second.setStatus(admin->status());
    if (admin->adminStatus() == ADMIN_FORCE) {
      it->second.setAdminStatus(ADMIN_FORCE);
    } else {
      it->second.setAdminStatus(ADMIN_NONE);
    }
  }
}

//------------------------------------------------------------------------------
// handleFileSystemAdminUpdate
//------------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::handleFileSystemAdminUpdate
(castor::monitoring::admin::FileSystemAdminReport* admin)
  throw (castor::exception::Exception) {
  // Throw away reports with no name cause the build of
  // a shared memory string fails for empty strings. This
  // is due to an optimization inside the string implementation
  // that tries to delay memory allocation when not needed
  // (typically for empty strings). This could be avoided
  // by recompiling libstdc++ with -D_GLIBCXX_FULLY_DYNAMIC_STRING
  // but the default version distributed does not have this
  // The consequence of accepting this is a seg fault in any
  // process attempting to read it (other than the one that
  // created the empty string).
  if (admin->diskServerName().size() == 0) {
    // "Ignored admin filesystem report for machine with empty name"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 24);
    return;
  }
  if (admin->mountPoint().size() == 0) {
    // "Ignored admin filesystem report for filesystem with empty name"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 25);
    return;
  }
  // cast normal string into sharedMemory one in order to be able
  // to search for it in the ClusterStatus map.
  // This is safe because the difference in the types is only
  // the allocator and because the 2 allocators have identical
  // members
  std::string machineName = admin->diskServerName();
  castor::monitoring::SharedMemoryString *smMachineName =
    (castor::monitoring::SharedMemoryString*)(&machineName);
  // Find out DiskServer
  castor::monitoring::ClusterStatus::iterator it =
    m_clusterStatus->find(*smMachineName);
  if (it == m_clusterStatus->end()) {
    // "Ignored admin fileSystem report for unknown machine"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Machine name", admin->diskServerName())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 30, 1, params);
    // inform user via the exception mechanism
    castor::exception::NoEntry e;
    e.getMessage() << "Unknown machine '"
		   << admin->diskServerName()
		   << "'. Please check the name and provide the domain.";
    throw e;
  }
  // cast normal string into sharedMemory one in order to be able
  // to search for it in the DiskServerStatus map.
  // This is safe because the difference in the types is only
  // the allocator and because the 2 allocators have identical
  // members
  std::string mountPoint = admin->mountPoint();
  castor::monitoring::SharedMemoryString *smMountPoint =
    (castor::monitoring::SharedMemoryString*)(&mountPoint);
  // Find out FileSystem
  castor::monitoring::DiskServerStatus::iterator it2 =
    it->second.find(*smMountPoint);
  if (it2 == it->second.end()) {
    // "Ignored admin fileSystem report for unknown mountPoint"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Machine name", admin->diskServerName()),
       castor::dlf::Param("MountPoint", admin->mountPoint())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 31, 2, params);
    // inform user via the exception mechanism
    castor::exception::NoEntry e;
    e.getMessage() << "Unknown mountPoint '"
		   << admin->mountPoint()
		   << "' on machine '"
		   << admin->diskServerName()
		   << "'.";
    throw e;
  }
  // Update status if needed
  if (it2->second.adminStatus() != ADMIN_FORCE ||
      admin->adminStatus() != ADMIN_NONE) {
    it2->second.setStatus(admin->status());
    if (admin->adminStatus() == ADMIN_FORCE) {
      it2->second.setAdminStatus(ADMIN_FORCE);
    } else {
      it2->second.setAdminStatus(ADMIN_NONE);
    }
  }
}
