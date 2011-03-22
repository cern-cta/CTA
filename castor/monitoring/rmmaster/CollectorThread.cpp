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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * The Collector thread of the RmMaster daemon. It collects the data from the
 * different nodes and updates a shared memory representation of the cluster.
 * The real implementation of the cluster update is implemented in a separated
 * helper class.
 *
 * @author castor-dev team
 *****************************************************************************/

// Include files
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
#include "castor/monitoring/MonitorMessageAck.hpp"
#include "castor/monitoring/FileSystemStateAck.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/Constants.hpp"
#include <time.h>
#include <errno.h>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::CollectorThread::CollectorThread
(castor::monitoring::ClusterStatus* clusterStatus, bool noLSF) : m_noLSF(noLSF) {
  m_updater =
    new castor::monitoring::rmmaster::ora::StatusUpdateHelper(clusterStatus);
  // "Collector thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 10, 0, 0);
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::monitoring::rmmaster::CollectorThread::~CollectorThread() throw() {
  if (m_updater != 0) {
    delete m_updater;
  }
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::monitoring::rmmaster::CollectorThread::run(void* par) throw() {
  try {

    // "Thread Collector running"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 0, 0);

    // Get the server socket
    castor::io::ServerSocket* sock = (castor::io::ServerSocket*) par;

    // Prepare the acknowledgement
    castor::monitoring::MonitorMessageAck ack;
    ack.setStatus(true);

    // Get the incoming object
    castor::IObject* obj = 0;
    unsigned short port;
    unsigned long  ip;
    try {
      obj = sock->readObject();
      sock->getPeerIp(port, ip);
    } catch (castor::exception::Exception& e) {
      // "Unable to read object from socket"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 12, 1, params);
      ack.setStatus(false);
      ack.setErrorCode(EINVAL);
      std::ostringstream stst;
      stst << "Unable to read request object from socket."
           << std::endl << e.getMessage().str();
      ack.setErrorMessage(stst.str());
      return;
    }

    // Get the information about who is the current resource monitoring master
    std::string masterName;
    std::string hostName;
    bool production = true;
    if (!m_noLSF) {
      try {
        castor::monitoring::rmmaster::LSFStatus::getInstance()->
          getLSFStatus(production, masterName, hostName, false);
      } catch (castor::exception::Exception& e) {
        // Ignore error
      }
    }

    // Handle it
    if (ack.status()) {
      try {
        if (OBJ_DiskServerStateReport == obj->type()) {
          castor::monitoring::DiskServerStateReport* dss =
            dynamic_cast<castor::monitoring::DiskServerStateReport*>(obj);
	  if (production) {
	    m_updater->handleStateUpdate(dss);
	  }
        } else if (OBJ_DiskServerMetricsReport == obj->type()) {
          castor::monitoring::DiskServerMetricsReport* dss =
            dynamic_cast<castor::monitoring::DiskServerMetricsReport*>(obj);
	  if (production) {
	    m_updater->handleMetricsUpdate(dss);
	  }
        } else if (OBJ_DiskServerAdminReport == obj->type()) {
          castor::monitoring::admin::DiskServerAdminReport* dss =
            dynamic_cast<castor::monitoring::admin::DiskServerAdminReport*>(obj);
	  if (production) {
	    m_updater->handleDiskServerAdminUpdate(dss, ip);
	  } else {
	    // Send "Try again" back to the client setting the error message
	    // to the name of production LSF master
	    ack.setStatus(false);
	    ack.setErrorCode(EAGAIN);
	    std::ostringstream stst;
	    stst << hostName << " is currently not the production rmMaster"
		 << " server, try " << masterName;
	    ack.setErrorMessage(stst.str());
	  }
        } else if (OBJ_FileSystemAdminReport == obj->type()) {
          castor::monitoring::admin::FileSystemAdminReport* dss =
            dynamic_cast<castor::monitoring::admin::FileSystemAdminReport*>(obj);
	  if (production) {
	    m_updater->handleFileSystemAdminUpdate(dss, ip);
	  } else {
	    // Send "Try again" back to the client setting the error message
	    // to the name of production LSF master
	    ack.setStatus(false);
	    ack.setErrorCode(EAGAIN);
	    std::ostringstream stst;
	    stst << hostName << " is currently not the production rmMaster"
		 << " server, try " << masterName;
	    ack.setErrorMessage(stst.str());
	  }
        } else {

          // "Received unknown object from socket"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("Type", obj->type()),
	     castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	     castor::dlf::Param("Port", port)};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 3, params);
          ack.setStatus(false);
          ack.setErrorCode(EINVAL);
          std::ostringstream stst;
          stst << "Received unknown object from socket."
               << std::endl << "Type was " << obj->type();
          ack.setErrorMessage(stst.str());
        }
      } catch (castor::exception::Exception& e) {

        // "Caught exception in CollectorThread"
        castor::dlf::Param params[] =
	  {castor::dlf::Param("Type", sstrerror(e.code())),
	   castor::dlf::Param("Message", e.getMessage().str()),
	   castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	   castor::dlf::Param("Port", port)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 45, 4, params);
        ack.setStatus(false);
        ack.setErrorCode(e.code());
        std::ostringstream stst;
        stst << e.getMessage().str();
        ack.setErrorMessage(stst.str());
      }
    }

    // For objects of type DiskServerStateReport send back the disk server and
    // file system status values in the acknowledgement. Only if we are the
    // master
    if ((OBJ_DiskServerStateReport == obj->type()) && (production)) {
      castor::monitoring::DiskServerStateReport* dss =
        dynamic_cast<castor::monitoring::DiskServerStateReport*>(obj);

      // Get ClusterStatus map
      bool create = false;
      castor::monitoring::ClusterStatus* cs =
        castor::monitoring::ClusterStatus::getClusterStatus(create);
      if (0 != cs) {
        // Cast normal string into sharedMemory one in order to be able to
	// search for it in the ClusterStatus map.
        std::string machineName = dss->name();
        castor::monitoring::SharedMemoryString *smMachineName =
          (castor::monitoring::SharedMemoryString*)(&machineName);
        // Find the diskServer
        castor::monitoring::ClusterStatus::iterator it = cs->find(*smMachineName);
        if (it != cs->end()) {
	  ack.setDiskServerStatus(it->second.status());
	  for (castor::monitoring::DiskServerStatus::const_iterator it2
		 = it->second.begin();
	       it2 != it->second.end();
	       it2++) {
            const castor::monitoring::FileSystemStatus& fss = it2->second;
	    castor::monitoring::FileSystemStateAck *fssAck =
              new castor::monitoring::FileSystemStateAck();
	    fssAck->setFileSystemStatus(fss.status());
	    ack.addAck(fssAck);
	  }
        }
      }
    }
    // Cleanup
    if (0 != obj) delete obj;

    // "Sending acknowledgement to client"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
       castor::dlf::Param("Port", port)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 15, 2, params2);
    try {
      sock->sendObject(ack);
    } catch (castor::exception::Exception& e) {
      // "Unable to send ack to client"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Type", sstrerror(e.code())),
	 castor::dlf::Param("Message", e.getMessage().str()),
	 castor::dlf::Param("IP", castor::dlf::IPAddress(ip)),
	 castor::dlf::Param("Port", port)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 16, 4, params);
    }
    // Closing and deleting socket
    delete sock;
  } catch (...) {
    // "General exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "CollectorThread::run")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 17, 1, params);
  }
}
