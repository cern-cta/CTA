/******************************************************************************
 *                      StateThread.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * The StateThread of the RmNode daemon collects information about the state
 * of the diskserver and its filesystems
 *
 * @author castor-dev team
 *****************************************************************************/

// Include files
#include "castor/monitoring/rmnode/StateThread.hpp"
#include "castor/monitoring/rmnode/RmNodeDaemon.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/stager/FileSystemStatusCodes.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/monitoring/DiskServerStateReport.hpp"
#include "castor/monitoring/FileSystemStateReport.hpp"
#include "castor/monitoring/MonitorMessageAck.hpp"
#include "castor/monitoring/FileSystemStateAck.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/IObject.hpp"
#include "castor/System.hpp"
#include "castor/Constants.hpp"
#include <iostream>
#include <fstream>
#include <vector>
#include "getconfent.h"
#include <unistd.h>
#include <sys/sysinfo.h>
#include <sys/vfs.h>
#include "errno.h"

// Definitions
#define DEFAULT_MINFREESPACE .10
#define DEFAULT_MAXFREESPACE .15
#define DEFAULT_MINALLOWEDFREESPACE .05
#define DEFAULT_STATUSFILE "/etc/castor/status"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmnode::StateThread::StateThread
(std::map<std::string, u_signed64> hostList, int port) :
  m_hostList(hostList),
  m_port(port) {

  // "State thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 6, 0, 0);
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::monitoring::rmnode::StateThread::~StateThread() throw() {
  // "State thread destroyed"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 12, 0, 0);
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::monitoring::rmnode::StateThread::run(void *param)
  throw(castor::exception::Exception) {

  // "State thread running"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 8, 0, 0);

  // Collect state information
  castor::monitoring::DiskServerStateReport *state = 0;
  try {
    state = collectDiskServerState();
  } catch (castor::exception::Exception e) {

    // "Failed to collect diskserver status"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 23, 2, params);
    return;
  }


  // Send information to resource masters
  std::vector<std::string> fsList = 
    castor::monitoring::rmnode::RmNodeDaemon::getMountPoints();
  for(std::map<std::string, u_signed64>::iterator it =
	m_hostList.begin();
      it != m_hostList.end(); it++) {
    try {
      castor::io::ClientSocket s((*it).second, (*it).first);
      s.connect();
      s.sendObject(*state);

      // "State information has been sent"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Content", state)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 16, 1, params);

      // Check the acknowledgement
      castor::IObject *obj = s.readObject();
      castor::monitoring::MonitorMessageAck *ack = 0;

      if (OBJ_MonitorMessageAck == obj->type()) {
	ack = dynamic_cast<castor::monitoring::MonitorMessageAck *>(obj);
      } else {

	// "Received unknown object for acknowledgement from socket"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Type", obj->type()),
	   castor::dlf::Param("Server", (*it).first),
	   castor::dlf::Param("Port", (*it).second)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 3, params);
      }

      // Update the status file
      if (ack != 0) {
     	const char *filename = getconfent("RmNode", "StatusFile", 0);
	if (filename == NULL) {
	  filename = DEFAULT_STATUSFILE;
	}
	if ((filename) && (ack->ack().size())) {
	  FILE *fp = fopen(filename, "w");
	  if (fp == NULL) {

	    // "Failed to update the RmNode/StatusFile"
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Filename", filename),
	       castor::dlf::Param("Error", strerror(errno))};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 22, 2, params);
	  } else if (ack->ack().size() != fsList.size()) {
	    
	    // "Failed to update the RmNode/StatusFile, filesystem acknowledgement mismatch"
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Filename", filename),
	       castor::dlf::Param("Acknowledged", ack->ack().size()),
	       castor::dlf::Param("Configured", fsList.size())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 3, params);
	  } else {

	    // Update the status of the diskserver
	    fprintf(fp, "DiskServerStatus=%s\n",
		    castor::stager::DiskServerStatusCodeStrings[ack->diskServerStatus()]);

	    // Update the status of the filesystems
	    std::vector<FileSystemStateAck *>::const_iterator it;
	    int i = 0;
	    for (it = ack->ack().begin();
		 it != ack->ack().end();
		 it++, i++) {
	      fprintf(fp, "%s=%s\n", fsList[i].c_str(),
		      castor::stager::FileSystemStatusCodesStrings[(*it)->fileSystemStatus()]);
	    }
	    fclose(fp);
	  }
	}
	delete ack;
      }
    } catch (castor::exception::Exception e) {

      // "Error caught when trying to send state information"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Type", sstrerror(e.code())),
	 castor::dlf::Param("Message", e.getMessage().str()),
	 castor::dlf::Param("Server", (*it).first),
	 castor::dlf::Param("Port", (*it).second)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 4, params);
    } catch (...) {

      // "Failed to send state information"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Message", "General exception caught")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 21, 1, params);
    }
  }

  // Cleanup
  if (state != 0) {
    delete state;
    state = 0;
  }
}


//-----------------------------------------------------------------------------
// CollectDiskServerState
//-----------------------------------------------------------------------------
castor::monitoring::DiskServerStateReport*
castor::monitoring::rmnode::StateThread::collectDiskServerState()
  throw(castor::exception::Exception) {
  castor::monitoring::DiskServerStateReport* state =
    new castor::monitoring::DiskServerStateReport();

  // Fill in the machine name
  state->setName(castor::System::getHostName());

  // Use sysinfo to get total RAM, Memory and Swap
  struct sysinfo si;
  if (0 == sysinfo(&si)) {
    // RAM
    u_signed64 ram = si.totalram;
    ram *= si.mem_unit;
    state->setRam(ram);
    // Memory
    u_signed64 memory = si.totalhigh;
    memory *= si.mem_unit;
    state->setMemory(memory);
    // Swap
    u_signed64 swap = si.totalswap;
    swap *= si.mem_unit;
    state->setSwap(swap);
  } else {
    delete state;
    castor::exception::Exception e(errno);
    e.getMessage() << "sysinfo call failed in collectDiskServerState";
    throw e;
  }

  // Set status (always the same)
  state->setStatus(castor::stager::DISKSERVER_PRODUCTION);
  state->setAdminStatus(castor::monitoring::ADMIN_NONE);

  // Get the current values for min, max and minAllowedFreeSpace
  char *value = getconfent("RmNode","MinFreeSpace", 0);
  float minFreeSpace = DEFAULT_MINFREESPACE;
  if (value) {
    minFreeSpace = strtof(value, 0);
    if (minFreeSpace > 1 || minFreeSpace < 0) {
      minFreeSpace = DEFAULT_MINFREESPACE;

      // "Invalid RmNode/MinFreeSpace option, using default"
      castor::dlf::Param initParams[] =
	{castor::dlf::Param("Value", value),
	 castor::dlf::Param("Default", minFreeSpace)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 18, 2, initParams);
    }
  }

  value = getconfent("RmNode","MaxFreeSpace", 0);
  float maxFreeSpace = DEFAULT_MAXFREESPACE;
  if (value) {
    maxFreeSpace = strtof(value, 0);
    if (maxFreeSpace > 1 || maxFreeSpace < 0) {
      maxFreeSpace = DEFAULT_MAXFREESPACE;

      // "Invalid RmNode/MaxFreeSpace option, using default"
      castor::dlf::Param initParams[] =
	{castor::dlf::Param("Value", value),
	 castor::dlf::Param("Default", maxFreeSpace)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 19, 2, initParams);
    }
  }

  value = getconfent("RmNode","MinAllowedFreeSpace", 0);
  float minAllowedFreeSpace = DEFAULT_MINALLOWEDFREESPACE;
  if (value) {
    minAllowedFreeSpace = strtof(value, 0);
    if (minAllowedFreeSpace > 1 || minAllowedFreeSpace < 0) {
      minAllowedFreeSpace = DEFAULT_MINALLOWEDFREESPACE;

      // "Invalid RmNode/MinAllowedFreeSpace option, using default"
      castor::dlf::Param initParams[] =
	{castor::dlf::Param("Value", value),
	 castor::dlf::Param("Default", minAllowedFreeSpace)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 20, 2, initParams);
    }
  }

  // Fill the state for each filesystem
  std::vector<std::string> fsList = 
    castor::monitoring::rmnode::RmNodeDaemon::getMountPoints();
  try {
    for (u_signed64 i = 0; i < fsList.size(); i++) {
      state->addFileSystemStatesReports
	(collectFileSystemState(fsList[i],
				minFreeSpace,
				maxFreeSpace,
				minAllowedFreeSpace));
    }
  } catch (castor::exception::Exception e) {
    delete state;
    throw e;
  }

  return state;
}


//-----------------------------------------------------------------------------
// CollectFileSystemState
//-----------------------------------------------------------------------------
castor::monitoring::FileSystemStateReport*
castor::monitoring::rmnode::StateThread::collectFileSystemState
(std::string mountPoint, float minFreeSpace,
 float maxFreeSpace, float minAllowedFreeSpace)
  throw(castor::exception::Exception) {
  castor::monitoring::FileSystemStateReport* state =
    new castor::monitoring::FileSystemStateReport();

  // Set mountpoint
  state->setMountPoint(mountPoint);
  state->setMinFreeSpace(minFreeSpace);
  state->setMaxFreeSpace(maxFreeSpace);
  state->setMinAllowedFreeSpace(minAllowedFreeSpace);

  // Set space
  struct statfs sf;
  if (statfs(mountPoint.c_str(),&sf) == 0) {
    state->setSpace(((u_signed64) sf.f_blocks) * ((u_signed64) sf.f_bsize));
  } else {
    delete state;
    castor::exception::Exception e(errno);
    e.getMessage() << "statfs call failed for " << mountPoint;
    throw e;
  }

  // Set status (always the same)
  state->setStatus(castor::stager::FILESYSTEM_PRODUCTION);
  state->setAdminStatus(castor::monitoring::ADMIN_NONE);
  return state;
}
