/******************************************************************************
 *                      MetricsThread.cpp
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
 * The MetricsThread of the RmNode daemon collects the metrics of the
 * diskserver and sends them to the resource master
 *
 * @author castor-dev team
 *****************************************************************************/

// Include files
#include "castor/monitoring/DiskServerMetricsReport.hpp"
#include "castor/monitoring/FileSystemMetricsReport.hpp"
#include "castor/monitoring/rmnode/MetricsThread.hpp"
#include "castor/monitoring/rmnode/RmNodeDaemon.hpp"
#include "castor/monitoring/MonitorMessageAck.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/IObject.hpp"
#include "castor/System.hpp"
#include <sys/sysinfo.h>
#include <iostream>
#include <fstream>
#include "getconfent.h"
#include "stage_constants.h"
#include <sys/vfs.h>
#include "errno.h"
#include <mntent.h>      /* To get partition table */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <pwd.h>
#include <string.h>
#include <algorithm>

// Definitions
#define PARTITIONS_FILE "/proc/partitions"
#define DISKSTATS_FILE "/proc/diskstats"
#define MTAB_FILE "/etc/mtab"


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::monitoring::rmnode::MetricsThread::MetricsThread
(std::map<std::string, u_signed64> hostList, int port) :
  m_hostList(hostList),
  m_port(port) {

  // "Metrics thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 7, 0, 0);
  m_diskServerMetrics = new castor::monitoring::DiskServerMetricsReport();
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::monitoring::rmnode::MetricsThread::~MetricsThread() throw() {
  // "Metrics thread destroyed"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 13, 0, 0);
  if (m_diskServerMetrics != 0) {
    delete m_diskServerMetrics;
    m_diskServerMetrics = 0;
  }
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::monitoring::rmnode::MetricsThread::run(void*)
  throw(castor::exception::Exception) {

  // "Metrics thread running"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 0, 0);

  // Collect metrics information
  try {
    collectDiskServerMetrics();
  } catch (castor::exception::Exception& e) {

    // "Failed to collect diskserver metrics"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 24, 2, params);
    return;
  }

  // Send information to resource masters
  for(std::map<std::string, u_signed64>::iterator it =
	m_hostList.begin();
      it != m_hostList.end(); it++) {
    try {
      castor::io::ClientSocket s((*it).second, (*it).first);
      s.connect();
      s.sendObject(*m_diskServerMetrics);

      // "Metrics information has been sent"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Content", m_diskServerMetrics)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 17, 1, params);

      // Check the acknowledgement
      castor::IObject *obj = s.readObject();
      castor::MessageAck *ack =
	dynamic_cast<castor::monitoring::MonitorMessageAck *>(obj);
      if (ack == 0) {

	// "Received no acknowledgement from server"
	castor::dlf::Param params[] =
	  {castor::dlf::Param("Server", (*it).first),
	   castor::dlf::Param("Port", (*it).second)};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 26, 2, params);
      } else {
	delete ack;
      }
    } catch (castor::exception::Exception& e) {

      // "Error caught when trying to send metric information"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Type", sstrerror(e.code())),
	 castor::dlf::Param("Message", e.getMessage().str()),
	 castor::dlf::Param("Server", (*it).first),
	 castor::dlf::Param("Port", (*it).second)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 15, 4, params);
    } catch (...) {

      // "Failed to send metrics information"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Message", "General exception caught")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 25, 1, params);
    }
  }
}


//------------------------------------------------------------------------------
// CollectDiskServerMetrics
//------------------------------------------------------------------------------
void castor::monitoring::rmnode::MetricsThread::collectDiskServerMetrics()
  throw(castor::exception::Exception) {

  // Fill in the machine name
  m_diskServerMetrics->setName(castor::System::getHostName());

  // Use sysinfo to get total RAM, Memory and Swap
  struct sysinfo si;
  if (0 == sysinfo(&si)) {
    // RAM
    u_signed64 ram = si.freeram;
    ram *= si.mem_unit;
    m_diskServerMetrics->setFreeRam(ram);
    // Memory
    u_signed64 memory = si.freehigh;
    memory *= si.mem_unit;
    m_diskServerMetrics->setFreeMemory(memory);
    // Swap
    u_signed64 swap = si.freeswap;
    swap *= si.mem_unit;
    m_diskServerMetrics->setFreeSwap(swap);
  } else {
    castor::exception::Exception e(errno);
    e.getMessage() << "sysinfo call failed in collectDiskServerMetrics";
    throw e;
  }

  // Fill metrics for each filesystem
  std::vector<std::string> fsList = 
    castor::monitoring::rmnode::RmNodeDaemon::getMountPoints();
  try {
    for (u_signed64 i = 0; i < fsList.size(); i++) {

      // Search for the filesystem in the filesystem vector
      castor::monitoring::FileSystemMetricsReport* metrics = 0;
      for (std::vector<castor::monitoring::FileSystemMetricsReport*>::const_iterator
	     it = m_diskServerMetrics->fileSystemMetricsReports().begin();
	   it != m_diskServerMetrics->fileSystemMetricsReports().end();
	   it++) {
	if ((*it)->mountPoint() == fsList[i]) {
	  metrics = *it;
	  break;
	}
      }

      // Entry not found? This must be a new filesystem
      if (metrics == 0) {
	metrics = new castor::monitoring::FileSystemMetricsReport();
	metrics->setMountPoint(fsList[i]);
	m_diskServerMetrics->addFileSystemMetricsReports(metrics);

	// Search the passwd database for the stage super user
	passwd *pw = getpwnam(STAGERSUPERUSER);
	if (pw == 0) {
	  castor::exception::Exception e(errno);
	  e.getMessage() << "Failed to user information for the stage super "
			 << "user: " << STAGERSUPERUSER ;
	  throw e;
	}

	// Create the sub-directories in the filesystem
	for (int j = 0; j < 100; j++) {
	  char path[CA_MAXPATHLEN + 1];
	  snprintf(path, CA_MAXPATHLEN + 1, "%s/%.2d", fsList[i].c_str(), j);

	  int rv = mkdir(path, 0700);
	  if ((rv < 0) && (errno != EEXIST)) {
	    castor::exception::Exception e(errno);
	    e.getMessage() << "Failed to create directory: " << path;
	    throw e;
	  }

	  rv = chown(path, pw->pw_uid, pw->pw_gid);
	  if (rv < 0) {
	    castor::exception::Exception e(errno);
	    e.getMessage() << "Unable to change directory ownersip on :"
			   << path << " to " << pw->pw_uid << ":" << pw->pw_gid;
	    throw e;
	  }
	}
      }

      // Get the filesystems metric information
      try {
	collectFileSystemMetrics(metrics);
      } catch (castor::exception::InvalidArgument& e) {

	// If we got this far then the mountpoint/filesystem has been considered
	// invalid. We throttle the error message here to not fill up the log
	// file.
	std::map<std::string, u_signed64>::const_iterator it =
	  m_invalidMountPoints.find(metrics->mountPoint());
	if ((it == m_invalidMountPoints.end()) ||
	    ((time(NULL) - (*it).second) > 3600)) {

	  // "Failed to collect filesystem metrics"
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("Message", e.getMessage().str())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 27, 1, params);
	  m_invalidMountPoints[metrics->mountPoint()] = time(NULL);
	}
	m_diskServerMetrics->removeFileSystemMetricsReports(metrics);
      }
    }
  } catch (castor::exception::Exception& e) {
    throw e;
  }
}


//------------------------------------------------------------------------------
// CollectFileSystemMetrics
//------------------------------------------------------------------------------
void castor::monitoring::rmnode::MetricsThread::collectFileSystemMetrics
(castor::monitoring::FileSystemMetricsReport* filesystem)
  throw(castor::exception::Exception) {

  // Determine the number of streams reading, writing and in read/write mode on
  // the given mountpoint/filesystem. The only way to do this is to cycle
  // through all processes listed in /proc/ and determine if ant file descriptors
  // are open to the mountpoint in question
  DIR *dir_proc = opendir("/proc/");
  if (dir_proc == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to opendir: /proc/";
    throw e;
  }

  // Set initial stream values
  filesystem->setNbReadStreams(0);
  filesystem->setNbWriteStreams(0);
  filesystem->setNbReadWriteStreams(0);
  filesystem->setNbMigratorStreams(0);
  filesystem->setNbRecallerStreams(0);

  // Loop over directory entries
  dirent *entry_proc = 0;
  while ((entry_proc = readdir(dir_proc))) {
    std::ostringstream path("");
    if (strspn(entry_proc->d_name, "0123456789") != strlen(entry_proc->d_name)) {
      continue;            // not a process, i.e not a number
    }

    // Open the processes file descriptor listing. The 'fd' directory can only be
    // viewed by the user of the process or root!! We don't trap any more errors
    // from this point forward as the error is mostly likely related to process
    // death.
    path << "/proc/" << entry_proc->d_name << "/fd";
    DIR *dir_fd = opendir(path.str().c_str());
    if (dir_fd == NULL) {
      continue;
    }

    dirent *entry_fd = 0;
    while ((entry_fd = readdir(dir_fd))) {
      if (strspn(entry_fd->d_name, "0123456789") != strlen(entry_fd->d_name)) {
	continue;          // not a file descriptor
      }

      // The fd directory contains a list of symlinks to a given resource. We
      // need to resolve these first before processing further.
      std::ostringstream fdpath("");
      fdpath << path.str() << "/" << entry_fd->d_name;
      struct stat statbuf;
      if (lstat(fdpath.str().c_str(), &statbuf) != 0) {
	continue;
      }
      if (!S_ISLNK(statbuf.st_mode)) {
	continue;
      }
      char buf[CA_MAXPATHLEN + 1];
      int len;
      buf[0] = '\0';
      if ((len = readlink(fdpath.str().c_str(), buf, CA_MAXPATHLEN)) < 0) {
	continue;
      }
      buf[len] = '\0';

      // Resource not of interest ?
      if (strncmp(filesystem->mountPoint().c_str(), buf,
		  filesystem->mountPoint().length())) {
	continue;
      }

      // Migrator or recaller ? we distinguish between the two in order to
      // improve scheduling on the box. If the command line of the process is
      // /usr/bin/rfiod -sl it is either a migrator or a recaller. The exact one
      // depends on the direction of the stream.
      std::ostringstream cmdpath("");
      cmdpath << "/proc/" << entry_proc->d_name << "/cmdline";
      int migrecal = 0;

      std::ifstream in(cmdpath.str().c_str());
      if (in) {
	std::stringstream ss("");
	ss << in.rdbuf();
	// /proc/<pid>/cmdline stores its values using NULL byte termination for
	// each field. As a result it is necessary to replace all NULL bytes with
	// spaces.
	std::string cmdline(ss.str());
	std::replace(cmdline.begin(), cmdline.end(), '\0', ' ');
	if (!strncmp("/usr/bin/rfiod -sl", cmdline.c_str(), 18)) {
	  migrecal = 1;
	}
	in.close();
      }

      // The permission bits on the file indicate what mode the stream is in
      if ((statbuf.st_mode & S_IRUSR) == S_IRUSR) {
	if ((statbuf.st_mode & S_IWUSR) == S_IWUSR) {
	  // read/write (can only be user streams)
	  filesystem->setNbReadWriteStreams
	    (filesystem->nbReadWriteStreams() + 1);
	} else {
	  if (migrecal) {
	    filesystem->setNbMigratorStreams
	      (filesystem->nbMigratorStreams() + 1);
	  } else {
	    filesystem->setNbReadStreams
	      (filesystem->nbReadStreams() + 1); // read only;
	  }
	}
      } else if ((statbuf.st_mode & S_IWUSR) == S_IWUSR) {
	if (migrecal) {
	  filesystem->setNbRecallerStreams
	    (filesystem->nbRecallerStreams() + 1);
	} else {
	  filesystem->setNbWriteStreams
	    (filesystem->nbWriteStreams() + 1);  // write only
	}
      }
    }
    closedir(dir_fd);
  }
  closedir(dir_proc);

  // Access the filesystem descriptor table and determine if the mountpoint is
  // actually present and what mnt_fsname it is associated with.
  FILE *fd = setmntent(MTAB_FILE, "r");
  if (fd == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to setmntent: " << MTAB_FILE;
    throw e;
  }

  // Loop over the mounted file system information
  mntent *m;
  std::string fsname("");
  while ((m = getmntent(fd)) != NULL) {
    unsigned int len = strlen(m->mnt_dir);
    if (!strncmp(m->mnt_dir, filesystem->mountPoint().c_str(), len)) {
      if ((len == filesystem->mountPoint().length()) ||
	  ((len + 1 == filesystem->mountPoint().length()) &&
	   filesystem->mountPoint()[len] == '/')) {
	fsname = m->mnt_fsname;
	fsname = fsname.substr(5);    // remove /dev/ from the fsname
	break;
      }
    }
  }
  endmntent(fd);

  // Entry found ?
  if (fsname == "") {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid mountpoint " + filesystem->mountPoint();
    throw e;
  }

  // Extract the IO statistics for the mountpoint/filesystem from proc. The exact
  // file to open is dependant  on the kernel. On 2.4 its /proc/partitions and on
  // 2.6 its /proc/diskstats
  fd = fopen(DISKSTATS_FILE, "r");
  if (fd == NULL) {
    fd = fopen(PARTITIONS_FILE, "r");
  } else {

    // 2.6 kernel doesn't provide statistics at the partition level. If this is a
    // partition remove the last digit to get the block device. This method is
    // probably not the best!!
    std::istringstream i(fsname.substr(fsname.length() - 1));
    u_signed64 x;
    if (i >> x) {
      fsname = fsname.substr(0, fsname.length() - 1);
    }
  }

  if (fd == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Unable to get IO statistics for mountpoint "
		   << filesystem->mountPoint();
    throw e;
  }

  // Useful variables
  signed64 rsect = 0, wsect = 0;
  unsigned long ul;
  unsigned int  ui;
  char   line[256];
  char   dev_name[72];
  time_t now = time(NULL);
  time_t diff;

  while (fgets(line, sizeof(line), fd) != NULL) {

    // 2.4 kernel
    if (sscanf(line, "%u %u %lu %70s %lu %lu %lld %lu %lu %lu %lld %lu %lu %lu %lu",
	       &ui, &ui, &ul, dev_name, &ul, &ul, &rsect, &ul, &ul, &ul, &wsect,
	       &ul, &ul, &ul, &ul) != 15) {

      // 2.6 kernel
      if (sscanf(line, "%u %u %70s %lu %lu %lld %lu %lu %lu %lld %lu %lu %lu %lu",
		 &ui, &ui, dev_name, &ul, &ul, &rsect, &ul, &ul, &ul, &wsect,
		 &ul, &ul, &ul, &ul) != 14) {
	continue;        // format not recognised
      }
    }

    if (fsname != dev_name) {
      continue;          // device/partition not of interest
    }

    // Protect against counter overflows
    if (filesystem->lastUpdateTime()) {
      if ((wsect >= (signed64)filesystem->previousWriteCounter()) &&
	  (rsect >= (signed64)filesystem->previousReadCounter())) {
	diff = now - filesystem->lastUpdateTime();

	// Update read rate
	filesystem->setReadRate((u_signed64)(
	  0.5 * (rsect - filesystem->previousReadCounter()) / diff)
	);

	// Update write rate
	filesystem->setWriteRate((u_signed64)(
	  0.5 * (wsect - filesystem->previousWriteCounter()) / diff)
	);
      }
    }

    // Record values for later
    filesystem->setLastUpdateTime(now);
    filesystem->setPreviousWriteCounter(wsect);
    filesystem->setPreviousReadCounter(rsect);
  }
  fclose(fd);

  // Set free space
  struct statfs sf;
  if (statfs(filesystem->mountPoint().c_str(),&sf) == 0) {
    filesystem->setFreeSpace(((u_signed64) sf.f_bavail) * ((u_signed64) sf.f_bsize));
  } else {
    castor::exception::Exception e(errno);
    e.getMessage() << "statfs call failed for " << filesystem->mountPoint();
    throw e;
  }
}
