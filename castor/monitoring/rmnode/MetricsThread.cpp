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
 * The MetricsThread of the RmNode daemon collects and send to
 * the rmmaster the metrics of the node on which it runs.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include files
#include "castor/monitoring/DiskServerMetricsReport.hpp"
#include "castor/monitoring/FileSystemMetricsReport.hpp"
#include "castor/monitoring/rmnode/MetricsThread.hpp"
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
#include <sys/vfs.h>
#include "errno.h"
#include <mntent.h>      /* To get partition table */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <pwd.h>

/// File locations
#define PARTITIONS_FILE "/proc/partitions"
#define DISKSTATS_FILE  "/proc/diskstats"
#define MTAB_FILE       "/etc/mtab"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmnode::MetricsThread::MetricsThread
(std::string rmMasterHost, int rmMasterPort) :
  m_rmMasterHost(rmMasterHost), m_rmMasterPort(rmMasterPort) {
  // "Metrics thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 7, 0, 0);
  
  dsMetrics = new castor::monitoring::DiskServerMetricsReport();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::monitoring::rmnode::MetricsThread::~MetricsThread() throw() {
  // "Metrics thread destructed"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 13, 0, 0);

  if (dsMetrics != 0) {
    delete dsMetrics;
    dsMetrics = 0;
  }
}

//------------------------------------------------------------------------------
// runs the thread starts by threadassign()
//------------------------------------------------------------------------------
void castor::monitoring::rmnode::MetricsThread::run(void* par)
  throw(castor::exception::Exception) {
  // "Metrics thread started"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 0, 0);
  try {
    // collect
    collectDiskServerMetrics();
    // send metrics to rmMaster
    castor::io::ClientSocket s(m_rmMasterPort, m_rmMasterHost);
    s.connect();
    s.sendObject(*dsMetrics);
    // "Metrics sent to rmMaster"
    castor::dlf::Param params[] =
      {castor::dlf::Param("content", dsMetrics)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 17, 1, params);
    // check the acknowledgment. Status file updates are only performed in the
    // state update thread
    castor::IObject* obj = s.readObject();
    castor::monitoring::MonitorMessageAck* ack =
      dynamic_cast<castor::monitoring::MonitorMessageAck*>(obj);
    if (0 == ack) {
      castor::exception::InvalidArgument e; // XXX To be changed
      e.getMessage() << "No Acknowledgement from the Server";
      delete ack;
      throw e;
    }
    delete ack;
  }
  catch(castor::exception::Exception e) {
    // "Error caught in MetricsThread::run"
    castor::dlf::Param params[] =
      {castor::dlf::Param("code", sstrerror(e.code())),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 15, 2, params);
  }
  catch (...) {
    // "Error caught in MetricsThread::run"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("message", "general exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 15, 1, params2);
  }
}

//------------------------------------------------------------------------------
// collectDiskServerMetrics
//------------------------------------------------------------------------------
void castor::monitoring::rmnode::MetricsThread::collectDiskServerMetrics()
  throw(castor::exception::Exception) {
  // set diskServer name
  dsMetrics->setName(castor::System::getHostName());
  // use sysinfo to get all data
  struct sysinfo si;
  if (0 == sysinfo(&si)) {
    // RAM
    u_signed64 ram = si.freeram;
    ram *= si.mem_unit;
    dsMetrics->setFreeRam(ram);
    // Memory
    u_signed64 memory = si.freehigh;
    memory *= si.mem_unit;
    dsMetrics->setFreeMemory(memory);
    // Swap
    u_signed64 swap = si.freeswap;
    swap *= si.mem_unit;
    dsMetrics->setFreeSwap(swap);
  } else {
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectDiskServerMetrics : sysinfo call failed";
    throw e;
  }
  // get current list of filesystems
  char** fs;
  char   path[CA_MAXPATHLEN + 1];
  int    nbFs;
  int    rc;

  if (getconfent_multi
      ("RmNode", "MountPoints", 1, &fs, &nbFs) < 0) {
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectDiskServerMetrics : getconfent_multi failed";
    throw e;
  }
  // fill metrics for each FileSystems
  try {
    for (int i = 0; i < nbFs; i++) {
 
      // search for the mountpoint in the filesystem vector
      castor::monitoring::FileSystemMetricsReport* metrics = NULL;
      for (std::vector<castor::monitoring::FileSystemMetricsReport*>::const_iterator
	     it = dsMetrics->fileSystemMetricsReports().begin();
	   it != dsMetrics->fileSystemMetricsReports().end();
	   it++) { 
	if ((*it)->mountPoint() == fs[i]) {
	  metrics = *it;
	  break;
	}
      }

      // entry not found ?
      if (metrics == NULL) {
	metrics = new castor::monitoring::FileSystemMetricsReport();
	metrics->setMountPoint(fs[i]);
	dsMetrics->addFileSystemMetricsReports(metrics);

	// search the password file for the stage user.
	struct passwd *pw = getpwnam("stage");
	if (pw == NULL) {
	  castor::exception::Exception e(errno);
	  e.getMessage() << "MetricsThread::collectDiskServerMetrics : "
			 << "failed to get username information for user "
			 << "'stage', check account exists";
	  throw e;
	}

	// create the sub-directories in the filesystem
	for (int j = 0; j < 100; j++) {
	  sprintf(path, "%s/%.2d", fs[i], j);

	  rc = mkdir(path, 0700);
	  if ((rc != 0) && (errno != EEXIST)) {
	    castor::exception::Exception e(errno);
	    e.getMessage() << "MetricsThread::collectDiskServerMetrics : "
			   << "failed to create directory " << path;
	    throw e;
	  }

	  rc = chown(path, pw->pw_uid, pw->pw_gid);
	  if (rc != 0) {
	    castor::exception::Exception e(errno);
	    e.getMessage() << "MetricsThread::collectDiskServerMetrics : "
			   << "unable to change directory ownership on " 
			   << path << " to uid:" << pw->pw_uid << " gid:"
			   << pw->pw_gid;
	    throw e;
	  }
	}
      }
      collectFileSystemMetrics(metrics);
    }
  } catch (castor::exception::Exception e) {
    // free memory
    for (int i = 0; i < nbFs; i++) {
      free(fs[i]);
    }
    free(fs);
    throw e;
  }
  // free memory
  for (int i = 0; i < nbFs; i++) {
    free(fs[i]);
  }
  free(fs);
}

//------------------------------------------------------------------------------
// collectFileSystemMetrics
//------------------------------------------------------------------------------
void castor::monitoring::rmnode::MetricsThread::collectFileSystemMetrics
(castor::monitoring::FileSystemMetricsReport* filesystem)
  throw(castor::exception::Exception) {

  // determine the number of streams reading, writing and in read/write mode on 
  // the given mountpoint/filesystem. The only way to do this is to cycle through
  // all processes listed in /proc/ and determine if ant file descriptors are 
  // open to the mountpoint in question
  DIR *dir_proc = opendir("/proc/");
  if (dir_proc == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectFileSystemMetrics : opendir failed";
    throw e;
  }

  // variables
  struct dirent *entry_proc;
  struct dirent *entry_fd;
  struct stat   statbuf;

  unsigned int  nBReadUsers      = 0;
  unsigned int  nBWriteUsers     = 0;
  unsigned int  nBReadWriteUsers = 0;
  unsigned int  nBReadMigrators  = 0;
  unsigned int  nBWriteRecallers = 0;
  unsigned int  x        = 0;
  int           len      = 0;
  int           migrecal = 0;
  char          buf[CA_MAXPATHLEN + 1];
  FILE          *fd;

  // loop over directory entries
  while ((entry_proc = readdir(dir_proc))) {
    std::ostringstream path;
    std::istringstream i(entry_proc->d_name);
    if (!(i >> x)) {
      continue;            // not a process, i.e not a digit
    }
    // open the processes file descriptor listing. The 'fd' directory can only be
    // viewed by the user of the process or root!! We don't trap any more errors
    // from this point forward as the error is mostly likely related to process
    // death.
    path << "/proc/" << entry_proc->d_name << "/fd";
    DIR *dir_fd = opendir(path.str().c_str());
    if (dir_fd == NULL) {
      continue;
    }
    while ((entry_fd = readdir(dir_fd))) {
      std::ostringstream file(entry_fd->d_name);
      std::istringstream fd(file.str());
      if (!(fd >> x)) {
	continue;          // not a file descriptor
      }
      std::ostringstream fdpath;
      fdpath << path.str() << "/" << fd.str();
      // the fd directory contains a list of symlinks to a given resource. We
      // need to resolve these first before processing further.
      if (lstat(fdpath.str().c_str(), &statbuf) != 0) {
	continue;
      }
      if (!S_ISLNK(statbuf.st_mode)) {
	continue;
      }
      buf[0] = '\0';
      if ((len = readlink(fdpath.str().c_str(), buf, CA_MAXPATHLEN)) < 0) {
	continue;
      }
      buf[len] = '\0';
      // resource not of interest ?
      if (strncmp(filesystem->mountPoint().c_str(), buf,
		  filesystem->mountPoint().length())) {
	continue;
      }
      std::ostringstream cmdpath;
      migrecal = 0;
      cmdpath << "/proc/" << entry_proc->d_name << "/cmdline";
      // migrator or recaller ? we distinguish between the two in order to improve
      // scheduling on the box. If the command line of the process is /usr/bin/rfiod 
      // -sl it is either a migrator or a recaller. The exact one depends on the 
      // direction of the stream.
      std::ifstream in(cmdpath.str().c_str());
      if (in) {
	std::stringstream ss;
	ss << in.rdbuf();
	// /proc/<pid>/cmdline stores its values using NULL byte termination for each
	// field. As a result it is necessary to replace all NULL bytes with spaces.
	std::string cmdline(ss.str());	
	std::replace(cmdline.begin(), cmdline.end(), '\0', ' ');
	if (!strncmp("/usr/bin/rfiod -sl", cmdline.c_str(), 18)) {
	  migrecal = 1;
	}
	in.close();
      }
      // the permission bits on the file indicate what mode the stream is in
      if ((statbuf.st_mode & S_IRUSR) == S_IRUSR) {
	if ((statbuf.st_mode & S_IWUSR) == S_IWUSR) {
	  nBReadWriteUsers++;  // read/write (can only be user streams)
	} else {
	  if (migrecal) {
	    nBReadMigrators++;
	  } else {
	    nBReadUsers++;     // read only;
	  }
	}
      } else if ((statbuf.st_mode & S_IWUSR) == S_IWUSR) {
	if (migrecal) {
	  nBWriteRecallers++;
	} else {
	  nBWriteUsers++;      // write only
	}
      }
    }
    closedir(dir_fd);
  }
  closedir(dir_proc);

  // Set stream values
  filesystem->setNbReadStreams(nBReadUsers);
  filesystem->setNbWriteStreams(nBWriteUsers);
  filesystem->setNbReadWriteStreams(nBReadWriteUsers);
  filesystem->setNbMigratorStreams(nBReadMigrators);
  filesystem->setNbRecallerStreams(nBWriteRecallers);

  // access the mounted file system description file and determine if the mountpoint
  // is actually present and what file system (mnt_fsname) it is associated with.
  fd = setmntent(MTAB_FILE, "r");
  if (fd == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectFileSystemMetrics : setmntent failed '/etc/mtab'";
    throw e;
  }

  // loop over mounted file system information
  struct mntent *m;
  std::string   fsname("");
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

  // entry found ?
  if (fsname == "") {
    castor::exception::InvalidArgument e; 
    e.getMessage() << "invalid mountpoint " + filesystem->mountPoint();
    throw e;
  }

  // extract the IO statistics for the mountpoint/filesystem from proc. The exact
  // file to open is dependant on the kernel. On 2.4 its /proc/partitions and on 2.6
  // its /proc/diskstats
  fd = fopen(DISKSTATS_FILE, "r");
  if (fd == NULL) {
    fd = fopen(PARTITIONS_FILE, "r");
  } else {

    // 2.6 kernel doesn't provide statistics at the partition level. If this is a
    // partition remove the last digit to get the block device. This method is
    // probably not the best!!
    std::istringstream i(fsname.substr(fsname.length() - 1));
    if (i >> x) {
      fsname = fsname.substr(0, fsname.length() - 1);
    }
  }
  
  if (fd == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage() << "fopen failed, unable to get IO statistics";
    throw e;
  }

  // variables
  signed64 rsect = 0;
  signed64 wsect = 0;
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
	continue;
      }
    } 

    if (fsname != dev_name) {
      continue;          // device/partition not of interest
    }

    // protect against counter overflows
    if (filesystem->lastUpdateTime()) {
      if ((wsect >= (signed64)filesystem->previousWriteCounter()) &&
	  (rsect >= (signed64)filesystem->previousReadCounter())) {
	diff = now - filesystem->lastUpdateTime();

	// update read rate
	filesystem->setReadRate((u_signed64)(
	  0.5 * (rsect - filesystem->previousReadCounter()) / diff)
	);

	// update write rate
	filesystem->setWriteRate((u_signed64)(
	  0.5 * (wsect - filesystem->previousWriteCounter()) / diff)
	);
      } 
    }
    
    // record values for later
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
    e.getMessage()
      << "MetricsThread::collectFileSystemMetrics : statfs call failed";
    throw e;
  }
}
