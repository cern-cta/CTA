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
#include "castor/monitoring/rmnode/MetricsThread.hpp"
#include "castor/monitoring/rmnode/RmNodeConfig.hpp"
#include "castor/monitoring/DiskServerMetricsReport.hpp"
#include "castor/monitoring/FileSystemMetricsReport.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/MessageAck.hpp"
#include "castor/IObject.hpp"
#include <sys/sysinfo.h>
#include "getconfent.h"
#include <sys/vfs.h>
#include "errno.h"

/// The XFS stat file
#define XFS_STAT_FILE "/proc/fs/xfs/stat"

/// The partition file
#define PARTITIONS_FILE "/proc/partitions"

/// generic stat file
#define STAT_FILE "/proc/stat"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::monitoring::rmnode::MetricsThread::MetricsThread
(std::string rmMasterHost, int rmMasterPort) :
  m_rmMasterHost(rmMasterHost), m_rmMasterPort(rmMasterPort) {
  // "Metrics thread created"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 7, 0, 0);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::monitoring::rmnode::MetricsThread::~MetricsThread() throw() {
  // "Metrics thread destructed"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 13, 0, 0);
}

//------------------------------------------------------------------------------
// runs the thread starts by threadassign()
//------------------------------------------------------------------------------
void castor::monitoring::rmnode::MetricsThread::run(void* par)
  throw(castor::exception::Exception) {
  // "Metrics thread started"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 9, 0, 0);
  castor::monitoring::DiskServerMetricsReport* metrics = 0;
  try {
    // collect
    metrics = collectDiskServerMetrics();
    // send metrics to rmMaster
    castor::io::ClientSocket s(m_rmMasterPort, m_rmMasterHost);
    s.connect();
    s.sendObject(*metrics);
    // "Metrics sent to rmMaster"
    castor::dlf::Param params[] =
      {castor::dlf::Param("content", metrics)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 17, 1, params);
    // cleanup memory
    delete metrics;
    metrics = 0;
    // check the acknowledgment
    castor::IObject* obj = s.readObject();
    castor::MessageAck* ack =
      dynamic_cast<castor::MessageAck*>(obj);
    if (0 == ack) {
      castor::exception::InvalidArgument e; // XXX To be changed
      e.getMessage() << "No Acknowledgement from the Server";
      throw e;
    }
  }
  catch(castor::exception::Exception e) {
    // cleanup
    if (0 != metrics) {
      delete metrics;
      metrics = 0;
    }
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
castor::monitoring::DiskServerMetricsReport*
castor::monitoring::rmnode::MetricsThread::collectDiskServerMetrics()
  throw(castor::exception::Exception) {
  castor::monitoring::DiskServerMetricsReport* metrics =
    new castor::monitoring::DiskServerMetricsReport();
  // use sysinfo to get all data
  struct sysinfo si;
  if (0 == sysinfo(&si)) {
    // RAM
    u_signed64 ram = si.freeram;
    ram *= si.mem_unit;
    metrics->setFreeRam(ram);
    // Memory
    u_signed64 memory = si.freehigh;
    memory *= si.mem_unit;
    metrics->setFreeMemory(memory);
    // Swap
    u_signed64 swap = si.freeswap;
    swap *= si.mem_unit;
    metrics->setFreeSwap(swap);
    // Load
    metrics->setLoad(si.loads[0]);
  } else {
    delete metrics;
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectDiskServerMetrics : sysinfo call failed";
    throw e;
  }
  // get current list of filesystems
  char** fs;
  int nbFs;
  if (getconfent_multi_fromfile
      (RMNODECONFIGFILE, "RMNODE", "MOUNTPOINT", 0, &fs, &nbFs) < 0) {
    castor::exception::Exception e(errno);
    delete metrics;
    e.getMessage()
      << "MetricsThread::collectDiskServerMetrics : getconfent_multi_fromfile failed";
    throw e;
  }
  // fill state for each FileSystems
  try {
    for (int i = 0; i < nbFs; i++) {
      metrics->addFileSystemMetricsReports(collectFileSystemMetrics(fs[i]));
    }
  } catch (castor::exception::Exception e) {
    delete metrics;
    throw e;
  }
  return metrics;
}

//------------------------------------------------------------------------------
// collectFileSystemMetrics
//------------------------------------------------------------------------------
castor::monitoring::FileSystemMetricsReport*
castor::monitoring::rmnode::MetricsThread::collectFileSystemMetrics
(std::string mountPoint)
  throw(castor::exception::Exception) {
  castor::monitoring::FileSystemMetricsReport* metrics =
    new castor::monitoring::FileSystemMetricsReport();
  // Set streams
  int nr, nrw, nw;
  if (Crm_util_countstream(mountPoint.c_str(), &nr, &nrw, &nw) < 0) {
    delete metrics;
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectFileSystemMetrics : Crm_util_countstream failed";
    throw e;
  }
  metrics->setNbReadStreams(nr);
  metrics->setNbWriteStreams(nw);
  metrics->setNbReadWriteStreams(nrw);
  // Set rates
  u_signed64 r, w;
  if (Crm_util_io_wrapper(mountPoint.c_str(), &r, &w, nr, nw, nrw) < 0) {
    delete metrics;
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectFileSystemMetrics : Crm_util_io failed";
    throw e;
  }
  metrics->setReadRate(r);
  metrics->setWriteRate(w);
  // Set free space
  struct statfs sf;
  if (statfs(mountPoint.c_str(),&sf) == 0) {
    metrics->setFreeSpace(((u_signed64) sf.f_bavail) * ((u_signed64) sf.f_bsize));
  } else {
    delete metrics;
    castor::exception::Exception e(errno);
    e.getMessage()
      << "MetricsThread::collectFileSystemMetrics : statfs call failed";
    throw e;
  }
  return metrics;
}

//////////////////////////////////////////////////////////////////
// XXX    old code imported from previous implementation    XXX //
// XXX  To be reviewd and probably simplified and improved  XXX //
//////////////////////////////////////////////////////////////////

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "Csnprintf.h"
#include "Cglobals.h"
#include <fcntl.h>
#include "log.h"

int castor::monitoring::rmnode::MetricsThread::Crm_util_countstream
(const char* fs, int*nr, int*nrw, int*nw) {
  int rc = -1;
  int fslen = strlen(fs);
  int nrresult = 0;
  int nrwresult = 0;
  int nwresult = 0;
  DIR *proc_dir;
  if ((proc_dir = opendir("/proc")) == NULL) {
    serrno = errno;
  } else {
    int have_error = 0;
    int proc_dir_return_code;
    struct dirent proc_dir_entry;
    struct dirent *proc_dir_result;
    for (proc_dir_return_code = readdir_r(proc_dir, &proc_dir_entry, &proc_dir_result);
         proc_dir_result != NULL && proc_dir_return_code == 0;
         proc_dir_return_code = readdir_r(proc_dir, &proc_dir_entry, &proc_dir_result)) {
      char *fullpath;
      DIR *fd_dir;
      int fd_dir_return_code;
      struct dirent fd_dir_entry;
      struct dirent *fd_dir_result;
      /* We accept entries that have digits only */
      if (strspn(proc_dir_entry.d_name,"1234567890") != strlen(proc_dir_entry.d_name)) {
        continue;
      }
      /* We try to open right now /proc/proc_dir_entry.d_name/fd/ */
      if ((fullpath = (char *) malloc(strlen("/proc/") + strlen(proc_dir_entry.d_name) + strlen("/fd") + 1)) == NULL) {
        have_error++;
        break;
      }
      strcpy(fullpath,"/proc/");
      strcat(fullpath,proc_dir_entry.d_name);
      strcat(fullpath,"/fd");
      fd_dir = opendir(fullpath);
      free(fullpath);
      if (fd_dir == NULL) {
        /* This should not happen but we anyway continue */
        continue;
      }
      for (fd_dir_return_code = readdir_r(fd_dir, &fd_dir_entry, &fd_dir_result);
           fd_dir_result != NULL && fd_dir_return_code == 0;
           fd_dir_return_code = readdir_r(fd_dir, &fd_dir_entry, &fd_dir_result)) {
        struct stat statbuf;
        char lpath[CA_MAXPATHLEN+1];
        /* Digits only */
        if (strspn(fd_dir_entry.d_name,"1234567890") != strlen(fd_dir_entry.d_name)) {
          continue;
        }
        /* We will try to lstat the /proc/<pid>/fd/<fd> file */
        if ((fullpath = (char *) malloc(strlen("/proc/") + strlen(proc_dir_entry.d_name) + strlen("/fd/") + strlen(fd_dir_entry.d_name) + 1)) == NULL) {
          have_error++;
          break;
        }
        strcpy(fullpath,"/proc/");
        strcat(fullpath,proc_dir_entry.d_name);
        strcat(fullpath,"/fd/");
        strcat(fullpath,fd_dir_entry.d_name);
        if (lstat(fullpath,&statbuf) != 0) {
          /* Oups */
          free(fullpath);
          continue;
        }
        if (!S_ISLNK(statbuf.st_mode)) {
          /* Not a link !? */
          free(fullpath);
          continue;
        }
        lpath[0] = '\0';
        if (readlink(fullpath,lpath,CA_MAXPATHLEN) < 0) {
          /* Fail to read the link !? */
          free(fullpath);
          continue;
        }
        free(fullpath);
        /* We have in lpath the full file name */
        /* We could be more intelligent but we suppose */
        /* there is no links below, so we just do an */
        /* strncmp */
        if (strncmp(lpath,fs,fslen) != 0) {
          /* Not the filesystem we are looking for */
          continue;
        }
        /* Fine - let's update the counters using statbuf */

        if ((statbuf.st_mode & S_IRUSR) == S_IRUSR) {
          /* Read permission */
          if ((statbuf.st_mode & S_IWUSR) == S_IWUSR) {
            /* And write permission */
            nrwresult++;
          } else {
            /* But no write permission */
            nrresult++;
          }
        } else if ((statbuf.st_mode & S_IWUSR) == S_IWUSR) {
          /* Write permission */
          nwresult++;
        }
      }
      closedir(fd_dir);
    }
    closedir(proc_dir);
    if (have_error == 0) {
      /* No error ! */
      rc = 0;
    }
  }
  if (rc == 0) {
    *nr = nrresult;
    *nw = nwresult;
    *nrw = nrwresult;
  }
  return(rc);
}

static int fd_proc_fs_xfs_stat_key = 0;
static int fd_proc_fs_xfs_stat_static = -1;
#define fd_proc_fs_xfs_stat (*C__fd_proc_fs_xfs_stat())

static int *C__fd_proc_fs_xfs_stat() {
  int *var, rc;
  rc = Cglobals_get(&fd_proc_fs_xfs_stat_key, (void **) &var, sizeof(int));
  /* If error, var will be NULL */
  if (var == NULL) {
    return(&fd_proc_fs_xfs_stat_static);
  }
  if (rc == 1) {
    /* First time */
    *var = -1;
  }
  return(var);
}

/* Later in the code you could say that I could/should have used << 11 ... Yes. */
/* Util macro from procps */
#define FILE_TO_BUF(filename, fd, buf) { \
    int n;                               \
    if (fd == -1 && (fd = open(filename, O_RDONLY)) == -1) { \
	  serrno = errno;                    \
	  return(-1);                        \
    }                                    \
    if (lseek(fd, 0, SEEK_SET) != 0) {   \
	  close(fd);                         \
	  fd = -1;                           \
      serrno = errno;                    \
      return(-1);                        \
	}                                    \
	if ((n = read(fd, buf, sizeof(buf) - 1)) < 0) { \
	  serrno = errno;                    \
	  close(fd);                         \
	  fd = -1;                           \
	  return(-1);                        \
    }                                    \
    buf[n] = '\0';                       \
}

static int fd_proc_partitions_key = 0;
#define fd_proc_partitions (*C__fd_proc_partitions())
static int fd_proc_partitions_static = -1;

static int *C__fd_proc_partitions() {
  int *var, rc;
  rc = Cglobals_get(&fd_proc_partitions_key, (void **) &var, sizeof(int));
  /* If error, var will be NULL */
  if (var == NULL) {
    return(&fd_proc_partitions_static);
  }
  if (rc == 1) {
    /* First time */
    *var = -1;
  }
  return(var);
}

#include <linux/major.h>
#if ! defined (SCSI_DISK0_MAJOR)
#define SCSI_DISK0_MAJOR	8
#endif
#if ! defined (MD_MAJOR)
#define MD_MAJOR	9
#endif

#if !defined(IDE4_MAJOR)
#define IDE4_MAJOR	56
#endif
#if !defined(IDE5_MAJOR)
#define IDE5_MAJOR	57
#endif
#if !defined(IDE6_MAJOR)
#define IDE6_MAJOR	88
#endif
#if !defined(IDE7_MAJOR)
#define IDE7_MAJOR	89
#endif
#if !defined(IDE8_MAJOR)
#define IDE8_MAJOR	90
#endif
#if !defined(IDE9_MAJOR)
#define IDE9_MAJOR	91
#endif
#if !defined(DAC960_MAJOR)
#define DAC960_MAJOR	48
#endif
#if !defined(COMPAQ_SMART2_MAJOR)
#define COMPAQ_SMART2_MAJOR	72
#endif
#if !defined(COMPAQ_CISS_MAJOR)
#define COMPAQ_CISS_MAJOR	104
#endif
#if !defined(LVM_BLK_MAJOR)
#define LVM_BLK_MAJOR 58
#endif

struct _disk_name_map {
	char	*name;
	int	major;
	int	minor_mod;
	char	suffix_base;
};
static struct _disk_name_map disk_name_map[] = {
	{"hd",	IDE0_MAJOR,					64,	'a' },	/* 3:  hda, hdb */
	{"hd",	IDE1_MAJOR,					64,	'c' },	/* 22: hdc, hdd */
	{"hd",	IDE2_MAJOR,					64,	'e' },	/* 33: hde, hdf */
	{"hd",	IDE3_MAJOR,					64,	'g' },	/* 34: hdg, hdh */
	{"hd",	IDE4_MAJOR,					64,	'i' },	/* 56: hdi, hdj */
	{"hd",	IDE5_MAJOR,					64,	'k' },	/* 57: hdk, hdl */
	{"hd",	IDE6_MAJOR,					64,	'm' },	/* 88: hdm, hdn */
	{"hd",	IDE7_MAJOR,					64,	'o' },	/* 89: hdo, hdp */
	{"hd",	IDE8_MAJOR,					64,	'q' },	/* 90: hdq, hdr */
	{"hd",	IDE9_MAJOR,					64,	's' },	/* 91: hds, hdt */
	{"sd",	SCSI_DISK0_MAJOR,			16,	'a' },	/* 8:  sda-sdh */
	{"sg",	SCSI_GENERIC_MAJOR,			0,	'0' },	/* 21: sg0-sg16 */
	{"scd",	SCSI_CDROM_MAJOR,			0,	'0' },	/* 11: scd0-scd16 */
	{"md",	MD_MAJOR,					0,	'0' },	/* 9:  md0-md3 */
	
	{"c0d",	DAC960_MAJOR,				0,	'0' },	/* 48:  c0d0-c0d31 */
	{"c1d",	DAC960_MAJOR + 1,			0,	'0' },	/* 49:  c1d0-c1d31 */
	{"c2d",	DAC960_MAJOR + 2,			0,	'0' },	/* 50:  c2d0-c2d31 */
	{"c3d",	DAC960_MAJOR + 3,			0,	'0' },	/* 51:  c3d0-c3d31 */
	{"c4d",	DAC960_MAJOR + 4,			0,	'0' },	/* 52:  c4d0-c4d31 */
	{"c5d",	DAC960_MAJOR + 5,			0,	'0' },	/* 53:  c5d0-c5d31 */
	{"c6d",	DAC960_MAJOR + 6,			0,	'0' },	/* 54:  c6d0-c6d31 */
	{"c7d",	DAC960_MAJOR + 7,			0,	'0' },	/* 55:  c7d0-c7d31 */
	
	{"cs0d", COMPAQ_SMART2_MAJOR,		0,	'0' },	/* 72:  c0d0-c0d15 */
	{"cs1d", COMPAQ_SMART2_MAJOR + 1,	0,	'0' },	/* 73:  c1d0-c1d15 */
	{"cs2d", COMPAQ_SMART2_MAJOR + 2,	0,	'0' },	/* 74:  c2d0-c2d15 */
	{"cs3d", COMPAQ_SMART2_MAJOR + 3,	0,	'0' },	/* 75:  c3d0-c3d15 */
	{"cs4d", COMPAQ_SMART2_MAJOR + 4,	0,	'0' },	/* 76:  c4d0-c4d15 */
	{"cs5d", COMPAQ_SMART2_MAJOR + 5,	0,	'0' },	/* 77:  c5d0-c5d15 */
	{"cs6d", COMPAQ_SMART2_MAJOR + 6,	0,	'0' },	/* 78:  c6d0-c6d15 */
	{"cs7d", COMPAQ_SMART2_MAJOR + 7,	0,	'0' },	/* 79:  c7d0-c7d15 */
	
	{"cc0d", COMPAQ_CISS_MAJOR,			0,	'0' },	/* 104:  c0d0-c0d15 */
	{"cc1d", COMPAQ_CISS_MAJOR + 1,		0,	'0' },	/* 105:  c1d0-c1d15 */
	{"cc2d", COMPAQ_CISS_MAJOR + 2,		0,	'0' },	/* 106:  c2d0-c2d15 */
	{"cc3d", COMPAQ_CISS_MAJOR + 3,		0,	'0' },	/* 107:  c3d0-c3d15 */
	{"cc4d", COMPAQ_CISS_MAJOR + 4,		0,	'0' },	/* 108:  c4d0-c4d15 */
	{"cc5d", COMPAQ_CISS_MAJOR + 5,		0,	'0' },	/* 109:  c5d0-c5d15 */
	{"cc6d", COMPAQ_CISS_MAJOR + 6,		0,	'0' },	/* 110:  c6d0-c6d15 */
	{"cc7d", COMPAQ_CISS_MAJOR + 7,		0,	'0' },	/* 111:  c7d0-c7d15 */

	{"fd",	FLOPPY_MAJOR,				0,	'0' }	/* 2:  fd0-fd3  */
};

static int fd_proc_stat_key = 0;
#define fd_proc_stat (*C__fd_proc_stat())
static int fd_proc_stat_static = -1;

static int *C__fd_proc_stat() {
	int *var, rc;

	rc = Cglobals_get(&fd_proc_stat_key, (void **) &var, sizeof(int));
	/* If error, var will be NULL */
	if (var == NULL) {
		return(&fd_proc_stat_static);
	}
	if (rc == 1) {
		/* First time */
		*var = -1;
	}
	return(var);
}

int castor::monitoring::rmnode::MetricsThread::Crm_util_io
(const char* part, u_signed64 *rd, u_signed64 *wr) {
  u_signed64 result1 = 0;
  u_signed64 result2 = 0;
  u_signed64 result1_from_disk = 0;
  u_signed64 result2_from_disk = 0;
  int rc = -1;
  int found = 0, found_from_disk = 0;
  const char *p;
  char buf[4096];
#ifdef USE_PARTITION_FS_IO_IF_AVAILABLE
  char bufmatch_part[4096];
#endif
  char bufmatch_disk[4096];
  char bufmatch_disk_minimal[4096];
  int major_disk = -1;
  int minor_disk = -1;
  int n, i_disk, major = -1, minor = -1;
  unsigned long sectors;
  int found_major_minor = 0;
  char disk[CA_MAXPATHLEN+1];
  const char *diskbasename;
  size_t  thissize;

  /* Linux hook only : if partition is (forced to be) xfs */
  /* then we will look into /proc/fs/xfs/stat */

  /* Thank you to Peter Kelemen (Peter.Kelemen@cern.ch) for helping us in here */
  if (strcmp(part,"xfs") == 0) {
    FILE_TO_BUF(XFS_STAT_FILE, fd_proc_fs_xfs_stat, buf);
    if ((p = strstr(buf, "xpc ")) != NULL) {
      unsigned long long int xfs_strat, xfs_write, xfs_read;

      p += strlen("xpc ");
      n = sscanf(p, "%llu %llu %llu", &xfs_strat, &xfs_write, &xfs_read);
      if (n == 3) {
        /* Our caller expects output in blocks of 512 */
        result1 = (xfs_read >> 9);
        result2 = (xfs_write >> 9);
        rc = 0;
      }
    }
  } else {
    /* Strip numbers for partition to get disk */
    strcpy(disk,part);
    if ((thissize = strcspn(disk,"1234567890")) > 0) {
      disk[thissize] = '\0';
    }

    FILE_TO_BUF(PARTITIONS_FILE, fd_proc_partitions, buf);
    /* major minor  #blocks  name
       |  rio rmerge rsect ruse wio wmerge wsect wuse running use aveq
    */

#ifdef USE_PARTITION_FS_IO_IF_AVAILABLE
    /* Take basename of partition */
    if ((p = strrchr(part,'/')) != NULL) {
      if (*++p == '\0') {
        log(LOG_ERR,"%s: Partition %s must not end with the '/' character\n", part);
        p = part;
      }
    } else {
      p = part;
    }
    Csnprintf(bufmatch_part, sizeof(bufmatch_part), "%%d %%d %%lu %s %%*d %%*d %%lu %%*d %%*d %%*d %%lu", p);
    bufmatch_part[4095] = '\0';
#endif

    /* Take basename of disk */
    if ((p = strrchr(disk,'/')) != NULL) {
      if (*++p == '\0') {
        log(LOG_ERR,"%s: Disk %s must not end with the '/' character\n", part);
        p = part;
      }
    } else {
      p = part;
    }
    diskbasename = p;
    Csnprintf(bufmatch_disk, sizeof(bufmatch_disk), "%%d %%d %%lu %s %%*d %%*d %%lu %%*d %%*d %%*d %%lu", p);
    bufmatch_disk[4095] = '\0';
    Csnprintf(bufmatch_disk_minimal, sizeof(bufmatch_disk), "%%d %%d %%lu %s", p);
    bufmatch_disk_minimal[4095] = '\0';

    p = strtok(buf,"\n");
    while (p != NULL) {

#ifdef USE_PARTITION_FS_IO_IF_AVAILABLE
      /* Try to match the partition */
      n = sscanf(p, bufmatch_part, &major, &minor, &sectors, &thisrd, &thiswr);

      if (n == 5 && sectors > 1 && major != LVM_BLK_MAJOR) {

        result1 = thisrd;
        result2 = thiswr;
        rc = 0;
        found = 1;
        break;
      }
#endif /* USE_PARTITION_FS_IO_IF_AVAILABLE */
#ifdef USE_PARTITION_DISK_IO_IF_AVAILABLE
      if (! found_from_disk) {
        /* Try to match the disk, we will certainly not have the i/o, but at least the major/minor */
        n = sscanf(p, bufmatch_disk, &major, &minor, &sectors, &thisrd, &thiswr);

        if (n == 5 && sectors > 1 && major != LVM_BLK_MAJOR) {

          major_disk = major;
          minor_disk = minor;
          found_major_minor = 1;
          result1_from_disk = thisrd;
          result2_from_disk = thiswr;
          found_from_disk = 1;
        }
      }
#endif /* USE_PARTITION_DISK_IO_IF_AVAILABLE */

      if (! found_major_minor) {
        
        n = sscanf(p, bufmatch_disk_minimal, &major, &minor, &sectors);
        
        /* Hmmm... Last match is a string, can be partial */
        if (n == 3 && sectors > 1 && major != LVM_BLK_MAJOR) {
          char *cthis;

          if ((cthis = strstr(p, diskbasename)) != NULL) {

            if ((cthis > p) && (*(cthis-1) == ' ') && ((cthis[strlen(diskbasename)] == '\0') || (cthis[strlen(diskbasename)] == ' ') || (cthis[strlen(diskbasename)] == '\t') || (cthis[strlen(diskbasename)] == '\n'))) {
              /* Last string is really our disk (either it is end of line or it is a blank/tab/newline */
              major_disk = major;
              minor_disk = minor;
              found_major_minor = 1;
            }
          }
        }
      }

      p = strtok(NULL,"\n");
    }
    if ((! found) && (found_from_disk)) {
      /* We got information from disk, not from partition, but that's quite ok */
      result1 = result1_from_disk;
      result2 = result2_from_disk;
      found = 1;
      rc = 0;
    }
    if ((! found) && (major_disk >= 0) && (minor_disk >= 0)) {
      int disk_index;
      char *q;
      unsigned int i;
      struct _disk_name_map *dm = NULL;

      /* We want to have the disk index */
      for (i = 0; i < sizeof(disk_name_map) / sizeof(struct _disk_name_map); ++i) {
        if (major_disk != disk_name_map[i].major) {
          continue;
        }
        dm = &disk_name_map[i];
        break;
      }

      if (dm) {
        int got_result1 = 0;
        int got_result2 = 0;
        disk_index = minor_disk;
        if (dm->minor_mod > 0 && minor_disk >= dm->minor_mod) {
          disk_index /= dm->minor_mod;
        }
        strcpy(bufmatch_disk,"(%d,%d):(%*d,%lu,%lu,%lu,%lu)");

        /* Give it a try with /proc/stat */
        FILE_TO_BUF(STAT_FILE, fd_proc_stat, buf);

        if ((q = strstr(buf, "disk_io: ")) != NULL) { /* Kernel 2.4 format */
          q += strlen("disk_io: ");

          if ((p = strtok(q," \t\n")) != NULL) {
            while (p != NULL) {
              unsigned long rb1, rb2, wb1, wb2;

              if (strchr(p,':') == NULL) {
                /* End of disk_io */
                break;
              }

              /* disk_io lines in 2.4.x kernels have had 2 formats */
              n = sscanf(p, bufmatch_disk, &major, &i_disk, &rb1, &rb2, &wb1, &wb2);
              if (major == major_disk && i_disk == disk_index) {
                if (n == 6) { /* patched as of 2.4.0-test1-ac9 */
                  /* (major,disk):(total_io,rio,rblk,wio,wblk) */
                  result1 = rb2;
                  result2 = wb2;
                  got_result1 = got_result2 = 1;
                } else { /* 2.3.99-pre8 to 2.4.0-testX */
                  /* (major,disk):(rio,rblk,wio,wblk) */
                  result1 = rb1;
                  result2 = wb1;
                  got_result1 = got_result2 = 1;
                }
              }
              p = strtok(NULL," \t\n");
            }
          }
        } else {
          char *qrblk;
          char *qwblk;
          if (((qrblk = strstr(buf, "disk_rblk ")) != NULL) &&
              ((qwblk = strstr(buf, "disk_wblk ")) != NULL)) { /* Pre kernel 2.4 format */
            int disk_nb;

            /* Because of strtok we parse the one that is the latest */
            if (qwblk > qrblk) {
              goto do_wblk;
            }

          do_rblk:
            disk_nb = 0;
            q = qrblk;
            q += strlen("disk_rblk ");

            if ((p = strtok(q," \t\n")) != NULL) {
              while (p != NULL) {
                if (disk_nb++ == disk_index) {
                  result1 = atoi(p);
                  got_result1 = 1;
                  break;
                }
                p = strtok(NULL," \t\n");
              }
            }
            if (qwblk <= qrblk) {

            do_wblk:
              disk_nb = 0;
              q = qwblk;
              q += strlen("disk_wblk ");

              if ((p = strtok(q," \t\n")) != NULL) {
                while (p != NULL) {
                  if (disk_nb++ == disk_index) {
                    result2 = atoi(p);
                    got_result2 = 1;
                    break;
                  }
                  p = strtok(NULL," \t\n");
                }
              }
              if (qwblk > qrblk) {
                goto do_rblk;
              }
            }
          }
        }
        if (got_result1 && got_result2) {
          rc = 0;
        }
      }
    }
  }
  if (rc == 0) {
    *rd = result1; /* In blocks of 512 */
    *wr = result2; /* In blocks of 512 */
  } else {
    serrno = EINVAL;
  }
  return(rc);
}

#include <sys/time.h>
#include <time.h>
#include "rm_constants.h"
#include "rm_struct.h"
#include "Cthread_api.h"

int nfilesystems = 0;
int nfilesystems_with_xfs = 0;
/* We need a synch when checking number of streams per filesystem */
void *_allfs_cthread_structure = NULL; /* Cthread structure for restart mutex */
struct Crm_filesystem *allfs = NULL;

int castor::monitoring::rmnode::MetricsThread::Crm_util_io_wrapper
(const char* part, u_signed64 *rdRate, u_signed64 *wrRate,
 u_signed64 nr, u_signed64 nrw, u_signed64 nw) {
  // very bad way of having some memory...
  static u_signed64 previous_rd = 0;
  static u_signed64 previous_wr = 0;
  static u_signed64 previous_precise_time = 0;
  static u_signed64 previous_precise_microtime = 0;
  char* func = "Crm_util_io_wrapper";
  u_signed64 rd, wr;

  if (Crm_util_io(part,&rd,&wr) != 0) {
    log(LOG_ERR,"%s: Crm_util_io error, %s\n", func, sstrerror(serrno));
    /* Reset */
    previous_precise_time = 0;
    return -1;
  } else if ((rd < previous_rd) || (wr < previous_wr)) {
    /* Wrap around */
    log(LOG_ERR,"%s: Crm_util_io error, counters wraparound\n", func);
    /* Reset */
    previous_precise_time = 0;
    return -1;
  } else {
    struct timeval thistime;
    u_signed64 current_precise_time = 0, current_precise_microtime = 0;
    if (gettimeofday(&thistime,NULL) < 0) {
      log(LOG_ERR,"%s: gettimeofday error, %s\n", func, strerror(errno));
      /* Reset */
      current_precise_time = previous_precise_time = 0;
    } else {
      current_precise_time = thistime.tv_sec;
      current_precise_microtime = thistime.tv_usec;
    }
    if ((previous_precise_time > 0) && (current_precise_time >= previous_precise_time)) {
      float r_rate_corr = 1.;
      float w_rate_corr = 1.;
      float diff_time = current_precise_time - previous_precise_time;
      /* current_precise_time + 1000000*current_precise_microtime */
      /* previous_precise_time + 1000000*previous_precise_microtime */
      if (current_precise_microtime >= previous_precise_microtime) {
        diff_time -= (current_precise_microtime - previous_precise_microtime) / 1000000. ;
      } else {
        diff_time -= (previous_precise_microtime - current_precise_microtime) / 1000000. ;
      }
      if (strcmp(part,"xfs") == 0) {
        /* We will apply a correction factor because what we have read */
        /* is a shared I/O state on the whole system */
        if (nfilesystems_with_xfs <= 0) {
          log(LOG_ERR,"%s: Crm_util_io warning on \"??\", partition \"%s\", total number of xfs partition is <= 0 !?\n", func, part);
        } else {
          /* We need to synch with the number of opened streams on all the filesystems */
          if (Cthread_mutex_timedlock_ext(_allfs_cthread_structure,RMMUTEXTIMEOUT) != 0) {
            log(LOG_ERR, "Cthread_mutex_timedlock_ext error: %s\n", sstrerror(serrno));
          } else {
            int tot_n_rdonly = 0;
            int tot_n_wronly_or_rdwr = 0;
            int j;
            /* We count the number of read and of write streams */
            for (j = 0; j < nfilesystems; j++) {
              tot_n_rdonly += allfs[j].n_rdonly;
              tot_n_wronly_or_rdwr += allfs[j].n_rdwr + allfs[j].n_wronly;
            }
            if ((tot_n_rdonly > 0) && (nr > 0)) {
              r_rate_corr = (float) nr / (float) tot_n_rdonly;
            } else {
              /* No stream accounted ? Do global distribution */
              r_rate_corr = (float) 1. / nfilesystems;
            }
            if ((tot_n_wronly_or_rdwr > 0) && ((nrw + nw) > 0)) {
              w_rate_corr = (float) (nrw + nw) / (float) tot_n_wronly_or_rdwr;
            } else {
              /* No stream accounted ? Do global distribution */
              w_rate_corr = (float) 1. / nfilesystems;
            }
          }
          if (Cthread_mutex_unlock_ext(_allfs_cthread_structure) != 0) {
            log(LOG_ERR, "%s: Cthread_mutex_unlock_ext error : %s\n", func, sstrerror(serrno));
          }
        }
      }
      u_signed64 r_rate = (rd >= previous_rd) ? rd - previous_rd : 0;
      u_signed64 w_rate = (wr >= previous_wr) ? wr - previous_wr : 0;
      /* Apply correction factor if any */
      r_rate = (u_signed64) (r_rate * r_rate_corr);
      w_rate = (u_signed64) (w_rate * w_rate_corr);
      /* 512bytes + <<9 on the left -> bytes + >>20 on the right -> MBytes */
      r_rate >>= 11;
      w_rate >>= 11;
      r_rate = (u_signed64) (r_rate / diff_time);
      w_rate = (u_signed64) (w_rate / diff_time);
      *rdRate = r_rate;
      *wrRate = w_rate;
    }
    previous_rd = rd;
    previous_wr = wr;
    previous_precise_time = current_precise_time;
    previous_precise_microtime = current_precise_microtime;
    return 0;
  }
}
