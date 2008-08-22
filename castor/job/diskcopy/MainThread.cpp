/******************************************************************************
 *                      MainThread.cpp
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
 * @(#)$RCSfile: MainThread.cpp,v $ $Revision: 1.15 $ $Release$ $Date: 2008/08/22 13:04:37 $ $Author: kotlyar $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/job/diskcopy/MainThread.hpp"
#include "castor/job/SharedResourceHelper.hpp"
#include "castor/job/RfioMover.hpp"
#include "castor/System.hpp"
#include "Cgetopt.h"
#include "u64subr.h"
#include "getconfent.h"

#include <errno.h>
#include <pwd.h>
#include <grp.h>
#include <unistd.h>

// Defaults
#define DEFAULT_RETRY_ATTEMPTS 60
#define DEFAULT_RETRY_INTERVAL 10


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::job::diskcopy::MainThread::MainThread(int argc, char *argv[])
  throw(castor::exception::Exception) :
  m_mover(0),
  m_protocol("Rfio"),
  m_requestUuid(nullCuuid),
  m_subRequestUuid(nullCuuid),
  m_svcClass(""),
  m_diskCopyId(0),
  m_sourceDiskCopyId(0),
  m_resourceFile(""),
  m_requestCreationTime(0) {

  // Initialize the Cns_fileid structure
  memset(&m_fileId, 0, sizeof(m_fileId));

  // For statistical purposes
  timeval tv;
  gettimeofday(&tv, NULL);
  m_startTime = (tv.tv_sec * 1000000) + tv.tv_usec;

  // Process the command line arguments. Note: we log the error to stderr here
  // and not DLF as the most probable cause for this to fail is user error
  // running the diskCopyTransfer mover manually.
  try {
    parseCommandLine(argc, argv);
  } catch (castor::exception::Exception e) {
    std::cerr << argv[0] << ": " << e.getMessage().str() << std::endl;
    help(argv[0]);
    exit(USERR);
  }

  // Drop privileges to the stage superuser.
  try {
    changeUidGid(STAGERSUPERUSER, STAGERSUPERGROUP);
  } catch (castor::exception::Exception e) {

    // "Failed to change uid and gid"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", strerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Username", STAGERSUPERUSER),
       castor::dlf::Param("Groupname", STAGERSUPERGROUP),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 20, 5, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  }

  // Initialize the remote job service
  castor::IService *orasvc =
    castor::BaseObject::services()->service
    ("RemoteJobSvc", castor::SVC_REMOTEJOBSVC);
  if (orasvc == 0) {
    // "Unable to get RemoteJobSvc, transfer terminated"
    castor::dlf::Param params[] =
      {castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 10, 1, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  }
  m_jobSvc = dynamic_cast<castor::stager::IJobSvc *>(orasvc);
  if (m_jobSvc == 0) {
    // "Could not convert newly retrieved service into IJobSvc"
    castor::dlf::Param params[] =
      {castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 11, 1, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  }

  // Determine the number of times that the shared resource helper should try
  // to download the resource file from the shared resource URL
  char *value = getconfent("DiskCopy", "RetryAttempts", 0);
  int attempts = DEFAULT_RETRY_ATTEMPTS;
  if (value) {
    attempts = std::strtol(value, 0, 10);
    if (attempts == 0) {
      attempts = 1;
    } else if (attempts < 0) {
      attempts = DEFAULT_RETRY_ATTEMPTS;

      // "Invalid DiskCopy/RetryInterval option, using default"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Default", attempts),
	 castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_WARNING, 12, 2, params, &m_fileId);
    }
  }

  // Extract the value of the retry interval. This value determines how long
  // we sleep between retry attempts
  value = getconfent("DiskCopy", "RetryInterval", 0);
  int interval = DEFAULT_RETRY_INTERVAL;
  if (value) {
    interval = std::strtol(value, 0, 10);
    if (interval < 1) {
      interval = DEFAULT_RETRY_INTERVAL;

      // "Invalid DiskCopy/RetryAttempts option, value too small. Using default"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Default", interval),
	 castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_WARNING, 13, 2, params, &m_fileId);
    }
  }

  // Initialize the mover. For now we only support RFIO!
  try {
    m_mover = new castor::job::RfioMover();
  } catch (castor::exception::Exception e) {

    // "Failed to initialize mover"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Protocol", m_protocol),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 14, 3, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  }

  // Initialize the shared resource helper
  try {
    m_resHelper =
      new castor::job::SharedResourceHelper(attempts, interval);
  } catch (castor::exception::Exception e) {

    // "Failed to create sharedResource helper"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 15, 3, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  }
}


//-----------------------------------------------------------------------------
// ParseCommandLine
//-----------------------------------------------------------------------------
void castor::job::diskcopy::MainThread::parseCommandLine
(int argc, char *argv[])
  throw(castor::exception::Exception) {

  // Command line options. Note: the defaults are listed here but not parsed.
  // Default parsing is the responsibility of the BaseServer.
  Coptions_t longopts[] = {

    // Defaults
    { "config",        REQUIRED_ARGUMENT, NULL, 'c' },
    { "help",          NO_ARGUMENT,       NULL, 'h' },

    // These options are for logging purposes only!
    { "request",       REQUIRED_ARGUMENT, NULL, 'r' },
    { "subrequest",    REQUIRED_ARGUMENT, NULL, 's' },

    // The nameserver invariants
    { "fileid",        REQUIRED_ARGUMENT, NULL, 'F' },
    { "nshost",        REQUIRED_ARGUMENT, NULL, 'H' },

    // Source and destination diskcopy ids
    { "destdc",        REQUIRED_ARGUMENT, NULL, 'D' },
    { "srcdc",         REQUIRED_ARGUMENT, NULL, 'X' },

    // Resources
    { "svcclass",      REQUIRED_ARGUMENT, NULL, 'S' },
    { "resfile",       REQUIRED_ARGUMENT, NULL, 'R' },

    // Logging/statistics
    { "rcreationtime", REQUIRED_ARGUMENT, NULL, 't' },
    { NULL,            0,                 NULL, 0   }
  };

  Coptind   = 1;
  Copterr   = 1;
  Coptreset = 1;

  // Parse command line arguments
  char c;
  while ((c = Cgetopt_long(argc, argv, "c:hr:s:F:H:D:X:S:R:t:", longopts, NULL)) != -1) {
    switch (c) {
    case 'c':
    case 'h':
      break;
    case 'r':
      string2Cuuid(&m_requestUuid, Coptarg);
      break;
    case 's':
      string2Cuuid(&m_subRequestUuid, Coptarg);
      break;
    case 'F':
      m_fileId.fileid = strutou64(Coptarg);
      break;
    case 'H':
      strncpy(m_fileId.server, Coptarg, CA_MAXHOSTNAMELEN);
      m_fileId.server[CA_MAXHOSTNAMELEN] = '\0';
      break;
    case 'D':
      m_diskCopyId = strutou64(Coptarg);
      break;
    case 'X':
      m_sourceDiskCopyId = strutou64(Coptarg);
      break;
    case 'S':
      m_svcClass = Coptarg;
      break;
    case 'R':
      m_resourceFile = Coptarg;
      break;
    case 't':
      m_requestCreationTime = strutou64(Coptarg);
      break;
    default:
      help(argv[0]);
      terminate(0, USERR);
    }
  }

  // Check that all mandatory command line options have been specified
  if (m_svcClass.empty() || m_resourceFile.empty() || !m_diskCopyId ||
      !m_sourceDiskCopyId || !m_fileId.fileid || !m_fileId.server[0]) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "mandatory command line arguments missing";
    throw e;
  }

  // Check to make sure the request and subrequest uuid's are set
  if (!Cuuid_compare(&m_requestUuid, &nullCuuid) ||
      !Cuuid_compare(&m_subRequestUuid, &nullCuuid)) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "invalid request and/or subrequest uuid";
    throw e;
  }
}


//-----------------------------------------------------------------------------
// Help
//-----------------------------------------------------------------------------
void castor::job::diskcopy::MainThread::help(std::string programName) {
  std::cout << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--config <config-file>  or -c  \tConfiguration file\n"
    "\t--help                  or -h  \tPrint this help and exit\n"
    "\t--request <uuid>        or -r  \tThe request uuid\n"
    "\t--subrequest <uuid>     or -s  \tThe subrequest uuid\n"
    "\t--fileid <id>           of -F  \tThe fileid of the castor file to be created\n"
    "\t--nshost <hostname>     or -H  \tThe name server host associated with the castor file\n"
    "\t--destdc <id>           or -D  \tThe diskcopy id of the destination file to be created\n"
    "\t--srcdc <id>            or -X  \tThe diskcopy id of the source file to be copied\n"
    "\t--svcclass <name>       or -S  \tThe service class of the file to be created\n"
    "\t--resfile <location>    or -R  \tThe location of the SharedLSFResource\n"
    "\t--rcreationtime <time>  or -t  \tThe creation time of the disk copy replication request\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
}


//-----------------------------------------------------------------------------
// Run
//-----------------------------------------------------------------------------
void castor::job::diskcopy::MainThread::run(void *param) {

  // Download the resource file
  std::string content("");
  try {
    // "Downloading resource file"
    castor::dlf::Param params[] =
      {castor::dlf::Param("ResourceFile", m_resourceFile),
       castor::dlf::Param("MaxAttempts", m_resHelper->retryAttempts()),
       castor::dlf::Param("RetryInterval", m_resHelper->retryInterval()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_DEBUG, 35, 4, params, &m_fileId);

    m_resHelper->setUrl(m_resourceFile);
    content = m_resHelper->download(false);
  } catch (castor::exception::Exception e) {
    if (e.code() == EINVAL) {

      // "Invalid Uniform Resource Indicator, cannot download resource file"
      castor::dlf::Param params[] =
	{castor::dlf::Param("URI", m_resourceFile.substr(0, 7))};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 21, 1, params, &m_fileId);
    } else if (e.code() == SERTYEXHAUST) {

      // "Exceeded maximum number of attempts trying to download resource file"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Error", m_resHelper->errorBuffer() != "" ?
			    m_resHelper->errorBuffer() : "no message"),
	 castor::dlf::Param("MaxAttempts", m_resHelper->retryAttempts()),
	 castor::dlf::Param("URL", m_resourceFile),
	 castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 22, 4, params, &m_fileId);
    } else {

      // "Exception caught trying to download resource file"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Message", e.getMessage().str()),
	 castor::dlf::Param("Filename", m_resourceFile.substr(7)),
	 castor::dlf::Param(m_subRequestUuid)};
      castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 23, 3, params, &m_fileId);
    }
    terminate(m_diskCopyId, EXIT_FAILURE);
  }

  // If the diskserver name from the resource file is the same as the
  // execution host that the mover is running on, then this is the destination
  // end of the transfer. Otherwise, its the source.
  std::string hostname, diskserver, filesystem;
  std::istringstream iss(content);
  std::getline(iss, diskserver, ':');
  std::getline(iss, filesystem, '\n');

  // "The content of the resource file is invalid"
  if (iss.fail() || diskserver.empty() || filesystem.empty()) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("RequiredFormat", "diskserver:filesystem"),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 32, 2, params, &m_fileId);

    // If the content of the resource file is invalid both the destination and
    // source ends will try and fail the disk2disk copy transfer. The first one
    // to be processed by the stager will succeed the second will throw an
    // Oracle exception. (nothing can be done about this!)
    terminate(m_diskCopyId, EXIT_FAILURE);
  }

  try {
    hostname = castor::System::getHostName();
  } catch (castor::exception::Exception e) {

    // "Exception caught trying to getHostName, unable to determine which
    // end of a disk2disk copy transfer is the destination"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", strerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 24, 3, params, &m_fileId);
    terminate(m_diskCopyId, EXIT_FAILURE);
  }

  if (hostname != diskserver) {
    // "Starting source end of mover"
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID") != NULL ? getenv("LSB_JOBID") : "Unknown"),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_DEBUG, 33, 2, params, &m_fileId);

    m_mover->source();

    // Calculate statistics
    timeval tv;
    gettimeofday(&tv, NULL);
    signed64 elapsedTime =
      (((tv.tv_sec * 1000000) + tv.tv_usec) - m_startTime);

    // "Source end of mover terminated"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("ElapsedTime", elapsedTime * 0.000001),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_DEBUG, 34, 2, params2, &m_fileId);

    dlf_shutdown(5);
    exit(0);
  }

  // Calculate the amount of time that the user has had to wait since the
  // creation of the original request.
  timeval tv;
  gettimeofday(&tv, NULL);
  signed64 totalWaitTime = ((tv.tv_sec - m_requestCreationTime) * 1000000) + tv.tv_usec;

  // "DiskCopyTransfer started"
  castor::dlf::Param params[] =
    {castor::dlf::Param("JobId", getenv("LSB_JOBID") != NULL ? getenv("LSB_JOBID") : "Unknown"),
     castor::dlf::Param("DiskCopy", m_diskCopyId),
     castor::dlf::Param("SourceDiskCopy", m_sourceDiskCopyId),
     castor::dlf::Param("Protocol", m_protocol),
     castor::dlf::Param("TotalWaitTime",
			totalWaitTime > 0 ? totalWaitTime * 0.000001 : 0),
     castor::dlf::Param(m_subRequestUuid)};
  castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_SYSTEM, 25, 6, params, &m_fileId);

  // Call the disk2DiskCopyStart function to find out the information about the
  // destination and source diskcopy's. Note: disk2DiskCopyFailed will be
  // called automatically by the stager in case of exception.
  castor::stager::DiskCopyInfo *diskCopy = 0;
  castor::stager::DiskCopyInfo *sourceDiskCopy = 0;
  try {
    m_jobSvc->disk2DiskCopyStart(m_diskCopyId,
				 m_sourceDiskCopyId,
				 m_svcClass,
				 diskserver,
				 filesystem,
				 diskCopy,
				 sourceDiskCopy,
				 m_fileId.fileid,
				 m_fileId.server);
  } catch (castor::exception::Exception e) {

    // "Exception caught trying to get information on destination and source
    // disk copies"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("SourceDiskCopy", m_sourceDiskCopyId),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 26, 4, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  } catch (...) {

    // "Failed to remotely execute disk2DiskCopyStart"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught"),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 27, 2, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  }

  // "Starting destination end of mover"
  try {
    castor::dlf::Param params[] =
      {castor::dlf::Param("SourcePath",
			  sourceDiskCopy->diskServer() + ":" +
			  sourceDiskCopy->mountPoint() +
			  sourceDiskCopy->diskCopyPath()),
       castor::dlf::Param("DestinationPath",
			  diskCopy->mountPoint() +
			  diskCopy->diskCopyPath()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_DEBUG, 28, 3, params, &m_fileId);

    sourceDiskCopy->setFileId(m_fileId.fileid);
    sourceDiskCopy->setNsHost(m_fileId.server);

    m_mover->destination(diskCopy, sourceDiskCopy);
  } catch (castor::exception::Exception e) {

    // "Exception caught at the destination of the transfer"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Protocol", m_protocol),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 29, 4, params, &m_fileId);
    terminate(m_diskCopyId, EXIT_FAILURE);
  }

  // Transfer successful. Note: we don't need to call disk2DiskCopyFailed if
  // an exception is thrown here. The stager will do this automatically for us.
  try {
    m_jobSvc->disk2DiskCopyDone(m_diskCopyId,
				m_sourceDiskCopyId,
				m_fileId.fileid,
				m_fileId.server);
  } catch (castor::exception::Exception e) {

    // "Exception caught trying to finalize disk2disk copy transfer, transfer
    // failed"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 30, 3, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  } catch (...) {

    // "Failed to remotely execute disk2DiskCopyDone"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught"),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 31, 2, params, &m_fileId);
    terminate(0, EXIT_FAILURE);
  }

  // Calculate statistics
  gettimeofday(&tv, NULL);
  signed64 elapsedTime =
    (((tv.tv_sec * 1000000) + tv.tv_usec) - m_startTime);

  // "DiskCopyTransfer successful"
  castor::dlf::Param params2[] =
    {castor::dlf::Param("ElapsedTime", elapsedTime  * 0.000001),
     castor::dlf::Param("FileSize", m_mover->fileSize()),
     castor::dlf::Param("Direction",
			sourceDiskCopy->svcClass() + " -> " +
			diskCopy->svcClass()),
     castor::dlf::Param(m_subRequestUuid)};
  castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_SYSTEM, 39, 4, params2, &m_fileId);
  terminate(0, EXIT_SUCCESS);
}


//-----------------------------------------------------------------------------
// Stop
//-----------------------------------------------------------------------------
void castor::job::diskcopy::MainThread::stop() {
  if (m_mover != 0) {
    m_mover->stop(false);
  }
}


//-----------------------------------------------------------------------------
// changeUidGid
//-----------------------------------------------------------------------------
void castor::job::diskcopy::MainThread::changeUidGid
(std::string user, std::string group)
  throw(castor::exception::Exception){

  // Find a group and user entry in the passwd database
  struct passwd *usr = getpwnam(user.c_str());
  if (usr == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to getpwnam for user: " << user << std::endl;
    throw e;
  }

  struct group *grp = getgrnam(group.c_str());
  if (grp == NULL) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to getgrnam for group: " << group << std::endl;
    throw e;
  }

  // Verify that the group id associated with the group is the same as reported
  // by the user
  if (grp->gr_gid != usr->pw_gid) {
    castor::exception::Exception e(EPERM);
    e.getMessage() << "Group id does not match group id of User" << std::endl;
    throw e;
  }

  // Already running as the requested user ?
  if ((geteuid() == usr->pw_uid) && (getegid() == usr->pw_gid)) {
    return;
  }

  // Undo group and user privileges
  if ((setregid(getegid(), getgid()) < 0) ||
      (setreuid(geteuid(), getuid()) < 0)) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to undo group privileges" << std::endl;
    throw e;
  }

  // Set user privileges
  if ((setegid(usr->pw_gid) < 0) || (seteuid(usr->pw_uid) < 0)) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to undo user privileges" << std::endl;
    throw e;
  }

  // Now that we have swapped real and effective ids, we can set the effective
  // to the same values as the real
  if ((setegid(usr->pw_gid) < 0) || (seteuid(usr->pw_uid) < 0)) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Failed to set effective uid and/or gid" << std::endl;
    throw e;
  }
}


//-----------------------------------------------------------------------------
// terminate
//-----------------------------------------------------------------------------
void castor::job::diskcopy::MainThread::terminate
(u_signed64 diskCopyId, int status) {

  // Everything ok ?
  if (status == 0) {
    dlf_shutdown(5);
    exit(status);
  }

  // Notify the stager to the failure in this transfer attempt
  try {
    if (diskCopyId != 0) {
      m_jobSvc->disk2DiskCopyFailed(diskCopyId, m_fileId.fileid, m_fileId.server);
    }
  } catch (castor::exception::Exception e) {

    // "Exception caught trying to fail disk2DiskCopy transfer"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Type", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("DiskCopyId", diskCopyId),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 42, 4, params, &m_fileId);
  } catch (...) {

    // "Failed to remotely execute disk2DiskCopyFailed"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", "General exception caught"),
      castor::dlf::Param("DiskCopyId", diskCopyId),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_ERROR, 43, 3, params, &m_fileId);
  }

  // "DiskCopyTransfer failed"
  if (m_mover) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("ExitCode", status),
       castor::dlf::Param("FileSize", m_mover->fileSize()),
       castor::dlf::Param("BytesTransferred", m_mover->bytesTransferred()),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_SYSTEM, 41, 4, params, &m_fileId);
  } else {
    castor::dlf::Param params[] =
      {castor::dlf::Param("ExitCode", status),
       castor::dlf::Param(m_subRequestUuid)};
    castor::dlf::dlf_writep(m_requestUuid, DLF_LVL_SYSTEM, 41, 2, params, &m_fileId);
  }
  dlf_shutdown(5);
  exit(status);
}
