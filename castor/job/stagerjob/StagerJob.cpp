/******************************************************************************
 *                      stagerJob.cpp
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
 * this is the job launched by LSF on the diskserver in order to allow some I/O
 *
 * @author castor dev team
 *****************************************************************************/

// Include Files
#include <signal.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <Cgrp.h>
#include "common.h"
#include "getconfent.h"
#include "castor/BaseObject.hpp"
#include "castor/Services.hpp"
#include "castor/IClientFactory.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/job/stagerjob/IPlugin.hpp"
#include "castor/job/stagerjob/StagerJob.hpp"
#include "castor/job/SharedResourceHelper.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/RequestCanceled.hpp"

// default number of retries and interval between retries
// for the sharedResourceHelper
#define DEFAULT_RETRY_ATTEMPTS 60
#define DEFAULT_RETRY_INTERVAL 10

// static map s_plugins
static std::map<std::string, castor::job::stagerjob::IPlugin*> *s_plugins =
  new std::map<std::string, castor::job::stagerjob::IPlugin*>();

// -----------------------------------------------------------------------
// getPlugin
// -----------------------------------------------------------------------
castor::job::stagerjob::IPlugin*
castor::job::stagerjob::getPlugin(std::string protocol)
  throw (castor::exception::Exception) {
  if (s_plugins->find(protocol) != s_plugins->end()) {
    return s_plugins->operator[](protocol);
  }
  castor::exception::NoEntry e;
  e.getMessage() << "No mover plugin found for protocol "
                 << protocol;
  throw e;
}

// -----------------------------------------------------------------------
// registerPlugin
// -----------------------------------------------------------------------
void castor::job::stagerjob::registerPlugin
(std::string protocol, castor::job::stagerjob::IPlugin* plugin)
  throw () {
  s_plugins->operator[](protocol) = plugin;
}

// -----------------------------------------------------------------------
// parseCommandLine
// -----------------------------------------------------------------------
void parseCommandLine
(int argc, char** argv, castor::job::stagerjob::InputArguments &args) {
  // We expect nine arguments :
  // fileid@nshost requestuuid subrequestuuid rfeatures
  // subrequest_id@type host:fs mode clientString securityOption
  if (argc != 10) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Wrong number of arguments given";
    throw e;
  }

  // fileid and nshost
  std::string input = argv[1];
  unsigned int atPos = input.find('@');
  if (atPos == input.npos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "First argument should be <fileid>@<nshost>. "
                   << "No '@' found.";
    throw e;
  }
  if (input.find_first_not_of("0123456789") != atPos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "First argument should be <fileid>@<nshost> "
                   << "and <fileid> should be a numerical value";
    throw e;
  }
  strcpy(args.fileId.server, input.substr(atPos+1).c_str());
  args.fileId.fileid = strtou64(input.substr(0, atPos).c_str());

  // request uuid
  args.rawRequestUuid = argv[2];
  if (string2Cuuid(&args.requestUuid,argv[2]) != 0) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid request uuid : " << argv[2];
    throw e;
  }

  // subRequest uuid
  args.rawSubRequestUuid = argv[3];
  if (string2Cuuid(&args.subRequestUuid,argv[3]) != 0) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid request uuid : " << argv[3];
    throw e;
  }

  // rfeatures
  args.protocol = argv[4];
  if (args.protocol!= "rfio" &&
      args.protocol!= "rfio3" &&
      args.protocol!= "root" &&
      args.protocol!= "xroot" &&
      args.protocol!= "gsiftp") {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unsupported protocol " << args.protocol;
    throw e;
  }

  // subrequest_id and type
  input = argv[5];
  atPos = input.find('@');
  if (atPos == input.npos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Fifth argument should be <subreqId>@<type>. "
                   << "No '@' found.";
    throw e;
  }
  if (input.find_first_not_of("0123456789") != atPos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Fifth argument should be <subreqId>@<type> "
                   << "and <subreqId> should be a numerical value";
    throw e;
  }
  if (input.find_last_not_of("0123456789") != atPos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Fifth argument should be <subreqId>@<type> "
                   << "and <type> should be a numerical value";
    throw e;
  }
  args.subRequestId = strtou64(input.substr(0, atPos).c_str());
  args.type = atoi(input.substr(atPos+1).c_str());

  // Determine the number of times that the shared resource helper should try
  // to download the resource file from the shared resource URL
  char *value = getconfent("DiskCopy", "RetryAttempts", 0);
  int attempts = DEFAULT_RETRY_ATTEMPTS;
  if (value) {
    attempts = std::strtol(value, 0, 10);
    if (attempts < 1) {
      // "Invalid DiskCopy/RetryInterval option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", attempts),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep
        (args.requestUuid, DLF_LVL_WARNING,
         castor::job::stagerjob::INVRETRYINT, 2, params, &args.fileId);
    }
  }

  // Extract the value of the retry interval. This value determines how long
  // we sleep between retry attempts
  value = getconfent("DiskCopy", "RetryInterval", 0);
  int interval = DEFAULT_RETRY_INTERVAL;
  if (value) {
    interval = std::strtol(value, 0, 10);
    if (interval < 1) {
      // "Invalid DiskCopy/RetryAttempts option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", interval),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep
        (args.requestUuid, DLF_LVL_WARNING,
         castor::job::stagerjob::INVRETRYNBAT, 2, params, &args.fileId);
    }
  }

  // Get the diskserver and filesystem from the resource file
  std::string path = argv[6];
  if (path[path.size()-1] != '/') path += '/';
  path += getenv("LSB_JOBID");
  castor::job::SharedResourceHelper resHelper(attempts, interval);
  resHelper.setUrl(path);
  std::string content = resHelper.download(false);
  std::istringstream iss(content);
  std::getline(iss, args.diskServer, ':');
  std::getline(iss, args.fileSystem, '\n');
  if (iss.fail() || args.diskServer.empty() || args.fileSystem.empty()) {
    castor::exception::Internal e;
    e.getMessage() << "Sixth argument should be a resource file containing "
                   << "diskserver:filesystem";
    throw e;
  }

  // Retrieve mode
  char* mode = argv[7];
  if (mode[0] == 'r') {
    args.accessMode = castor::job::stagerjob::ReadOnly;
  } else if (mode[0] == 'w') {
    args.accessMode = castor::job::stagerjob::WriteOnly;
  } else if (mode[0] == 'o') {
    args.accessMode = castor::job::stagerjob::ReadWrite;
  } else {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Seventh argument should be a mode. "
                   << "Legal values are 'r', 'w' and 'o'";
    throw e;
  }

  // clientString
  args.client = castor::IClientFactory::string2Client(argv[8]);

  // security Option
  std::string secureFlag = argv[9];
  args.isSecure = false;
  if (secureFlag == "1") {
    args.isSecure = true;
  }
}

// -----------------------------------------------------------------------
// setCPULimit
// -----------------------------------------------------------------------
void setCPULimit() {
  // get limit from config file
  char* value = getconfent("Job", "LimitCPU", 0);
  if (value == NULL) return;
  // check value
  unsigned int limit = atol(value);
  if (limit <= 0) {
    // "Invalid LimitCPU value found in configuration file, will be ignored"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Value found", value)};
    castor::dlf::dlf_writep
      (nullCuuid, DLF_LVL_ERROR,
       castor::job::stagerjob::INVLIMCPU, 1, params);
    return;
  }
  // get the old limit
  struct rlimit rlim;
  if (getrlimit(RLIMIT_CPU, &rlim) != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Error caught in call to getrlimit";
    throw e;
  }
  // check range
  rlim.rlim_cur = limit;
  if (rlim.rlim_max < limit) {
    rlim.rlim_cur = rlim.rlim_max;
    // "rlimit found in config file exceeds hard limit"
    castor::dlf::Param params[] = {
      castor::dlf::Param("Found in config file", limit),
      castor::dlf::Param("Hard limit", rlim.rlim_max)};
    castor::dlf::dlf_writep
      (nullCuuid, DLF_LVL_WARNING,
       castor::job::stagerjob::INVRLIMIT, 2, params);
  }
  // Set new limit
  if (setrlimit(RLIMIT_CPU, &rlim) != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Error caught in call to setrlimit";
    throw e;
  }
}

// -----------------------------------------------------------------------
// getJobSvc
// -----------------------------------------------------------------------
castor::stager::IJobSvc* getJobSvc()
  throw (castor::exception::Exception) {
  // Initialize the remote job service
  castor::IService *remsvc =
    castor::BaseObject::services()->service
    ("RemoteJobSvc", castor::SVC_REMOTEJOBSVC);
  if (remsvc == 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to get RemoteJobSvc";
    throw e;
  }
  castor::stager::IJobSvc *jobSvc =
    dynamic_cast<castor::stager::IJobSvc *>(remsvc);
  if (jobSvc == 0) {
    castor::exception::Internal e;
    e.getMessage() << "Could not convert newly retrieved service into IJobSvc";
    throw e;
  }
  return jobSvc;
}

// -----------------------------------------------------------------------
// startAndGetPath
// -----------------------------------------------------------------------
std::string startAndGetPath
(castor::job::stagerjob::InputArguments& args,
 castor::job::stagerjob::PluginContext& context)
  throw (castor::exception::Exception) {

  // create diskserver and filesystem in memory
  castor::stager::DiskServer diskServer;
  diskServer.setName(args.diskServer);
  castor::stager::FileSystem fileSystem;
  fileSystem.setMountPoint(args.fileSystem);
  fileSystem.setDiskserver(&diskServer);
  diskServer.addFileSystems(&fileSystem);

  // create a subreq in memory and we will just fill its id
  castor::stager::SubRequest subrequest;
  subrequest.setId(args.subRequestId);

  // Get & Update case
  if ((args.accessMode == castor::job::stagerjob::ReadOnly) ||
      (args.accessMode == castor::job::stagerjob::ReadWrite)) {
    bool emptyFile;
    castor::stager::DiskCopy* diskCopy =
      context.jobSvc->getUpdateStart
      (&subrequest, &fileSystem, &emptyFile,
       args.fileId.fileid, args.fileId.server);
    if (diskCopy == NULL) {
      // No DiskCopy return, nothing should be done
      // The job was scheduled for nothing
      // This happens in particular when a diskCopy gets invalidated
      // while the job waits in the scheduler queue
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep
        (args.requestUuid, DLF_LVL_SYSTEM,
         castor::job::stagerjob::JOBNOOP, 2, params, &args.fileId);
      return "";
    }
    std::string fullDestPath = args.fileSystem + diskCopy->path();
    // Deal with recalls of empty files
    if (emptyFile) {
      int thisfd = creat(fullDestPath.c_str(), (S_IRUSR|S_IWUSR));
      if (thisfd < 0) {
        // "Failed to create empty file"
        castor::dlf::Param params[] =
          {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
           castor::dlf::Param("Path", fullDestPath),
           castor::dlf::Param("Error", strerror(errno)),
           castor::dlf::Param(args.subRequestUuid)};
        castor::dlf::dlf_writep
          (args.requestUuid, DLF_LVL_ERROR,
           castor::job::stagerjob::CREATFAILED, 4, params, &args.fileId);
        delete diskCopy;
        castor::exception::Exception e(errno);
        e.getMessage() << "Failed to create empty file";
        throw e;
      }
      if (close(thisfd) != 0) {
        castor::dlf::Param params[] =
          {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
           castor::dlf::Param("Path", fullDestPath),
           castor::dlf::Param("Error", strerror(errno)),
           castor::dlf::Param(args.subRequestUuid)};
        castor::dlf::dlf_writep
          (args.requestUuid, DLF_LVL_ERROR,
           castor::job::stagerjob::FCLOSEFAILED, 4, params, &args.fileId);
      }
    }
    delete diskCopy;
    return fullDestPath;

    // Put case
  } else {
    // Call putStart
    castor::stager::DiskCopy* diskCopy =
      context.jobSvc->putStart
      (&subrequest, &fileSystem,
       args.fileId.fileid, args.fileId.server);
    std::string fullDestPath = args.fileSystem + diskCopy->path();
    delete diskCopy;
    return fullDestPath;
  }
  // never reached
  castor::exception::Internal e;
  e.getMessage() << "reached unreachable code !";
  throw e;
}

// -----------------------------------------------------------------------
// switchToCastorSuperuser
// -----------------------------------------------------------------------
void switchToCastorSuperuser(castor::job::stagerjob::InputArguments &args)
  throw (castor::exception::Exception) {

  struct passwd *stage_passwd;    // password structure pointer
  struct group  *stage_group;     // group structure pointer
  uid_t ruid, euid;               // Original uid/euid
  gid_t rgid, egid;               // Original gid/egid

  // Save original values
  ruid = getuid ();
  euid = geteuid ();
  rgid = getgid ();
  egid = getegid ();

  // "Credentials at start time"
  castor::dlf::Param params[] =
    {castor::dlf::Param("uid", getuid()),
     castor::dlf::Param("gid", getgid()),
     castor::dlf::Param("euid", geteuid()),
     castor::dlf::Param("egid", getegid()),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep
    (args.requestUuid, DLF_LVL_DEBUG,
     castor::job::stagerjob::JOBORIGCRED, 5, params, &args.fileId);

  // Get information on generic stage account from password file
  if ((stage_passwd = Cgetpwnam(STAGERSUPERUSER)) == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Castor super user " << STAGERSUPERUSER
                   << " not found in password file";
    throw e;
  }
  // verify existence of its primary group id
  if (Cgetgrgid(stage_passwd->pw_gid) == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Castor super user group does not exist";
    throw e;
  }
  // Get information on generic stage account from group file
  if ((stage_group = Cgetgrnam(STAGERSUPERGROUP)) == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Castor super user group " << STAGERSUPERGROUP
                   << " not found in group file";
    throw e;
  }
  // Verify consistency
  if (stage_group->gr_gid != stage_passwd->pw_gid) {
    castor::exception::Internal e;
    e.getMessage() << "Inconsistent password file. The group of the "
                   << "castor superuser " << STAGERSUPERUSER
                   << " should be " << stage_group->gr_gid
                   << "(" << STAGERSUPERGROUP << "), but is "
                   << stage_passwd->pw_gid;
    throw e;
  }
  // set the effective privileges to superuser
  if (setegid(stage_passwd->pw_gid) < 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to set group privileges of Castor Superuser. "
                   << "You may want to check that the suid bit is set properly";
    throw e;
  }
  if (seteuid(stage_passwd->pw_uid) < 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to set privileges of Castor Superuser.";
    throw e;
  }

  // "Actual credentials used"
  castor::dlf::Param params2[] =
    {castor::dlf::Param("uid", getuid()),
     castor::dlf::Param("gid", getgid()),
     castor::dlf::Param("euid", geteuid()),
     castor::dlf::Param("egid", getegid()),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep
    (args.requestUuid, DLF_LVL_DEBUG,
     castor::job::stagerjob::JOBACTCRED, 5, params2, &args.fileId);
}


// -----------------------------------------------------------------------
// bindSocketAndListen
// -----------------------------------------------------------------------
void bindSocketAndListen
(castor::job::stagerjob::PluginContext &context,
 std::pair<int,int> &range)
  throw (castor::exception::Exception) {
  // build address
  struct sockaddr_in sin;
  memset(&sin,'\0',sizeof(sin));
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  sin.sin_family = AF_INET;
  // try to bind the socket to a random port in the range
  int counter = 0;
  int rc;
  while (0 != rc && counter < 1000){
    int port = (rand() % (range.second - range.first+1)) + range.first;
    sin.sin_port = htons(port);
    rc = ::bind(context.socket, (struct sockaddr *)&sin, sizeof(sin));
    counter++;
  }
  if (0 != rc) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to bind socket after 1000 attempts with range ["
                   << range.first << "," << range.second << "]";
    throw e;
  }
  // listen for the client connection
  if (listen(context.socket,1) < 0 ) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Error caught in call to listen";
    throw e;
  }
  fd_set readmask;
  FD_ZERO (&readmask);
  FD_SET(context.socket, &readmask);
  socklen_t len = sizeof(sin);
  if (getsockname(context.socket,(struct sockaddr *)&sin,&len) < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Error caught in call to getsockname";
    throw e;
  }
  context.port = ntohs(sin.sin_port);
}


// -----------------------------------------------------------------------
// process
// -----------------------------------------------------------------------
void process(castor::job::stagerjob::InputArguments& args)
  throw (castor::exception::Exception) {
  try {
    // First switch to stage/st privileges
    switchToCastorSuperuser(args);
    // Get an instance of the job service
    castor::stager::IJobSvc* jobSvc = getJobSvc();
    // get full path of the file we handle
    castor::job::stagerjob::PluginContext context;
    context.mask = S_IRWXG|S_IRWXO;
    context.jobSvc = jobSvc;
    context.fullDestPath = startAndGetPath(args, context);
    if ("" == context.fullDestPath) {
      // No DiskCopy return, nothing should be done
      // The job was scheduled for nothing
      // This happens in particular when a diskCopy gets invalidated
      // while the job waits in the scheduler queue
      // we've already logged, so just quit
      return;
    }
    // get proper plugin
    castor::job::stagerjob::IPlugin* plugin =
      castor::job::stagerjob::getPlugin(args.protocol);
    // create a socket
    context.socket = socket(AF_INET, SOCK_STREAM, 0);
    if (context.socket < 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Error caught in call to socket";
      throw e;
    }
    int rcode = 1;
    int rc = setsockopt(context.socket, SOL_SOCKET, SO_REUSEADDR, (char *)&rcode, sizeof(rcode));
    if (rc < 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Error caught in call to setsockopt";
      throw e;
    }
    // get available port range for the socket
    std::pair<int,int> portRange = plugin->getPortRange(args);
    // bind socket and listen for client connection
    bindSocketAndListen(context, portRange);
    // "Mover will use the following port"
    std::ostringstream sPortRange;
    sPortRange << portRange.first << ":" << portRange.second;
    castor::dlf::Param params[] =
      {castor::dlf::Param("Protocol", args.protocol),
       castor::dlf::Param("Available port range", sPortRange.str()),
       castor::dlf::Param("Port used", context.port),
       castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep
      (args.requestUuid, DLF_LVL_DEBUG,
       castor::job::stagerjob::MOVERPORT, 5, params, &args.fileId);
    // prefork hook for the different movers
    plugin->preForkHook(args, context);
    // Set our mask to the most restrictive mode
    umask(context.mask);
    // chdir into something else but the root system...
    if (chdir("/tmp") != 0) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep
        (args.requestUuid, DLF_LVL_ERROR,
         castor::job::stagerjob::CHDIRFAILED, 2, params, &args.fileId);
      // not fatal, we just ignore the error
    }
    // Fork and execute the mover
    dlf_prepare();
    context.childPid = fork();
    if (context.childPid < 0) {
      dlf_parent();
      castor::exception::Exception e(errno);
      e.getMessage() << "Error caught in call to fork";
      throw e;
    }
    if (context.childPid == 0) {
      // Child side of the fork
      dlf_child();
      // this call will never come back, since it call execl
      plugin->execMover(args, context);
      // but in case, let's fail
      dlf_shutdown(5);
      exit(EXIT_FAILURE);
    }
    // Parent side of the fork
    dlf_parent();
    plugin->postForkHook(args, context);
  } catch (castor::exception::RequestCanceled ex) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep
      (args.requestUuid, DLF_LVL_SYSTEM,
       castor::job::stagerjob::REQCANCELED, 2, params, &args.fileId);
  }
}


// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------
int main(int argc, char** argv) {
  // Ignore SIGPIPE to avoid being brutally interrupted because of network [write] error
  signal(SIGPIPE,SIG_IGN);
  castor::job::stagerjob::InputArguments arguments;
  try {

    // Initializing logging
    castor::BaseObject::initLog("job", castor::SVC_DLFMSG);
    using namespace castor::job::stagerjob;
    castor::dlf::Message messages[] = {
      // system call errors
      { CREATFAILED,   "Failed to create empty file"},
      { FCLOSEFAILED,  "Failed to close file"},
      { SCLOSEFAILED,  "Failed to close socket"},
      { CHDIRFAILED,   "Failed to change directory to tmp"},
      { DUP2FAILED,    "Failed to duplicate socket"},
      { MOVERNOTEXEC,  "Mover program can not be executed. Check permissions"},
      // Invalid configurations or parameters
      { INVRLIMIT,     "rlimit found in config file exceeds hard limit"},
      { INVLIMCPU,     "Invalid LimitCPU value found in configuration file, will be ignored"},
      { CPULIMTOOHIGH, "CPU limit value found in config file exceeds hard limit, using hard limit"},
      { INVRETRYINT,   "Invalid DiskCopy/RetryInterval option, using default" },
      { INVRETRYNBAT,  "Invalid DiskCopy/RetryAttempts option, using default" },
      // Informative logs
      { JOBSTARTED,    "Job Started" },
      { JOBENDED,      "Job exiting successfully" },
      { JOBFAILED,     "Job failed" },
      { JOBORIGCRED,   "Credentials at start time" },
      { JOBACTCRED,    "Actual credentials used" },
      { JOBNOOP,       "No operation performed" },
      { FORKMOVER,     "Forking mover" },
      { REQCANCELED,   "Request canceled" },
      { MOVERPORT,     "Mover will use the following port" },
      { MOVERFORK,     "Mover fork uses the following command line" },
      { ACCEPTCONN,    "Client connected" },
      // Errors
      { STAT64FAIL,    "rfio_stat64 error" },
      { CHILDEXITED,   "Child exited" },
      { CHILDSIGNALED, "Child exited due to uncaught signal" },
      { CHILDSTOPPED,  "Child was stopped" },
      // Protocol specific. Should not be here if the plugins
      // were properly packaged in separate libs
      { GSIBADPORT,    "Invalid port range for GridFTP in config file. using default" },
      { GSIBADMINPORT, "Invalid lower bound for GridFTP port range in config file. Using default" },
      { GSIBADMAXPORT, "Invalid upper bound for GridFTP port range in config file. Using default" },
      { GSIBADMINVAL,  "Lower bound for GridFTP port range not in valid range. Using default" },
      { GSIBADMAXVAL,  "Upper bound for GridFTP port range not in valid range. Using default" },
      { GSIPORTRANGE,  "GridFTP Port range" },
      { GSIPORTUSED,   "GridFTP Socket bound" },
      { -1, ""}};
    castor::dlf::dlf_init("job", messages);

    // Set CPU limits
    setCPULimit();

    // Parse the command line
    parseCommandLine(argc, argv, arguments);

    // "Job Started"
    std::string stagerConcatenatedArgv;
    for (int i = 0; i < argc; i++) {
      stagerConcatenatedArgv += argv[i];
      stagerConcatenatedArgv += " ";
    }
    castor::dlf::Param params[] =
      {castor::dlf::Param("Arguments", stagerConcatenatedArgv),
       castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param(arguments.subRequestUuid)};
    castor::dlf::dlf_writep
      (arguments.requestUuid, DLF_LVL_SYSTEM,
       castor::job::stagerjob::JOBSTARTED, 3, params, &arguments.fileId);

    // Call stagerJobProcess
    process(arguments);

    // "Job exiting successfully"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param(arguments.subRequestUuid)};
    castor::dlf::dlf_writep(arguments.requestUuid, DLF_LVL_SYSTEM,
                            castor::job::stagerjob::JOBENDED, 2, params2, &arguments.fileId);

  } catch (castor::exception::Exception e) {
    // "Job failed"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", strerror(e.code())),
       castor::dlf::Param("Detailed Error", e.getMessage().str()),
       castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param(arguments.subRequestUuid)};
    castor::dlf::dlf_writep
      (arguments.requestUuid, DLF_LVL_SYSTEM,
       castor::job::stagerjob::JOBENDED, 4, params, &arguments.fileId);
    return -1;
  }
}
