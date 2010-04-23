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
#include <sys/types.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <sstream>
#include <signal.h>
#include <fcntl.h>
#include "common.h"
#include "getconfent.h"
#include "castor/System.hpp"
#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/rh/EndResponse.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/job/stagerjob/IPlugin.hpp"
#include "castor/job/stagerjob/StagerJob.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/Internal.hpp"

// Static map s_plugins
static std::map<std::string, castor::job::stagerjob::IPlugin*> *s_plugins = 0;

// Flag to indicate whether the client has already been sent a callback
static bool clientAnswered = false;

//------------------------------------------------------------------------------
// getPlugin
//------------------------------------------------------------------------------
castor::job::stagerjob::IPlugin*
castor::job::stagerjob::getPlugin(std::string protocol)
  throw (castor::exception::Exception) {
  if (0 != s_plugins && s_plugins->find(protocol) != s_plugins->end()) {
    return s_plugins->operator[](protocol);
  }
  castor::exception::NoEntry e;
  e.getMessage() << "No mover plugin found for protocol "
                 << protocol;
  throw e;
}

//------------------------------------------------------------------------------
// registerPlugin
//------------------------------------------------------------------------------
void castor::job::stagerjob::registerPlugin
(std::string protocol, castor::job::stagerjob::IPlugin* plugin)
  throw () {
  if (0 == s_plugins) {
    s_plugins = new std::map<std::string, castor::job::stagerjob::IPlugin*>();
  }
  s_plugins->operator[](protocol) = plugin;
}

//------------------------------------------------------------------------------
// getJobSvc
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
// startAndGetPath
//------------------------------------------------------------------------------
std::string startAndGetPath
(castor::job::stagerjob::InputArguments* args,
 castor::job::stagerjob::PluginContext& context)
  throw (castor::exception::Exception) {

  // Create diskserver and filesystem in memory
  castor::stager::DiskServer diskServer;
  diskServer.setName(args->diskServer);
  castor::stager::FileSystem fileSystem;
  fileSystem.setMountPoint(args->fileSystem);
  fileSystem.setDiskserver(&diskServer);
  diskServer.addFileSystems(&fileSystem);

  // Create a subreq in memory and we will just fill its id
  castor::stager::SubRequest subrequest;
  subrequest.setId(args->subRequestId);

  // Get & Update case
  if ((args->accessMode == castor::job::stagerjob::ReadOnly) ||
      (args->accessMode == castor::job::stagerjob::ReadWrite)) {
    bool emptyFile;
    castor::stager::DiskCopy* diskCopy =
      context.jobSvc->getUpdateStart
      (&subrequest, &fileSystem, &emptyFile,
       args->fileId.fileid, args->fileId.server);
    if (diskCopy == NULL) {
      // No DiskCopy return, nothing should be done
      // The job was scheduled for nothing
      // This happens in particular when a diskCopy gets invalidated
      // while the job waits in the scheduler queue
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param(args->subRequestUuid)};
      castor::dlf::dlf_writep
        (args->requestUuid, DLF_LVL_SYSTEM,
         castor::job::stagerjob::JOBNOOP, 2, params, &args->fileId);
      return "";
    }
    std::string fullDestPath = args->fileSystem + diskCopy->path();
    // Deal with recalls of empty files
    // the file may have been declared recalled without being created on disk
    // so let's check and create when needed
    if (emptyFile) {
      struct stat s;
      if (-1 == stat(fullDestPath.c_str(), &s) && errno == ENOENT) {
        int thisfd = creat(fullDestPath.c_str(), (S_IRUSR|S_IWUSR));
        if (thisfd < 0) {
          // "Failed to create empty file"
          castor::dlf::Param params[] =
            {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
             castor::dlf::Param("Path", fullDestPath),
             castor::dlf::Param("Error", strerror(errno)),
             castor::dlf::Param(args->subRequestUuid)};
          castor::dlf::dlf_writep
            (args->requestUuid, DLF_LVL_ERROR,
             castor::job::stagerjob::CREATFAILED, 4, params, &args->fileId);
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
             castor::dlf::Param(args->subRequestUuid)};
          castor::dlf::dlf_writep
            (args->requestUuid, DLF_LVL_ERROR,
             castor::job::stagerjob::FCLOSEFAILED, 4, params, &args->fileId);
        }
      }
    }

    // For Updates we should clear the files extended attributes. There is
    // no need to do something for errors as maybe we have no attributes
    // and we do not have to check RFIOD USE_CHECKSUM
    if (args->accessMode == castor::job::stagerjob::ReadWrite && !emptyFile) {
      removexattr(fullDestPath.c_str(), "user.castor.checksum.value");
      removexattr(fullDestPath.c_str(), "user.castor.checksum.type");
    }

    delete diskCopy;
    return fullDestPath;

    // Put case
  } else {
    // Call putStart
    castor::stager::DiskCopy* diskCopy =
      context.jobSvc->putStart
      (&subrequest, &fileSystem,
       args->fileId.fileid, args->fileId.server);
    std::string fullDestPath = args->fileSystem + diskCopy->path();
    delete diskCopy;
    return fullDestPath;
  }
  // Never reached
  castor::exception::Internal e;
  e.getMessage() << "reached unreachable code !";
  throw e;
}

//------------------------------------------------------------------------------
// switchToCastorSuperuser
//------------------------------------------------------------------------------
void switchToCastorSuperuser(castor::job::stagerjob::InputArguments *args)
  throw (castor::exception::Exception) {

  // "Credentials at start time"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Uid", getuid()),
     castor::dlf::Param("Gid", getgid()),
     castor::dlf::Param("Euid", geteuid()),
     castor::dlf::Param("Egid", getegid()),
     castor::dlf::Param(args->subRequestUuid)};
  castor::dlf::dlf_writep
    (args->requestUuid, DLF_LVL_DEBUG,
     castor::job::stagerjob::JOBORIGCRED, 5, params, &args->fileId);

  // Perform the switch
  castor::System::switchToCastorSuperuser();

  // "Actual credentials used"
  castor::dlf::Param params2[] =
    {castor::dlf::Param("Uid", getuid()),
     castor::dlf::Param("Gid", getgid()),
     castor::dlf::Param("Euid", geteuid()),
     castor::dlf::Param("Egid", getegid()),
     castor::dlf::Param(args->subRequestUuid)};
  castor::dlf::dlf_writep
    (args->requestUuid, DLF_LVL_DEBUG,
     castor::job::stagerjob::JOBACTCRED, 5, params2, &args->fileId);
}

//------------------------------------------------------------------------------
// createSocket
//------------------------------------------------------------------------------
void createSocket(castor::job::stagerjob::PluginContext &context) 
  throw (castor::exception::Exception) {
  // Create the socket
  context.socket = socket(AF_INET, SOCK_STREAM, 0);
  if (context.socket < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Error caught in call to socket";
    throw e;
  }
  // Set socket options
  int rcode = 1;
  int rc = setsockopt(context.socket, SOL_SOCKET, SO_REUSEADDR,
                      (char *)&rcode, sizeof(rcode));
  if (rc < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Error caught in call to setsockopt";
    throw e;
  }
}

//------------------------------------------------------------------------------
// bindSocketAndListen
//------------------------------------------------------------------------------
void bindSocketAndListen
(castor::job::stagerjob::PluginContext &context,
 castor::job::stagerjob::InputArguments* args,
 std::pair<int,int> &range)
  throw (castor::exception::Exception) {
  // Build address
  struct sockaddr_in sin;
  memset(&sin, '\0', sizeof(sin));

  // For xroot based transfer bind to the loopback device. This prevents
  // connections from external clients!
  if (args->protocol == "xroot") {
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  } else {
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
  }
  sin.sin_family = AF_INET;

  // Set the seed for the new sequence of pseudo-random numbers to be returned
  // by subsequent calls to rand()
  timeval tv;
  gettimeofday(&tv, NULL);
  srand(tv.tv_usec * tv.tv_sec);

  // Loop over all the ports in the specified range starting at a random
  // offset
  int port = (rand() % (range.second - (range.first + 1))) + range.first;
  int attempts = 0;
  while (port++) {
    attempts++;
    // If we reach the maximum allowed port, reset it!
    if (port > range.second) {
      port = range.first;
      sleep(5);  // sleep between complete loops, prevents CPU thrashing
      continue;
    }

    // Attempt to bind to the port
    sin.sin_port = htons(port);
    if (bind(context.socket, (struct sockaddr*)&sin, sizeof(sin)) == 0) {
      // Just because the bind was successfull, doesn't mean the listen
      // will succeed!
      if (listen(context.socket, 1) < 0) {
        // If the error is "Address already in use" we try and bind again after
        // 5 seconds unless we have already tried 10 times.
        if ((errno == EADDRINUSE) && (attempts <= 10)) {
          // Close and recreate the socket.
          close(context.socket);
          createSocket(context);    
          sleep(5);
          continue;
        } else {
          castor::exception::Exception e(errno);
          e.getMessage() << "Error caught in call to listen";
          throw e;
        }
      }
      break;
    }
  }

  socklen_t len = sizeof(sin);
  if (getsockname(context.socket, (struct sockaddr *)&sin, &len) < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Error caught in call to getsockname";
    throw e;
  }
  context.port = ntohs(sin.sin_port);
}

//------------------------------------------------------------------------------
// process
//------------------------------------------------------------------------------
void process(castor::job::stagerjob::PluginContext &context,
             castor::job::stagerjob::InputArguments* args)
  throw (castor::exception::Exception) {
  castor::job::stagerjob::IPlugin* plugin = 0;
  try {
    // First switch to stage:st privileges
    switchToCastorSuperuser(args);
    // Get an instance of the job service
    castor::stager::IJobSvc* jobSvc = getJobSvc();
    // Get full path of the file we handle
    context.host = castor::System::getHostName();
    context.mask = S_IWGRP|S_IWOTH;
    context.jobSvc = jobSvc;
    context.childPid = 0;
    context.fullDestPath = startAndGetPath(args, context);
    if ("" == context.fullDestPath) {
      // No DiskCopy return, nothing should be done
      // The job was scheduled for nothing
      // This happens in particular when a diskCopy gets invalidated
      // while the job waits in the scheduler queue
      // we've already logged, so just quit
      return;
    }
    // Get proper plugin
    plugin = castor::job::stagerjob::getPlugin(args->protocol);
    // Create the socket that the user will connect too
    createSocket(context);
    // Get available port range for the socket
    std::pair<int,int> portRange = plugin->getPortRange(*args);
    // Bind socket and listen for client connection
    bindSocketAndListen(context, args, portRange);
    // "Mover will use the following port"
    std::ostringstream sPortRange;
    sPortRange << portRange.first << ":" << portRange.second;
    castor::dlf::Param params[] =
      {castor::dlf::Param("Protocol", args->protocol),
       castor::dlf::Param("Port range", sPortRange.str()),
       castor::dlf::Param("Port used", context.port),
       castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param(args->subRequestUuid)};
    castor::dlf::dlf_writep
      (args->requestUuid, DLF_LVL_DEBUG,
       castor::job::stagerjob::MOVERPORT, 5, params, &args->fileId);
    // Prefork hook for the different movers
    plugin->preForkHook(*args, context);
  } catch (castor::exception::Exception e) {
    // If we got an exception before the fork, we need to cleanup
    // before letting the exception go further
    try {
      // Distinguish get from puts
      if (args->accessMode == castor::job::stagerjob::ReadOnly ||
          args->accessMode == castor::job::stagerjob::ReadWrite) {
        context.jobSvc->getUpdateFailed
          (args->subRequestId, args->fileId.fileid, args->fileId.server);
      } else {
        context.jobSvc->putFailed
          (args->subRequestId, args->fileId.fileid, args->fileId.server);
      }
    } catch (castor::exception::Exception e2) {
      // cleanup failed, log it
      // Unable to clean up stager DB for failed job
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param(args->subRequestUuid),
         castor::dlf::Param("ErrorCode", e2.code()),
         castor::dlf::Param("ErrorMessage", e2.getMessage().str())};
      castor::dlf::dlf_writep
        (args->requestUuid, DLF_LVL_ERROR,
         castor::job::stagerjob::CLEANUPFAILED, 2, params, &args->fileId);
    }
    // rethrow original exception. It will be caught in the main procedure,
    // logged properly and the client will be answered
    throw e;
  }
  // Set our mask to the most restrictive mode
  umask(context.mask);
  // chdir into something else but the root system...
  if (chdir("/tmp") != 0) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param(args->subRequestUuid)};
    castor::dlf::dlf_writep
      (args->requestUuid, DLF_LVL_ERROR,
       castor::job::stagerjob::CHDIRFAILED, 2, params, &args->fileId);
    // Not fatal, we just ignore the error
  }
  // Fork and execute the mover. Note: For xroot based transfers there is no
  // mover to execute so we avoid the need to fork.
  if (args->protocol != "xroot") {
    context.childPid = fork();
    if (context.childPid < 0) {
      castor::exception::Exception e(errno);
      e.getMessage() << "Error caught in call to fork";
      throw e;
    }
    if (context.childPid == 0) {
      // Child side of the fork
      // This call will never come back, since it call execl
      plugin->execMover(*args, context);
      // But in case, let's fail
      dlf_shutdown();
      exit(EXIT_FAILURE);
    }
    // Parent side of the fork
  }
  plugin->postForkHook(*args, context);
}

//------------------------------------------------------------------------------
// sendResponse
//------------------------------------------------------------------------------
void castor::job::stagerjob::sendResponse
(castor::IClient *client,
 castor::rh::IOResponse &response)
  throw (castor::exception::Exception) {
  castor::rh::Client* rhc = dynamic_cast<castor::rh::Client*>(client);
  if (0 == rhc) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to reply to client, unknown client type : "
                   << client->type();
    throw e;
  }
  if (clientAnswered == false) {
    clientAnswered = true;
    castor::io::ClientSocket s(rhc->port(), rhc->ipAddress());
    s.connect();
    s.sendObject(response);
    castor::rh::EndResponse endRes;
    s.sendObject(endRes);
  } else {
    // The client has already been sent a response, trying to send a second
    // one will just result in a failure to connect.
  }
}

//------------------------------------------------------------------------------
// terminateMover
//------------------------------------------------------------------------------
void terminateMover(castor::job::stagerjob::PluginContext &context,
                    castor::job::stagerjob::InputArguments* args,
                    int signal) {

  // If the callee supplies a custom signal use it and don't perform any
  // further processing
  if (signal) {
    kill(context.childPid, signal);
    return;
  }

  // Send a SIGTERM signal to the mover, hopefully this will trigger a
  // graceful shutdown
  int rv = kill(context.childPid, SIGTERM);
  if (rv < 0) {
    return;  // Signal failed, probably the process doesn't exist
  }
  
  // Wait for the process to terminate for up to 10 seconds
  std::string procFile = "/proc/";
  procFile += context.childPid;
  for (unsigned int i = 0; i < 10; i++) {
    struct stat s;
    if ((stat(procFile.c_str(), &s) < 0) && (errno == ENOENT)) {
      return;  // No process running
    }
    sleep(1);
  }
  
  // "Mover process still running after 10 seconds, killing process"
  castor::dlf::Param params[] =
    {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
     castor::dlf::Param("PID", context.childPid),
     castor::dlf::Param("Signal", "SIGKILL"),
     castor::dlf::Param(args->subRequestUuid)};
  castor::dlf::dlf_writep
    (args->requestUuid, DLF_LVL_DEBUG, castor::job::stagerjob::KILLMOVER,
     4, params, &args->fileId);
  
  // After 10 seconds the mover is still running so kill it!
  kill(context.childPid, SIGKILL);
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char** argv) {

  // Record start time
  timeval tv;
  gettimeofday(&tv, NULL);
  u_signed64 startTime = (tv.tv_sec * 1000000) + tv.tv_usec;

  // Ignore SIGPIPE to avoid being brutally interrupted
  // because of network [write] error
  signal(SIGPIPE,SIG_IGN);
  castor::job::stagerjob::InputArguments* arguments = 0;
  castor::job::stagerjob::PluginContext context;

  try {
    // Initializing logging
    using namespace castor::job::stagerjob;
    castor::dlf::Message messages[] = {

      // System call errors
      { CREATFAILED,     "Failed to create empty file" },
      { FCLOSEFAILED,    "Failed to close file" },
      { SCLOSEFAILED,    "Failed to close socket" },
      { CHDIRFAILED,     "Failed to change directory to tmp" },
      { DUP2FAILED,      "Failed to duplicate socket" },
      { MOVERNOTEXEC,    "Mover program cannot be executed. Check permissions" },
      { EXECFAILED,      "Failed to exec mover" },

      // Invalid configurations or parameters
      { INVRETRYINT,     "Invalid Job/RetryInterval option, using default" },
      { INVRETRYNBAT,    "Invalid Job/RetryAttempts option, using default" },
      { DOWNRESFILE,     "Downloading resource file" },
      { INVALIDURI,      "Invalid Uniform Resource Indicator, cannot download resource file" },
      { MAXATTEMPTS,     "Exceeded maximum number of attempts trying to download resource file" },
      { DOWNEXCEPT,      "Exception caught trying to download resource file" },
      { INVALRESCONT,    "The content of the resource file is invalid" },

      // Informative logs
      { JOBSTARTED,      "Job Started" },
      { JOBENDED,        "Job finished successfully" },
      { JOBFAILED,       "Job failed" },
      { JOBORIGCRED,     "Credentials at start time" },
      { JOBACTCRED,      "Actual credentials used" },
      { JOBNOOP,         "No operation performed" },
      { FORKMOVER,       "Forking mover" },
      { REQCANCELED,     "Request canceled" },
      { MOVERPORT,       "Mover will use the following port" },
      { MOVERFORK,       "Mover fork uses the following command line" },
      { ACCEPTCONN,      "Client connected" },
      { JOBFAILEDNOANS,  "Job failed before it could send an answer to client" },
      { TERMINATEMOVER,  "Mover process still running, sending signal" },
      { KILLMOVER,       "Mover process still running after 10 seconds, killing process" },

      // Errors
      { STAT64FAIL,      "stat64 error" },
      { NODATAWRITTEN,   "No data transfered" },
      { UNLINKFAIL,      "unlink error" },
      { CHILDEXITED,     "Child exited" },
      { CHILDSIGNALED,   "Child exited due to uncaught signal" },
      { CHILDSTOPPED,    "Child was stopped" },
      { NOANSWERSENT,    "Could not send answer to client" },
      { GETATTRFAILED,   "Failed to get checksum information from extended attributes" },
      { CSTYPENOTSOP,    "Unsupported checksum type, ignoring checksum information" },
      { CLEANUPFAILED,   "Unable to clean up stager DB for failed job" },

      // Protocol specific. Should not be here if the plugins
      // were properly packaged in separate libs
      { GSIBADPORT,      "Invalid port range for GridFTP in config file, using default" },
      { GSIBADMINPORT,   "Invalid lower bound for GridFTP port range in config file, using default" },
      { GSIBADMAXPORT,   "Invalid upper bound for GridFTP port range in config file, using default" },
      { GSIBADMINVAL,    "Lower bound for GridFTP port range not in valid range, using default" },
      { GSIBADMAXVAL,    "Upper bound for GridFTP port range not in valid range, using default" },
      { GSIBADTIMEOUT,   "Invalid value for GSIFTP/TIMEOUT option, using default" },

      { XROOTENOENT,     "Xrootd is not installed" },
      { XROOTBADTIMEOUT, "Invalid value for XROOT/TIMEOUTS option, using default" },

      { RFIODBADPORT,    "Invalid port range for RFIOD in config file, using default" },
      { RFIODBADMINPORT, "Invalid lower bound for RFIOD port range in config file, using default" },
      { RFIODBADMAXPORT, "Invalid upper bound for RFIOD port range in config file, using default" },
      { RFIODBADMINVAL,  "Lower bound for RFIOD port range not in valid range, using default" },
      { RFIODBADMAXVAL,  "Upper bound for RFIOD port range not in valid range, using default" },

      { ROOTDBADPORT,    "Invalid port range for ROOT in config file, using default" },
      { ROOTDBADMINPORT, "Invalid lower bound for ROOT port range in config file, using default" },
      { ROOTDBADMAXPORT, "Invalid upper bound for ROOT port range in config file, using default" },
      { ROOTDBADMINVAL,  "Lower bound for ROOT port range not in valid range, using default" },
      { ROOTDBADMAXVAL,  "Upper bound for ROOT port range not in valid range, using default" },
      { ROOTDBADTIMEOUT, "Invalid value for ROOT/TIMEOUT option, using default" },

      { -1, "" }};
    castor::dlf::dlf_init("stagerjob", messages);

    // Parse the command line
    arguments = new castor::job::stagerjob::InputArguments(argc, argv);

  } catch (castor::exception::Exception e) {
    // Something went wrong but we are not yet in a situation
    // where we can inform the client. So log it and return straight
    // "Job failed before it could send an answer to client"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(e.code())),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("JobId", getenv("LSB_JOBID"))};
    castor::dlf::dlf_writep
      (nullCuuid, DLF_LVL_ERROR,
       castor::job::stagerjob::JOBFAILEDNOANS, 3, params);
    dlf_shutdown();
    return -1;
  }

  // Check that the intended host of the transfer is this host!
  if (arguments->diskServer != castor::System::getHostName()) {
    // not the case, LSF has failed...
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(ESTSCHEDERR)),
       castor::dlf::Param("Message", "Hostname mismatch, job scheduled to the wrong host"),
       castor::dlf::Param("JobId", getenv("LSB_JOBID"))};
    castor::dlf::dlf_writep
      (nullCuuid, DLF_LVL_ERROR,
       castor::job::stagerjob::JOBFAILEDNOANS, 3, params);
    dlf_shutdown();
    return -1;
  }

  try {

    // Construct command line
    std::string stagerConcatenatedArgv;
    for (int i = 0; i < argc; i++) {
      stagerConcatenatedArgv += argv[i];
      stagerConcatenatedArgv += " ";
    }

    // Compute waiting time of the request
    struct timeval tv;
    gettimeofday(&tv, NULL);
    double totalWaitTime = tv.tv_usec;
    totalWaitTime = totalWaitTime/1000000 +
      tv.tv_sec - arguments->requestCreationTime;

    // "Job started"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Arguments", stagerConcatenatedArgv),
       castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param("Type", castor::ObjectsIdStrings[arguments->type]),
       castor::dlf::Param("Protocol", arguments->protocol),
       castor::dlf::Param("TotalWaitTime", totalWaitTime),
       castor::dlf::Param(arguments->subRequestUuid)};
    castor::dlf::dlf_writep
      (arguments->requestUuid, DLF_LVL_SYSTEM,
       castor::job::stagerjob::JOBSTARTED, 6, params, &arguments->fileId);

    // Call stagerJobProcess
    process(context, arguments);

    // Calculate statistics
    gettimeofday(&tv, NULL);
    signed64 elapsedTime =
      (((tv.tv_sec * 1000000) + tv.tv_usec) - startTime);

    // "Job finished successfully"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param("ElapsedTime", elapsedTime  * 0.000001),
       castor::dlf::Param(arguments->subRequestUuid)};
    castor::dlf::dlf_writep
      (arguments->requestUuid, DLF_LVL_SYSTEM,
       castor::job::stagerjob::JOBENDED, 3, params2, &arguments->fileId);

    // Memory cleanup
    delete arguments;

  } catch (castor::exception::Exception e) {
    if (e.code() == SENOVALUE) {
      // "manually" catch the NoValue exception
      // Nothing to be done in such a case, an error was already logged
      // and the error service will answer automatically to the client
    } else if (e.code() == ESTREQCANCELED) {
      // "manually" catch the RequestCanceled exception these are converted
      // to regular Exception objects by the internal remote procedure call
      // mechanism
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param(arguments->subRequestUuid)};
      castor::dlf::dlf_writep
        (arguments->requestUuid, DLF_LVL_SYSTEM,
         castor::job::stagerjob::REQCANCELED, 2, params, &arguments->fileId);
    } else {
      // "Job failed"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", sstrerror(e.code())),
         castor::dlf::Param("Message", e.getMessage().str()),
         castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param(arguments->subRequestUuid)};
      // A priori, we log an error
      int loglevel = DLF_LVL_ERROR;
      // But in some cases, it's actually a use error
      if (e.code() == ECONNREFUSED || // in case we can not connect to the client
          e.code() == SETIMEDOUT   || // in case the client never answered
          e.code() == EHOSTUNREACH || // the client is not visible
          e.code() == SENOVALUE    || // no data was transfered
          e.code() == ENOENT) {       // file was removed while being modified
        loglevel = DLF_LVL_USER_ERROR;
      }
      castor::dlf::dlf_writep
        (arguments->requestUuid, loglevel,
         castor::job::stagerjob::JOBFAILED, 4, params, &arguments->fileId);
      // Try to answer the client
      try {
        castor::rh::IOResponse ioResponse;
        ioResponse.setErrorCode(e.code());
        ioResponse.setErrorMessage(e.getMessage().str());
        castor::job::stagerjob::sendResponse(arguments->client, ioResponse);
      } catch (castor::exception::Exception e2) {
        // "Could not send answer to client"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Error", sstrerror(e2.code())),
           castor::dlf::Param("Message", e2.getMessage().str()),
           castor::dlf::Param("JobId", getenv("LSB_JOBID")),
           castor::dlf::Param(arguments->subRequestUuid)};
        castor::dlf::dlf_writep
          (arguments->requestUuid, DLF_LVL_USER_ERROR,
           castor::job::stagerjob::NOANSWERSENT, 4, params, &arguments->fileId);
      }
      // If the child process is still running kill it gracefully. This makes
      // sure that the LSF job slot is liberated quickly and does not wait
      // (#61085). Note: we ignore errors!
      if (context.childPid != 0) {
        // "Mover process still running, sending signal"
        castor::dlf::Param params[] =
          {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
           castor::dlf::Param("PID", context.childPid),
           castor::dlf::Param("Signal", "SIGTERM"),
           castor::dlf::Param(arguments->subRequestUuid)};
        castor::dlf::dlf_writep
          (arguments->requestUuid, DLF_LVL_DEBUG,
           castor::job::stagerjob::TERMINATEMOVER, 4, params,
           &arguments->fileId);
        terminateMover(context, arguments, SIGKILL);
      }
    }
    // Memory cleanup
    if (0 != arguments) delete arguments;
    dlf_shutdown();
    return -1;
  }
  dlf_shutdown();
  return 0;
}

