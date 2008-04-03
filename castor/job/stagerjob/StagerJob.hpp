/******************************************************************************
 *                      StagerJob.hpp
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
 * some useful declarations around the scheduler job implementation
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGERJOB_STAGERJOB_HPP
#define STAGERJOB_STAGERJOB_HPP 1

// Include Files
#include <map>
#include <string>
#include "Cns_api.h"
#include "Cuuid.h"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward declaration
  class IClient;
  namespace stager {
    class IJobSvc;
  }

  namespace job {

    namespace stagerjob {

      // Forward declaration
      class IPlugin;

      /**
       * internal enum describing the access mode
       */
      enum AccessMode {
        ReadOnly,
        WriteOnly,
        ReadWrite
      };

      /**
       * internal struct holding the list of arguments passed to stagerJob
       */
      struct InputArguments {
        struct Cns_fileid fileId;
        Cuuid_t requestUuid;
        Cuuid_t subRequestUuid;
        std::string rawRequestUuid;
        std::string rawSubRequestUuid;
        std::string protocol;
        u_signed64 subRequestId;
        int type;
        std::string diskServer;
        std::string fileSystem;
        enum AccessMode accessMode;
        castor::IClient* client;
        bool isSecure;
      };

      /**
       * internal struct holding the context passed to a plugin
       */
      struct PluginContext {
        int socket;
        std::string host;
        int port;
        mode_t mask;
        std::string fullDestPath;
        int childPid;
        castor::stager::IJobSvc* jobSvc;
      };

      /**
       * DLF messages defined for the stager job
       */
      enum StagerJobDlfMessages {
        // system call errors
        CREATFAILED =  1, /* Failed to create empty file */
        FCLOSEFAILED = 2, /* Failed to close file */
        SCLOSEFAILED = 3, /* Failed to close socket */
        CHDIRFAILED =  4, /* Failed to change directory to tmp */
        DUP2FAILED =   5, /* Failed to duplicate socket */
        MOVERNOTEXEC = 6, /* Mover program can not be executed. Check permissions */

        // Invalid configurations or parameters
        INVRLIMIT =     10, /* rlimit found in config file exceeds hard limit */
        INVLIMCPU =     11, /* Invalid LimitCPU value found in configuration file, will be ignored */
        CPULIMTOOHIGH = 12, /* CPU limit value found in config file exceeds hard limit, using hard limit */
        INVRETRYINT =   13, /* Invalid DiskCopy/RetryInterval option, using default */
        INVRETRYNBAT =  14, /* Invalid DiskCopy/RetryAttempts option, using default */

        // Informative logs
        JOBSTARTED =  20, /* Job Started */
        JOBENDED =    21, /* Job exiting successfully */
        JOBFAILED =   22, /* Job failed */
        JOBORIGCRED = 23, /* Credentials at start time */
        JOBACTCRED =  24, /* Actual credentials used */
        JOBNOOP =     25, /* No operation performed */
        FORKMOVER =   26, /* Forking mover */
        REQCANCELED = 27, /* Request canceled */
        MOVERPORT =   28, /* Mover will use the following port */
        MOVERFORK =   29, /* Mover fork uses the following command line */
        ACCEPTCONN =  30, /* Client connected */

        // Errors
        STAT64FAIL =    40, /* rfio_stat64 error */
        CHILDEXITED =   41, /* Child exited */
        CHILDSIGNALED = 42, /* Child exited due to uncaught signal */
        CHILDSTOPPED =  43, /* Child was stopped */

        // Protocol specific. Should not be here if the plugins
        // were properly packaged in separate libs
        GSIBADPORT =    44, /* Invalid port range for GridFTP in config file. using default */
        GSIBADMINPORT = 45, /* Invalid lower bound for GridFTP port range in config file. Using default */
        GSIBADMAXPORT = 46, /* Invalid upper bound for GridFTP port range in config file. Using default */
        GSIBADMINVAL =  47, /* Lower bound for GridFTP port range not in valid range. Using default */
        GSIBADMAXVAL =  48, /* Upper bound for GridFTP port range not in valid range. Using default */
        GSIPORTRANGE =  49, /* GridFTP Port range */
        GSIPORTUSED =   50  /* GridFTP Socket bound */
      };

      /**
       * a static class holding the list of available plugins
       */
      class StagerJob {

      public:

        /**
         * gets the plugin for a given protocol
         * @param protocol the requested protocol
         */
        static IPlugin* getPlugin(std::string protocol)
          throw (castor::exception::Exception);

        /**
         * registers a new plugin
         * @param protocol the associatedprotocol
         * @param protocol the plugin to register
         */
        static void registerPlugin(std::string protocol,
                                   IPlugin* plugin)
          throw();

      private:

        /**
         * the list of available Plugins
         */
        static std::map<std::string, IPlugin*> s_plugins;

      }; // end of class StagerJob

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_STAGERJOB_HPP
