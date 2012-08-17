/******************************************************************************
 *                      MainThread.hpp
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
 * @(#)$RCSfile: MainThread.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2009/03/23 15:26:20 $ $Author: sponcec3 $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef MAIN_THREAD_HPP
#define MAIN_THREAD_HPP 1

// Include files
#include "castor/job/d2dtransfer/IMover.hpp"
#include "castor/job/SharedResourceHelper.hpp"
#include "castor/server/IThread.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"


namespace castor {

  namespace job {

    namespace d2dtransfer {

      /**
       * Main Thread
       */
      class MainThread: public castor::server::IThread {

      public:

        /**
         * Default constructor
         */
        MainThread();

        /**
         * Constructor which supports processing command line arguments
         * @param argc the number of arguments supplied to the program
         * @param argv an array of pointers to the strings which represents
         * the values associated with the arguments
         * @exception Exception in case of error
         */
        MainThread(int argc, char *argv[])
          throw(castor::exception::Exception);

             /**
         * Default destructor
         */
        virtual ~MainThread() throw() {};

        /// Initialization of the thread
        virtual void init();

        /**
         * Executes the transfer between two diskservers. This runs once and
         * only once!
         */
        virtual void run(void *param);

        /**
         * Called by the BaseServer when a termination request has been
         * received by the signal handler. Here we simply forward the stop
         * request to the mover.
         */
        virtual void stop();

        /**
         * Parses the command line to set the movers options.
         * @param argc the number of arguments supplied to the program
         * @param argv an array of pointers to the strings which represents
         * the values associated with the arguments
         * @exception Exception in case of error
         */
        virtual void parseCommandLine(int argc, char *argv[])
          throw(castor::exception::Exception);

        /**
         * Changes the uid and gid of the running process
         * @param user the name of the user to set privileges to
         * @param group the name of the group to set privileges to
         * @exception Exception in case of error
         */
        void changeUidGid(std::string user, std::string group)
          throw(castor::exception::Exception);

        /**
         * Terminate the current process. If status does not indicate success
         * then disk2DiskCopyFailed will be called
         * @param diskCopyId id of the destination disk copy to be created
         * @param status the return code for exit;
         */
        virtual void terminate(u_signed64 diskCopyId, int status);

      protected:

        /**
         * Prints out the online help.
         * @param programName the name of the program usually argv[0]
         */
        virtual void help(std::string programName);

      private:

        /// The remote job service
        castor::stager::IJobSvc *m_jobSvc;

        /// The mover responsible for the actual data transfer
        castor::job::d2dtransfer::IMover *m_mover;

        /// The helper to download the resource file
        castor::job::SharedResourceHelper *m_resHelper;

        /// The start time of the mover
        u_signed64 m_startTime;

        /// The protocol used by the mover
        std::string m_protocol;

        /// The request uuid of the job being processed
        Cuuid_t m_requestUuid;

        /// The sub request uuid of the job being processed
        Cuuid_t m_subRequestUuid;

        /// The Cns invariant of the job
        Cns_fileid m_fileId;

        /// The name of the service class for the castor file
        std::string m_svcClass;

        /// The id of the destination diskcopy
        u_signed64 m_diskCopyId;

        /// The id of the source diskcopy
        u_signed64 m_sourceDiskCopyId;

        /// The location of the file to retrieve containing the diskserver and
        /// filesystem to write too
        std::string m_resourceFile;

        /// The creation time of the original request in seconds since EPOCH
        u_signed64 m_requestCreationTime;

      };

    } // End of namespace d2dtransfer

  } // End of namespace job

} // End of namespace castor

#endif // MAIN_THREAD_HPP
