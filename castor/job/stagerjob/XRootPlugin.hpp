/******************************************************************************
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
 *
 * Plugin of the stager job concerning XRoot
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

// Include Files
#include "castor/job/stagerjob/RawMoverPlugin.hpp"

namespace castor {

  namespace job {

    namespace stagerjob {

      /**
       * StagerJob plugin for the XRoot protocol
       */
      class XRootPlugin :
        public virtual castor::job::stagerjob::RawMoverPlugin {

      public:

        /**
         * Default constructor
         */
        XRootPlugin() throw();

        /**
         * Helper function to read data from the communication channel between
         * stagerjob and the local xrootd server.
         * @param socket The file descriptor to read data from
         * @param buf A pointer to a buffer in which to store data
         * @param len The amount of data to read from the socket
         * @param timeout The amount of time in seconds to wait for data to be
         * received.
         * @exception Exception in case of error.
         */
        void recvMessage(int socket, char *buf, ssize_t len, int timeout)
          ;

        /**
         * The preForkHook used to establish a control channel with the local
         * xrootd server prior to an xroot transfer taking place.
         * @param args The arguments given to the stager job
         * @param context The current context (localhost, port, etc...)
         */
        virtual void preForkHook(InputArguments &args,
                                 PluginContext &context)
          ;

        /**
         * Hook to wait for the closure of a file.
         * @param args The arguments given to the stager job
         * @param context The current context (localhost, port, etc...)
         * @param useChkSum flag to indicate whether checksum information
         * should be sent in the prepareForMigration call.
         * @param moverStatus status of the mover process. By default the
         * value is -1, indicating that the function should wait for child
         * processes to exit. If moverStatus > -1, the function will not
         * wait for child processes and the value of moverStatus 
         * argument will be used to indicate whether the transfer was
         * successful or not.
         */
        virtual void postForkHook(InputArguments &args,
                                  PluginContext &context,
                                  bool useChksSum = false,
                                  int moverStatus = -1)
          ;

        /// Not implemented
        virtual void execMover(InputArguments&,
                               PluginContext&)
           {};

      private:

        /// The amount of time in seconds to wait for the local xrootd server
        /// to connect to stagerjob.
        int m_openTimeout;

        /// The amount of time in seconds to wait for xrootd to send a CLOSE
        /// notification back to stagerjob.
        int m_closeTimeout;

      }; // end of class XRootPlugin

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

