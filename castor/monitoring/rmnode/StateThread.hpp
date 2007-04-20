/******************************************************************************
 *                      StateThread.hpp
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
 * @(#)$RCSfile$ $Author$
 *
 * The StateThread of the RmNode daemon collects and send to
 * the rmmaster the state of the node on which it runs.
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMNODE_STATETHREAD_HPP
#define RMNODE_STATETHREAD_HPP 1

#include "castor/server/IThread.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"

namespace castor {

  namespace monitoring {

    // Forward declaration
    class DiskServerStateReport;
    class FileSystemStateReport;

    namespace rmnode {

      /**
       * StateThread  thread.
       */
      class StateThread : public castor::server::IThread {

      public:

        /**
         * constructor
         * @param rmMasterHost the rmMasterHost to which we should log
         * @param rmMasterPort the port on which rmMaster is listening
         */
        StateThread(std::string rmMasterHost, int rmMasterPort);

        /**
         * destructor
         */
        virtual ~StateThread() throw();

        /**
         * Method called once per interval
         */
        virtual void run(void *param)
          throw(castor::exception::Exception);
      
        /// not implemented
        virtual void stop() {};

      private:

        /**
         * Collects the diskServer state
         * The user is responsible for deleting the returned object
         */
        castor::monitoring::DiskServerStateReport* collectDiskServerState()
          throw(castor::exception::Exception);

        /**
         * Collects a given fileSystem state.
         * The user is responsible for deleting the returned object
         * @param mountPoint the mountPoint of the fileSystem to deal with
         * @param minFreeSpace the minFreeSpace to use for the fileSystem
         * @param maxFreeSpace the maxFreeSpace to use for the fileSystem
         * @param minAllowedFreeSpace the minAllowedFreeSpace to use for the
	 * fileSystem
         */
        castor::monitoring::FileSystemStateReport* collectFileSystemState
        (std::string mountPoint, float minFreeSpace,
	 float maxFreeSpace, float minAllowedFreeSpace)
          throw(castor::exception::Exception);

      private:
      
        /// RmMaster host
        std::string m_rmMasterHost;

        /// RmMaster port
        int m_rmMasterPort;

      };

    } // end of namespace rmnode

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMNODE_STATETHREAD_HPP
