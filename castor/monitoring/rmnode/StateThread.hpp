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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * The StateThread of the RmNode daemon collects information about the state
 * of the diskserver and its filesystems
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMNODE_STATETHREAD_HPP
#define RMNODE_STATETHREAD_HPP 1

#include "castor/server/IThread.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include <map>


namespace castor {

  namespace monitoring {

    // Forward declaration
    class DiskServerStateReport;
    class FileSystemStateReport;

    namespace rmnode {

      /**
       * StateThread thread.
       */
      class StateThread : public castor::server::IThread {

      public:

        /**
         * Constructor
         * @param hostList a list of hosts to which information should be sent
         * @param port the port on which the resource masters are listening
         */
        StateThread
	(std::map<std::string, u_signed64> hostList, int port);

        /**
         * Destructor
         */
        virtual ~StateThread() throw();

        /// Not implemented
        virtual void init() {};

        /**
         * Method called once per interval
         */
        virtual void run(void *param)
          throw(castor::exception::Exception);

        /// Not implemented
        virtual void stop() {};

      private:

        /**
         * Collects the diskServer state
         * The user is responsible for deleting the returned object
	 * @exception Exception in case of error
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
	 * @exception Exception in case of error
         */
        castor::monitoring::FileSystemStateReport* collectFileSystemState
        (std::string mountPoint, float minFreeSpace,
	 float maxFreeSpace, float minAllowedFreeSpace)
          throw(castor::exception::Exception);

      private:

        /// The list of hosts to send information to.
        std::map<std::string, u_signed64> m_hostList;

        /// The port of the listening resource masters
        int m_port;

      };

    } // end of namespace rmnode

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMNODE_STATETHREAD_HPP
