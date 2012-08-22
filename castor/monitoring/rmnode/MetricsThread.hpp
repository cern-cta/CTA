/******************************************************************************
 *                      MetricsThread.hpp
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
 * The MetricsThread of the RmNode daemon collects the metrics of the
 * diskserver and sends them to the resource master
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMNODE_METRICSTHREAD_HPP
#define RMNODE_METRICSTHREAD_HPP 1

#include "castor/server/IThread.hpp"
#include <map>

namespace castor {

  namespace monitoring {

    // Forward declaration
    class DiskServerState;
    class DiskServerMetricsReport;
    class FileSystemStateReport;
    class FileSystemMetricsReport;

    namespace rmnode {

      /**
       * MetricsThread  thread.
       */
      class MetricsThread : public castor::server::IThread {

      public:

        /**
         * Constructor
         * @param hostList a list of hosts to which information should be sent
         * @param port the port on which the resource masters are listening
         */
        MetricsThread
	(std::map<std::string, u_signed64> hostList, int port);

        /**
         * Destructor
         */
        virtual ~MetricsThread() throw();

        /**
         * copy constructor. Not implemented so that it cannot be used
         */
        MetricsThread(const MetricsThread &s) throw();

        /**
         * assignement operator. Not implemented so that it cannot be used
         */
        MetricsThread &operator=(MetricsThread& obj) throw();

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
         * Collects the diskServer metrics
	 * @exception Exception in case of error
         */
        void collectDiskServerMetrics()
          throw(castor::exception::Exception);

        /**
         * Collects a given fileSystem metrics.
         * @param filesystem the file system object to be updated
	 * @exception Exception in case of error
         */
	void collectFileSystemMetrics
	(castor::monitoring::FileSystemMetricsReport* filesystem)
	  throw(castor::exception::Exception);

      private:

        /// The list of hosts to send information to.
        std::map<std::string, u_signed64> m_hostList;

        /// The port of the listening resource masters
        int m_port;

	/// DiskServerMetricsReport pointer
	castor::monitoring::DiskServerMetricsReport* m_diskServerMetrics;

	/// A list of invalid mountpoints
	std::map<std::string, u_signed64> m_invalidMountPoints;

      };

    } // end of namespace rmnode

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMNODE_METRICSTHREAD_HPP
