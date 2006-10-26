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
 * @(#)$RCSfile$ $Author$
 *
 * The MetricsThread of the RmNode daemon collects and send to
 * the rmmaster the metrics of the node on which it runs.
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMNODE_METRICSTHREAD_HPP
#define RMNODE_METRICSTHREAD_HPP 1

#include "castor/server/IThread.hpp"

namespace castor {

  namespace monitoring {

    // Forward declaration
    class DiskServerState;
    class DiskServerMetricsReport;
    class FileSystemMetricsReport;

    namespace rmnode {

      /**
       * MetricsThread  thread.
       */
      class MetricsThread : public castor::server::IThread {

      public:

        /**
         * constructor
         * @param rmMasterHost the rmMasterHost to which we should log
         * @param rmMasterPort the port on which rmMaster is listening
         */
        MetricsThread(std::string rmMasterHost, int rmMasterPort);

        /**
         * destructor
         */
        virtual ~MetricsThread() throw();

        /**
         * Method called once per interval
         */
        virtual void run(void *param)
          throw(castor::exception::Exception);
      
        /// not implemented
        virtual void stop() {};

        /**
         * Collects the diskServer metrics
         * The user is responsible for deleting the returned object
         */
        castor::monitoring::DiskServerMetricsReport* collectDiskServerMetrics()
          throw(castor::exception::Exception);

        /**
         * Collects a given fileSystem metrics.
         * The user is responsible for deleting the returned object
         * @param mountPoint the mountPoint of the fileSystem to deal with
         */
        castor::monitoring::FileSystemMetricsReport* collectFileSystemMetrics
        (std::string mountPoint)
          throw(castor::exception::Exception);

      private:

        /**
         * Old methods gathering statistics.
         * To be replaced
         */
        int Crm_util_countstream(const char* fs, int*nr, int*nrw, int*nw);
        int Crm_util_io(const char* part,
                        u_signed64 *rd, u_signed64 *wr);
        int Crm_util_io_wrapper(const char* part,
                                u_signed64 *rd, u_signed64 *wr,
                                u_signed64 nr, u_signed64 brw, u_signed64 nw);

      private:
      
        /// RmMaster host
        std::string m_rmMasterHost;

        /// RmMaster port
        int m_rmMasterPort;

      };

    } // end of namespace rmnode

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMNODE_METRICSTHREAD_HPP
