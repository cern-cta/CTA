/******************************************************************************
 *                      SchedulerStatus.hpp
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
 * Singleton used to access scheduler related information.
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef RMMASTER_SCHEDULER_STATUS_HPP
#define RMMASTER_SCHEDULER_STATUS_HPP 1

// Include files
#include "castor/exception/Exception.hpp"
#include <pthread.h>
#include "occi.h"

namespace castor {

  namespace db { 
    namespace ora {
      // Forward declarations
      class OraCnvSvc;
    }
  }

  namespace monitoring {

    namespace rmmaster {

      /**
       * SchedulerStatus singleton class
       */
      class SchedulerStatus {

      public:

        /**
         * Static method to get this singleton's instance
         */
        static castor::monitoring::rmmaster::SchedulerStatus* getInstance()
          throw(castor::exception::Exception);

        /**
         * Get information about the current status of the scheduler and the monitoring.
         * Also tells whether we are the production resource master.
         * @param production pointer to a bool value to update with a flag
         * to indicate whether we are the production resource master.
         * @exception Exception in case of error
         */
        void getSchedulerStatus(bool &production)
          throw(castor::exception::Exception);

        /**
         * Get information about the current status of the scheduler.
         * @param production pointer to a boolean value to update with a flag
         * to indicate whether we are the production resource master.
         * @param masterName pointer to a string to store the current rmmaster
         * master name.
         * @param hostName pointer to a string to store the current hostname.
         * @exception Exception in case of error
         */
        void getSchedulerStatus(bool &production,
                                std::string &masterName,
                                std::string &hostName)
          throw(castor::exception::Exception);

      private:

        /// This singleton's instance
        static castor::monitoring::rmmaster::SchedulerStatus *s_instance;

        /**
         * Default constructor
         * @exception Exception in case of error
         */
        SchedulerStatus() throw (castor::exception::Exception);

        /**
         * Default destructor
         */
        virtual ~SchedulerStatus() throw();

        /**
         * access to the underlying database conversion service
         */
        castor::db::ora::OraCnvSvc* cnvSvc() throw (castor::exception::Exception);

        /**
         * Check whether we are the monitoring master. This is given by the ability to
         * take a lock in the DB. The one that has this lock is declared the master.
         * If it dies, somebody else will be able to take the lock and will become the
         * new master.
         * @return true if we are the monitoring master
         * @exception Exception in case of error
         */
        virtual bool isMonitoringMaster() throw(castor::exception::Exception);

      private:

        /// ORACLE conversion service
        castor::db::ora::OraCnvSvc* m_cnvSvc;

        /// Mutex to protect DB access when checking who is master
        pthread_mutex_t m_masterCheckLock;

        /// The previous production status
        bool m_prevProduction;

        /// The last time data were refreshed
        u_signed64 m_lastUpdate;

        /// says whether getSchedulerStatus was already called or not
        bool m_getSchedulerStatusCalled;

        /// SQL statement for function isMonitoringMaster, only used in case in noLSF mode
        static const std::string s_isMonitoringMasterStatementString;

        /// SQL statement object for function isMonitoringMaster, only used in case in noLSF mode
        oracle::occi::Statement *m_isMonitoringMasterStatement;
      };

    } // End of namespace rmmaster

  } // End of namespace monitoring

} // End of namespace castor

#endif // RMMASTER_SCHEDULER_STATUS_HPP
