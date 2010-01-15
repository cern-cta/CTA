/******************************************************************************
 *                      OraJobManagerSvc.hpp
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
 * @(#)$RCSfile: OraJobManagerSvc.hpp,v $ $Revision: 1.9 $ $Release$ $Date: 2009/01/05 15:53:57 $ $Author: sponcec3 $
 *
 * Implementation of the IJobManagerSvc for Oracle
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef ORA_ORAJOBMANAGERSVC_HPP
#define ORA_ORAJOBMANAGERSVC_HPP 1

// Include files
#include "castor/BaseSvc.hpp"
#include "castor/db/newora/OraCommonSvc.hpp"
#include "castor/jobmanager/IJobManagerSvc.hpp"
#include "castor/jobmanager/JobSubmissionRequest.hpp"
#include "occi.h"
#include <map>
#include <vector>

// LSF headers
extern "C" {
#ifndef LSBATCH_H
#include "lsf/lssched.h"
#include "lsf/lsbatch.h"
#endif
}

namespace castor {

  namespace jobmanager {

    namespace ora {

      /**
       * Implementation of the IJobManagerSvc for Oracle
       */
      class OraJobManagerSvc: public castor::db::ora::OraCommonSvc,
                              public virtual castor::jobmanager::IJobManagerSvc {

      public:

        /**
         * Default constructor
         */
        OraJobManagerSvc(const std::string name);

        /**
         * Default destructor
         */
        virtual ~OraJobManagerSvc() throw();

        /**
         * Get the service id
         */
        virtual inline const unsigned int id() const;

        /**
         * Get the service id
         */
        static const unsigned int ID();

        /**
         * Reset the converter statements
         */
        void reset() throw();

        /**
         * Retrieve the next job to be scheduled from the database with
         * status SUBREQUEST_READYFORSCHED.
         * @exception Exception in case of error
         */
        virtual castor::jobmanager::JobSubmissionRequest *jobToSchedule()
          throw(castor::exception::Exception);

        /**
         * Update the status of a subrequest in the database to the value
         * defined in the status parameter. This method will only modify
         * subrequests that are in a SUBREQEST_BEINGSCHED status.
         * @param request the request to update
         * @param status the new status of the subrequest
         * @exception Exception in case of error
         */
        virtual void updateSchedulerJob
        (const castor::jobmanager::JobSubmissionRequest *request,
         const castor::stager::SubRequestStatusCodes status)
          throw(castor::exception::Exception);

        /**
         * Method to return a list of transfers from the stager database along
         * with any potential reason why the transfers should be terminated.
         * @return A map where the key is the job name (SubReq UUID) and the
         * value is a JobInfo object
         * @exception Exception in case of error
         */
        virtual std::map<std::string, castor::jobmanager::JobInfo>
        getSchedulerJobsFromDB()
          throw(castor::exception::Exception);

        /**
         * Fail a scheduler job in the stager database. This involves failing
         * the SubRequest and calling the necessary cleanup procedures on behalf
         * of the job.
         * The stager error service will then pick up the subrequest and
         * notify the client of the termination. This method will only modify
         * subrequests that are in a SUBREQUEST_READY or SUBREQEST_BEINGSCHED
         * status
         * @param A vector of std::pairs detailing the list of SubRequest uuid's
         * to fail and the reason why.
         * @return A list of SubRequest uuids that were failed.
         * @exception Exception in case of error
         */
        virtual std::vector<std::string> jobFailed
        (const std::vector<std::pair<std::string, int> > jobs)
          throw(castor::exception::Exception);

      private:

        /// SQL statement for function jobToSchedule
        static const std::string s_jobToScheduleString;

        /// SQL statement object for jobToSchedule
        oracle::occi::Statement *m_jobToScheduleStatement;

        /// SQL statement for function updateSchedulerJob
        static const std::string s_updateSchedulerJobString;

        /// SQL statement object for updateSchedulerJob
        oracle::occi::Statement *m_updateSchedulerJobStatement;

        /// SQL statement for function getSchedulerJobsFromDB
        static const std::string s_getSchedulerJobsFromDBString;

        /// SQL statement object for getSchedulerJobsFromDB
        oracle::occi::Statement *m_getSchedulerJobsFromDBStatement;

        /// SQL statement for function jobFailed
        static const std::string s_jobFailedString;

        /// SQL statement object for jobFailed
        oracle::occi::Statement *m_jobFailedStatement;

      };

    } // End of namespace ora

  } // End of namespace jobmanager

} // End of namespace castor

#endif // ORA_ORAJOBMANAGERSVC_HPP
