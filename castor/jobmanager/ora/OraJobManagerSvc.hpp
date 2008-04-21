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
 * @(#)$RCSfile: OraJobManagerSvc.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2008/04/21 11:53:01 $ $Author: waldron $
 *
 * Implementation of the IJobManagerSvc for Oracle
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef ORA_ORAJOBMANAGERSVC_HPP
#define ORA_ORAJOBMANAGERSVC_HPP 1

// Include files
#include "castor/BaseSvc.hpp"
#ifdef ORACDBC
#include "castor/db/newora/OraCommonSvc.hpp"
#else
#include "castor/db/ora/OraCommonSvc.hpp"
#endif
#include "castor/jobmanager/IJobManagerSvc.hpp"
#include "castor/jobmanager/JobSubmissionRequest.hpp"
#include "occi.h"
#include <map>
#include <vector>

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
	 * Fail a scheduler job in the stager database. This involves failing
	 * the subrequest and calling the necessary cleanup procedures
	 * on behalf of the job.
	 * The stager error service will then pick up the subrequest and
	 * notify the client of the termination. This method will only modify
	 * subrequests that are in a SUBREQUEST_READY or SUBREQEST_BEINGSCHED
	 * status.
	 * @param subReqId The SubRequest id to update
	 * @param errorCode The error code associated with the failure
	 * @exception Exception in case of error
	 */
	virtual bool failSchedulerJob
	(const std::string subReqId, const int errorCode)
	  throw(castor::exception::Exception);

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
	 * Get a list of all diskservers, their associated filesystems and
	 * the service class they are in.
	 * @exception Exception in case of error
	 */
	virtual std::map
	<std::string, castor::jobmanager::DiskServerResource *>
	getSchedulerResources()
	  throw(castor::exception::Exception);

	/**
	 * Run post job checks on an exited or terminated job. We run these
	 * checks for two reasons:
	 * A) to make sure that the subrequest and waiting subrequests are
	 * updated accordingly when a job is terminated/killed and
	 * B) to make sure that the status of the subrequest is not
	 * SUBREQUEST_READY after a job has successfully ended. If this is
	 * the case then it is an indication that LSF failed to schedule the
	 * job and gave up. As a consequence of LSF giving up the stager
	 * database is left in an inconsistent state which this method
	 * attempts to rectify.
	 * @param subReqId The SubRequest id to update
	 * @param errorCode The error code associated with the failure
	 * @exception Exception in case of error
	 */
	virtual bool postJobChecks
	(const std::string subReqId, const int errorCode)
	  throw(castor::exception::Exception);

	/**
	 * Get a list of all service classes which are classified as having
	 * diskonly behaviour and no longer have space available.
	 * @exception Exception in case of error
	 */
	virtual std::vector<std::string> getSvcClassesWithNoSpace()
	  throw(castor::exception::Exception);

      private:

	/// SQL statement for function failSchedulerJob
	static const std::string s_failSchedulerJobString;

	/// SQL statement object for failSchedulerJob
	oracle::occi::Statement *m_failSchedulerJobStatement;

	/// SQL statement for function jobToSchedule
	static const std::string s_jobToScheduleString;

	/// SQL statement object for jobToSchedule
	oracle::occi::Statement *m_jobToScheduleStatement;

	/// SQL statement for function updateSchedulerJob
	static const std::string s_updateSchedulerJobString;

	/// SQL statement object for updateSchedulerJob
	oracle::occi::Statement *m_updateSchedulerJobStatement;

	/// SQL statement for function getSchedulerResources
	static const std::string s_getSchedulerResourcesString;

	/// SQL statement object for getSchedulerResources
	oracle::occi::Statement *m_getSchedulerResourcesStatement;

	/// SQL statement for function postJobChecks
	static const std::string s_postJobChecksString;

	/// SQL statement object for postJobChecks
	oracle::occi::Statement *m_postJobChecksStatement;

	/// SQL statement for function getSvcClassesWithNoSpace
	static const std::string s_getSvcClassesWithNoSpaceString;

	/// SQL statement object for getSvcClassesWithNoSpace
	oracle::occi::Statement *m_getSvcClassesWithNoSpaceStatement;

      };

    } // End of namespace ora

  } // End of namespace jobmanager

} // End of namespace castor

#endif // ORA_ORAJOBMANAGERSVC_HPP
