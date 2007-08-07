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
 * @(#)$RCSfile: OraJobManagerSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/08/07 14:56:33 $ $Author: waldron $
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
	 * Terminate a subrequest in SUBREQUEST_STAGEOUT i.e. waiting to be
	 * started on a diskserver. Set the subrequest to SUBREQUEST_FAILED
	 * with the given error code. The stager error service will then pick 
	 * up the subrequest and notify the client of the termination.
	 * @param subReqId The SubRequest id to update
	 * @param errorCode The error code associated with the failure
	 * @param errorMessage The error message associated with the error. If
	 * NULL the SQL procedure will try to fill in the message based on the
	 * error given code.
	 * @exception Exception in case of error
	 */
	virtual bool failSchedulerJob(const std::string subReqId, 
				      const int errorCode,
				      const std::string errorMessage)
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

      };

    } // End of namespace ora

  } // End of namespace jobmanager

} // End of namespace castor

#endif // ORA_ORAJOBMANAGERSVC_HPP
