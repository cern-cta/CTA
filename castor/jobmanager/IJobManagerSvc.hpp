/******************************************************************************
 *                      IJobManagerSvc.hpp
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
 * @(#)$RCSfile: IJobManagerSvc.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2008/04/21 11:53:00 $ $Author: waldron $
 *
 * This class provides methods for managing jobs
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef JOBMANAGER_IJOBMANAGERSVC_HPP
#define JOBMANAGER_IJOBMANAGERSVC_HPP 1

// Include files
#include "castor/stager/ICommonSvc.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/jobmanager/JobSubmissionRequest.hpp"
#include "castor/jobmanager/DiskServerResource.hpp"
#include "castor/jobmanager/FileSystemResource.hpp"
#include <map>


namespace castor {

  namespace jobmanager {

    /**
     * This classs provides methods related to managing jobs in the database
     */
    class IJobManagerSvc: public virtual castor::stager::ICommonSvc {

    public:

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
	throw(castor::exception::Exception) = 0;

      /**
       * Retrieve the next job to be scheduled from the database with
       * status SUBREQUEST_READYFORSCHED.
       * @exception Exception in case of error
       */
      virtual castor::jobmanager::JobSubmissionRequest *jobToSchedule()
	throw(castor::exception::Exception) = 0;

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
	throw(castor::exception::Exception) = 0;

      /**
       * Get a list of all diskservers, their associated filesystems and
       * the service class they are in.
       * @exception Exception in case of error
       */
      virtual std::map
      <std::string, castor::jobmanager::DiskServerResource *>
      getSchedulerResources()
	throw(castor::exception::Exception) = 0;

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
	throw(castor::exception::Exception) = 0;

      /**
       * Get a list of all service classes which are classified as having
       * diskonly behaviour and no longer have space available.
       * @exception Exception in case of error
       */
      virtual std::vector<std::string> getSvcClassesWithNoSpace()
	throw(castor::exception::Exception) = 0;
      
    };

  } // End of namespace jobmanager

} // End of namespace castor

#endif // JOBMANAGER_IJOBMANAGERSVC_HPP
