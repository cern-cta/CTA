/******************************************************************************
 *                      ManagementThread.hpp
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
 * @(#)$RCSfile: ManagementThread.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/03/23 15:26:20 $ $Author: sponcec3 $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef JOBMANAGER_MANAGEMENT_THREAD_HPP
#define JOBMANAGER_MANAGEMENT_THREAD_HPP 1

// Include files
#include "castor/jobmanager/IJobManagerSvc.hpp"
#include "castor/jobmanager/JobRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"
#include <map>

// LSF headers
extern "C" {
#ifndef LSBATCH_H
#include "lsf/lssched.h"
#include "lsf/lsbatch.h"
#endif
}

namespace castor {

  namespace jobmanager {

    /**
     * Management Thread
     */
    class ManagementThread: public castor::server::IThread {

    public:

      /**
       * Default constructor
       */
      ManagementThread()
      throw(castor::exception::Exception);

      /**
       * Default destructor
       */
      virtual ~ManagementThread() throw() {};

      /// Initialization of the thread
      virtual void init();

      /**
       * Method called periodically to check if actions need to be taken on
       * behalf of a job in LSF
       */
      virtual void run(void *param);

      /// Not implemented
      virtual void stop() {};

      /**
       * Get a list of all LSF jobs.
       * @return A map where the key is the job name (SubReq UUID) and the
       * value, a JobRequest object providing information about the job.
       */
      std::map<std::string, castor::jobmanager::JobRequest> getSchedulerJobs()
        throw(castor::exception::Exception);

      /**
       * Method used to return a JobRequest object whose attributes have been
       * set based on the contents of a jobs external scheduler options.
       * @param job The LSF job structure
       * @return The jobRequest object
       * @note Not all fields of the jobRequest object are filled, only those
       * relevant to process the job later.
       */
      castor::jobmanager::JobRequest extractJobInfo(const jobInfoEnt *job)
        throw(castor::exception::Exception);

      /**
       * Process an LSF job to determine whether it exited the queue abnormally
       * timed out or has resource requirements that can no longer by fulfilled
       * @param job A job request object providing information about the job
       * being processed
       * @param noSpace A flag to indicate that the job should be terminated
       * as no space is available in the target service class.
       * @param noFsAvail A flag to indicate that the job should be terminated
       * because the requested filesystems are not available.
       * @return 0 if no operation was performed or error code (errno value)
       * which provides the reason for the termination.
       */
      int processJob(const castor::jobmanager::JobRequest job,
                     const bool noSpace,
                     const bool noFsAvail);

      /**
       * bkill a job in LSF.
       * @param jobId The job id to terminate in LSF
       * @return true if the job was terminated otherwise false
       */
      bool killJob(const LS_LONG_INT jobId);

    private:

      /// Jobmanager service to call oracle procedures
      castor::jobmanager::IJobManagerSvc *m_jobManagerService;

      /// The default queue in LSF
      std::string m_defaultQueue;

      /// Kill jobs if all requested filesystems are no longer available?
      bool m_resReqKill;

      /// The maximum pending time in LSF
      u_signed64 m_diskCopyPendingTimeout;

      /// A container holding the timeout values for all service classes
      std::map<std::string, u_signed64> m_pendingTimeouts;

    };

  } // End of namespace jobmanager

} // End of namespace castor

#endif // JOBMANAGER_MANAGEMENT_THREAD_HPP
