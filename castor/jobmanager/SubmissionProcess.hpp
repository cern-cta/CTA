/******************************************************************************
 *                      SubmissionProcess.cpp
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
 * @(#)$RCSfile: SubmissionProcess.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2008/09/22 12:33:51 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef JOBMANAGER_SUBMISSION_PROCESS_HPP
#define JOBMANAGER_SUBMISSION_PROCESS_HPP 1

// Include files
#include "castor/server/ForkedProcessPool.hpp"
#include "castor/jobmanager/IJobManagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"

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
     * Submission Process
     */
    class SubmissionProcess: public castor::server::IThread {

    public:

      /**
       * Default constructor
       */
      SubmissionProcess() {};

      /**
       * Constructor
       * @param reverseUidLookup flag to indicate whether reverse uid lookups
       * should be performed before submission into the scheduler
       * @param sharedLSFResource the location of LSF notification files
       */
      SubmissionProcess(bool reverseUidLookup, std::string sharedLSFResource);

      /**
       * Default destructor
       */
      virtual ~SubmissionProcess() throw() {};

      /**
       * Hook called after the creating a child process in the forked process
       * pool. Here we simple initialize the jobmanager service.
       * @exception Exception in case of error
       */
      virtual void init()
        throw(castor::exception::Exception);

      /**
       * Method called by the forked process pool dispatcher when processing
       * of a new request is required.
       */
      virtual void run(void *param);

      /// Not Implemented
      virtual void stop() {};

      /**
       * Submit a job into the LSF scheduler. If an error occurs the method
       * will also fail the subrequest in the database.
       * @param request The submission request information needed to submit
       * a job into the scheduler.
       * @exception Exception in case of error
       */
      virtual void submitJob
      (castor::jobmanager::JobRequest *request)
        throw(castor::exception::Exception);

      /**
       * Faile the subrequest associated with the LSF job in the database
       * using the given errorCode.
       * @param request The submission request information
       * @param errorCode The error code to use when failing the job
       */
      virtual void failRequest
      (castor::jobmanager::JobRequest *request, int errorCode);

    private:

      /// Jobmanager service to call oracle procedures
      castor::jobmanager::IJobManagerSvc *m_jobManagerService;

      /// Should uid checks be performed?
      bool m_reverseUidLookup;

      /// The location of the LSF scheduler notification files
      std::string m_sharedLSFResource;

      /// How many times should we attempt to submit a job into LSF?
      unsigned int m_submitRetryAttempts;

      /// The interval to wait between submission attempts
      unsigned int m_submitRetryInterval;

      /// The maximum amount of time that a StageDiskReplicaRequest job can
      /// run for in minutes
      int m_maxDiskCopyRunTime;

      /// The request uuid of the job being processed
      Cuuid_t m_requestUuid;

      /// The sub request uuid of the job being processed
      Cuuid_t m_subRequestUuid;

      /// The Cns invariant of the job
      Cns_fileid m_fileId;

      /// The job to submit into LSF
      submit m_job;

      /// The reply from lsb_submit
      submitReply m_jobReply;

    };

  } // End of namespace jobmanager

} // End of namespace castor

#endif // JOBMANAGER_SUBMISSION_PROCESS_HPP
