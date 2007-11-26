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
 * @(#)$RCSfile: ManagementThread.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/26 15:12:25 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef JOBMANAGER_MANAGEMENT_THREAD_HPP
#define JOBMANAGER_MANAGEMENT_THREAD_HPP 1

// Include files
#include "castor/jobmanager/IJobManagerSvc.hpp"
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
      ManagementThread();

      /**
       * Constructor which initializes the jobmanager service
       * @param timeout The threads timeout interval
       * @exception Exception in case of error
       */
      ManagementThread(int timeout)
	throw(castor::exception::Exception);

      /**
       * Default destructor
       */
      virtual ~ManagementThread() throw() {};

      /// Not implemented
      virtual void init() {};

      /**
       * Method called periodically to check if actions need to be taken on
       * behalf of a job in LSF
       */
      virtual void run(void *param);

      /// Not implemented
      virtual void stop() {};

      /**
       * Process an LSF job to determine whether it exited the queue abnormally
       * timed out or has resource requirements that can no longer by fulfilled
       * @param job The LSF job structure
       */
      virtual void processJob(jobInfoEnt *job);
    
      /**
       * Terminate a LSF job and the subrequest associated with it using the
       * provided error code.
       * @param jobId The job id to terminate in LSF. If 0 only the subrequest
       * will be terminated.
       * @param requestId The jobs request Id
       * @param subRequestId The jobs sub request Id
       * @param fileId The Cns invariant
       * @param errorCode The error code to use when terminating the job
       */
      virtual bool terminateRequest(LS_LONG_INT jobId,
				    Cuuid_t requestId, 
				    Cuuid_t subRequestId,
				    Cns_fileid fileId,
				    int errorCode);

    private:

      /// Jobmanager service to call oracle procedures
      castor::jobmanager::IJobManagerSvc *m_jobManagerService;

      /// The LSF CleanPeriod value obtained from lsb.params
      int m_cleanPeriod;

      /// The default queue in LSF
      std::string m_defaultQueue;

      /// The threads timeout interval
      int m_timeout;

      /// The LSF_API has been initialized ?
      bool m_initialized;

      /// Kill jobs if all requested filesystems are no longer available?
      bool m_resReqKill;
      
      /// The maximum pending time in LSF
      u_signed64 m_diskCopyPendingTimeout;

      /// A container holding the timeout values for all service classes
      std::map<std::string, u_signed64> m_pendingTimeouts;
 
      /// A container holding a list of jobs already processed
      std::map<std::string, u_signed64> m_processedCache;
    
      /// A container holding the resources available for scheduling
      std::map<std::string, castor::jobmanager::DiskServerResource *>
      *m_schedulerResources;

    };

  } // End of namespace jobmanager

} // End of namespace castor

#endif // JOBMANAGER_MANAGEMENT_THREAD_HPP
