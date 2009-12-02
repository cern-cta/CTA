/******************************************************************************
 *                      DispatchThread.cpp
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
 * @(#)$RCSfile: DispatchThread.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2009/03/23 15:26:20 $ $Author: sponcec3 $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef JOBMANAGER_DISPATCH_THREAD_HPP
#define JOBMANAGER_DISPATCH_THREAD_HPP 1

// Include files
#include "castor/server/SelectProcessThread.hpp"
#include "castor/server/ForkedProcessPool.hpp"
#include "castor/jobmanager/IJobManagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"

namespace castor {

  namespace jobmanager {

    /**
     * Dispatch Thread
     */
    class DispatchThread: public virtual castor::server::SelectProcessThread {

    public:

      /**
       * Default constructor
       */
      DispatchThread();

      /**
       * Constructor initializing the jobmanager service
       * @param processPool A pointer to the forked process pool
       * @exception Exception in case of error
       */
      DispatchThread(castor::server::ForkedProcessPool *processPool)
        throw(castor::exception::Exception);

      /// Initialization of the thread
      virtual void init();

      /**
       * Default destructor
       */
      virtual ~DispatchThread() throw() {};

      /**
       * Select a new job from the database that requires scheduling. This
       * method is called periodically and also woken up by external
       * notification from the stager.
       */
      virtual castor::IObject* select() throw();

      /**
       * Dispatch a job to the forked process pool assigning the task to a
       * child process for execution.
       * @param param The IObject returned by select
       */
      virtual void process(castor::IObject *param) throw();

    private:

      /// Jobmanager service to call oracle procedures
      castor::jobmanager::IJobManagerSvc *m_jobManagerService;

      /// The forked process pool
      castor::server::ForkedProcessPool *m_processPool;

      /// The request uuid of the job being processed
      Cuuid_t m_requestUuid;

      /// The sub request uuid of the job being processed
      Cuuid_t m_subRequestUuid;

      /// The Cns invariant of the job
      Cns_fileid m_fileId;

    };

  } // End of namespace jobmanager

} // End of namespace castor

#endif // JOBMANAGER_DISPATCH_THREAD_HPP
