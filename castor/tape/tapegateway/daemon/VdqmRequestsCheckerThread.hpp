
/******************************************************************************
 *                     VdqmRequestsCheckerThread.hpp
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
 * Regularly checks that ongoing recalls and migrations have an associated
 * request in VDQM. Cleans up when it's not the case
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef VDQMREQUESTSCHECKER_THREAD_HPP
#define VDQMREQUESTSCHECKER_THREAD_HPP 1

#include "castor/BaseObject.hpp"
#include "castor/server/IThread.hpp"
#include "castor/tape/utils/ShutdownBoolFunctor.hpp"

namespace castor     {
  namespace tape       {
    namespace tapegateway{

      /**
       * VdqmRequestsChecker tread.
       * Regularly checks that ongoing recalls and migrations have an associated
       * request in VDQM. Cleans up when it's not the case
       */
      class VdqmRequestsCheckerThread : public virtual castor::server::IThread,
                                        public castor::BaseObject {
      public:

        /** constructor
         * @param timeout the delay within which requests are not rechecked
         */
        VdqmRequestsCheckerThread(u_signed64 timeOut);

        /// destructor
        virtual ~VdqmRequestsCheckerThread() throw() {};

        /**
         * Initialization of the thread.
         */
        virtual void init() {}

        /**
         * Main work for this thread
         */
        virtual void run(void* param);

        /**
         * Stop of the thread
         */
        virtual void stop() {m_shuttingDown.set();}

      private:

        /// only requests not checked for more than this time out are considered
        u_signed64 m_timeOut;

        /// a functor telling whether the thread is shutting down
        utils::ShutdownBoolFunctor m_shuttingDown;
      };

    } // end of tapegateway
  } // end of namespace tape
} // end of namespace castor

#endif // VDQMREQUESTSCHECKER_THREAD_HPP
