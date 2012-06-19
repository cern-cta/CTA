
/******************************************************************************
 *                     VdqmRequestsProducerThread.hpp
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
 * This thread polls the DB to get the list of requests to be sent
 * to VDQM, sends them and updates the DB with the new VDQM
 * MountTransactionId
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef VDQMREQUESTSPRODUCER_THREAD_HPP
#define VDQMREQUESTSPRODUCER_THREAD_HPP 1

#include "castor/server/IThread.hpp"
#include "castor/BaseObject.hpp"
#include "castor/tape/utils/ShutdownBoolFunctor.hpp"
#include "castor/tape/tapegateway/daemon/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/VmgrTapeGatewayHelper.hpp"

namespace castor     {
  namespace tape       {
    namespace tapegateway{

      /**
       *  VdqmRequestsProducer tread.
       */

      class VdqmRequestsProducerThread : public virtual castor::server::IThread,
                                         public castor::BaseObject {

      public:

        /* constructor
         * @apram port : the port on which the VQQM should be contacted
         */
        VdqmRequestsProducerThread(int port);

        /* destructor */
        virtual ~VdqmRequestsProducerThread() throw() {};

        /**
         * Initialization of the thread.
         */
        virtual void init() {}

        /* run method
         * goes to the stager DB to get the list of requests to be sent
         * to VDQM, then sends them and updates the DB with the new VDQM
         * MountTransactionId
         * @param generic parameter, ignored in this case
         */
        virtual void run(void* param) throw();

        /**
         * Stop of the thread
         */
        virtual void stop() {m_shuttingDown.set();}

      private:

        /** get a tape to handle, either for recall or migration
         * @param oraSvc the ITapeGatewaySvc to use for stager DB access
         * @param vid a string filled with the VID of the selected tape
         * @param vdqmPriority an int filled with the priority to use in the call to VDQM
         * @param mode the mode of access to the tape (WRITE_DISABLE or WRITE_ENABLE)
         * @return whether a tape was found or not
         */
        bool getTapeToHandle(castor::tape::tapegateway::ITapeGatewaySvc* oraSvc, 
                             std::string &vid,
                             int &vdqmPriority,
                             int &mode)
          throw();

        /**
         * Check the status of a tape in VMGR and cancels the recall/migration
         * in case the tape is not available
         * @param oraSvc the ITapeGatewaySvc to use for stager DB access
         * @param vid the tape
         * @param mode the mode of access to the tape (WRITE_DISABLE or WRITE_ENABLE)
         * @return a struct TapeInfo containing all details on the tape.
         * in case of failure, this struct will be empty, with an empty dng in particular
         */
        TapeInfo checkInVMGR(castor::tape::tapegateway::ITapeGatewaySvc* oraSvc, 
                             const std::string &vid,
                             const int mode)
          throw();

        /**
         * Send a request to VDQM for the given tape
         * @param oraSvc the ITapeGatewaySvc to use for stager DB access
         * @param vid the tape
         * @param dgn the dgn to be used
         * @param mode the mode of access to the tape (WRITE_DISABLE or WRITE_ENABLE)
         * @param label the label of the tape
         * @param density the density of the tape
         * @param vdqmPriority the priority to be used for VDQM
         * @exception no exception thrown. In case the method fails to send a
         * request to VDQM, it will only log but will not report to the calles
         */
        void sendToVDQM(castor::tape::tapegateway::ITapeGatewaySvc* oraSvc, 
                        const std::string &vid,
                        const char *dgn,
                        const int mode,
                        const char *label,
                        const char *density,
                        const int vdqmPriority)
          throw();

      private:

        // port on which to connect to VDQM
        int m_port;

        // functor to know whether the thread is shutting down
        utils::ShutdownBoolFunctor m_shuttingDown;

      };

    } // end of tapegateway
  } // end of namespace tape
} // end of namespace castor

#endif // VDQMREQUESTSPRODUCER_THREAD_HPP
