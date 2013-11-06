/******************************************************************************
 *                      InstrumentedMoverPlugin.hpp
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
 *
 * Abstract plugin of the stager job suitable for instrumented
 * movers. That is movers that were modified to inform the
 * CASTOR framework of their behavior
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGERJOB_INSTRUMENTEDMOVERPLUGIN_HPP
#define STAGERJOB_INSTRUMENTEDMOVERPLUGIN_HPP 1

// Include Files
#include <string>
#include "castor/job/stagerjob/BasePlugin.hpp"

namespace castor {

  namespace job {

    namespace stagerjob {

      /**
       * Abstract plugin of the stager job suitable for instrumented
       * movers. That is movers that were modified to inform the
       * CASTOR framework of their behavior
       */
      class InstrumentedMoverPlugin :
        public castor::job::stagerjob::BasePlugin {

      public :

        /**
         * Default constructor
         * @param protocol the supported protocol
         */
        InstrumentedMoverPlugin(std::string protocol) throw();

        /**
         * Hook for the code to be executed just after the mover fork,
         * in the parent process.
         * Here we answer the client and check the exit status of the
         * mover in order to inform the stager.
         * @param args the arguments given to the stager job
         * @param context the current context (localhost, port, etc...)
         * @param useChkSum flag to indicate whether checksum information
         * should be sent in the prepareForMigration call.
         * @param moverStatus status of the mover process. By default the
         * value is -1, indicating that the function should wait for child
         * processes to exit. If moverStatus > -1, the function will not
         * wait for child processes and the value of moverStatus 
         * argument will be used to indicate whether the transfer was
         * successful or not.
         */
        virtual void postForkHook(InputArguments &args,
                                  PluginContext &context,
                                  bool useChksSum = false,
                                  int moverStatus = -1)
          throw (castor::exception::Exception);

      protected :

        /**
         * Waits for the child to end and informs stager
         * Used internally by postForkHook after the response was
         * sent to the client. Can be reused by children classes
         * reimplementing postForkHook with a different response
         * @param args the arguments given to the stager job
         * @param context the current context (localhost, port, etc...)
         */
        void waitChildAndInformStager(InputArguments &args,
                                      PluginContext &context)
          throw (castor::exception::Exception);

      }; // end of class InstrumentedMoverPlugin

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_INSTRUMENTEDMOVERPLUGIN_HPP
