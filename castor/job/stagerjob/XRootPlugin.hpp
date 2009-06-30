/******************************************************************************
 *                      XRootPlugin.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * Plugin of the stager job concerning XRoot
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGERJOB_XROOTPLUGIN_HPP
#define STAGERJOB_XROOTPLUGIN_HPP 1

// Include Files
#include "castor/job/stagerjob/RawMoverPlugin.hpp"

namespace castor {

  namespace job {

    namespace stagerjob {

      /**
       * StagerJob plugin for the XRoot protocol
       */
      class XRootPlugin :
        public virtual castor::job::stagerjob::RawMoverPlugin {

      public:

        /**
         * Default constructor
         */
        XRootPlugin() throw();

        /**
         *
         */
        void recvMessage(int socket, char *buf, ssize_t len, int timeout)
          throw(castor::exception::Exception);

        /**
         *
         */
        virtual void preForkHook(InputArguments &args,
                                 PluginContext &context)
          throw(castor::exception::Exception);

        /**
         *
         */
        virtual void postForkHook(InputArguments &args,
                                  PluginContext &context)
          throw (castor::exception::Exception);

        /**
         *
         */
        virtual void execMover(InputArguments &args,
                               PluginContext &context)
          throw (castor::exception::Exception) {};

      private:

        ///
        int m_openTimeout;

        ///
        int m_closeTimeout;

      }; // end of class XRootPlugin

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_XROOTPLUGIN_HPP
