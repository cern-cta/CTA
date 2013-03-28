/******************************************************************************
 *                      BasePlugin.hpp
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
 * Base class for a stagerjob plugin
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGERJOB_BASEPLUGIN_HPP
#define STAGERJOB_BASEPLUGIN_HPP 1

// include files
#include <string>
#include "castor/IClient.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/job/stagerjob/IPlugin.hpp"
#include "castor/job/stagerjob/StagerJob.hpp"

namespace castor {

  namespace job {

    namespace stagerjob {

      /**
       * Base, abstract class for a stagerjob plugin.
       * To be extended by each of the actual implementations
       */
      class BasePlugin : public IPlugin {

      public:

        /**
         * Default constructor, registering the plugin
         * @param protocol the supported protocol
         */
        BasePlugin(std::string protocol) throw();

        /**
         * Default implementation of getPortRange returning [1024:65535]
         */
        virtual std::pair<int, int> getPortRange
        (castor::job::stagerjob::InputArguments &args) throw();

        /**
         * Default, empty implementation of preForkHook
         * @param args the arguments given to the stager job
         * @param context the current context (localhost, port, etc...)
         */
        virtual void preForkHook
        (castor::job::stagerjob::InputArguments&,
         castor::job::stagerjob::PluginContext&)
          throw (castor::exception::Exception) {};

      protected:

        /**
         * Wait on a child (the mover) and know it failed or not
         * @param args the arguments given to the stager job
         * @return whether the child failed
         */
        bool waitForChild
        (castor::job::stagerjob::InputArguments &args) throw();

        /**
         * Set the select timeout for inetd mode
         * @param timeOut the new value in seconds
         */
        void setSelectTimeOut(time_t timeOut) {
          m_selectTimeOut = timeOut;
        }

        /**
         * Set the select timeout for inetd mode
         * @return the timout in seconds
         */
        time_t getSelectTimeOut() {
          return m_selectTimeOut;
        }

      private:

        /// The select timeout for inetd mode
        time_t m_selectTimeOut;

      }; // end of class BasePlugin

    } // end of namespace stager job

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_BASEPLUGIN_HPP
