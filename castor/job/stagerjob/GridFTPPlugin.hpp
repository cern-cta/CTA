/******************************************************************************
 *                      GridFTPPlugin.hpp
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
 * Plugin of the stager job concerning GridFTP
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGERJOB_GRIDFTPPLUGIN_HPP
#define STAGERJOB_GRIDFTPPLUGIN_HPP 1

// Include Files
#include <string>
#include "castor/job/stagerjob/RawMoverPlugin.hpp"

namespace castor {

  namespace job {

    namespace stagerjob {

      /**
       * Small struct holding a GridFTP enviroment
       */
      struct Environment {
        std::string globus_location;
        std::string xroot_location;
        std::string use_cksum;
        std::pair<int, int> tcp_port_range;
        std::pair<int, int> tcp_source_range;
        std::string globus_logfile;
        std::string globus_logfile_netlogger;
        std::string globus_loglevel;
        std::string globus_x509_user_cert;
        std::string globus_x509_user_key;
        std::string dsi_module_extension;
      };

      /**
       * StagerJob plugin for the GridFTP protocol
       */
      class GridFTPPlugin :
        public virtual castor::job::stagerjob::RawMoverPlugin {

      public:

        /**
         * Default constructor
         */
        GridFTPPlugin() throw();

        /**
         * Returns the port range for GridFTP
         */
        virtual std::pair<int, int> getPortRange
        (castor::job::stagerjob::InputArguments &args) throw();

        /**
         * Hook for the code to be executed just before the mover fork.
         * Used to set the timeout value on select calls waiting for
         * client connections.
         * @param args the arguments given to the stager job
         * @param context the current context (localhost, port, etc...)
         */
        virtual void preForkHook(InputArguments &args,
                                 PluginContext &context)
          throw (castor::exception::Exception);

        /**
         * Hook for the code to be executed just after the mover fork,
         * in the parent process. Only logging and calling the method
         * of InstrumentedPlugin.
         * @param args the arguments given to the stager job
         * @param context the current context (localhost, port, etc...)
         */
        virtual void postForkHook(InputArguments &args,
                                  PluginContext &context)
          throw (castor::exception::Exception);

        /**
         * Hook for the launching of the mover
         * @param args the arguments given to the stager job
         * @param context the current context (localhost, port, etc...)
         */
        virtual void execMover(InputArguments &args,
                               PluginContext &context)
          throw (castor::exception::Exception);

      private:

        /**
         * Get a given port range from the config file and checks it
         * @param name the config file entry
         */
        static std::pair<int, int> getPortRange
        (InputArguments &args, std::string name)
          throw();

        /**
         * Gather all environment values
         * @param env the environment, to be filled
         */
        static void getEnvironment(InputArguments &args,
                                   Environment &env)
          throw (castor::exception::Exception);

      }; // end of class GridFTPPlugin

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_GRIDFTPPLUGIN_HPP
