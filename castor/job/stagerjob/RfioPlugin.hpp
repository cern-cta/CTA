/******************************************************************************
 *                      RfioPlugin.hpp
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
 * Plugin of the stager job concerning Rfio
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGERJOB_RFIOPLUGIN_HPP
#define STAGERJOB_RFIOPLUGIN_HPP 1

// Include Files
#include "castor/job/stagerjob/InstrumentedMoverPlugin.hpp"

namespace castor {

  namespace job {

    namespace stagerjob {

      /**
       * Small struct holding a Rfio enviroment
       */
      struct EnvironmentRfio {
        std::string globus_location;
        std::string globus_x509_user_cert;
        std::string globus_x509_user_key;
        std::string gridmapfile;
        std::string keytab_location;
        std::string csec_trace;
        std::string csec_mech;
        std::string csec_tracefile;
      };

      /**
       * StagerJob plugin for the Rfio protocol
       */
      class RfioPlugin :
        public castor::job::stagerjob::InstrumentedMoverPlugin {

      public:

        /**
         * Default constructor
         */
        RfioPlugin() throw();

        /**
         * Returns the port range for rfio
         */
        virtual std::pair<int, int> getPortRange
        (castor::job::stagerjob::InputArguments &args) throw();

        /**
         * Hook for the code to be executed just after the mover fork,
         * in the parent process. Only logging and calling the method
         * of InstrumentedPlugin.
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
         * Get the log level and the log file name
         */
        static void getLogLevel(std::string &logFile, bool &debug) throw();

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
                                   EnvironmentRfio &env) throw();

        /**
         * Set the environment variables into the real environment
         * not the structure called environment
         * @param env the environment
         */
        static void setEnvironment(EnvironmentRfio env) throw();

      }; // end of class RfioPlugin

    } // end of namespace stagerjob

  } // end of namespace job

} // end of namespace castor

#endif // STAGERJOB_RFIOPLUGIN_HPP
