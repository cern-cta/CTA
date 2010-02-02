
/******************************************************************************
 *                      RecHandlerDaemon.hpp
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
 * @(#)$RCSfile: RecHandlerDaemon.hpp,v $ $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#ifndef RECHANDLER_DAEMON_HPP
#define RECHANDLER_DAEMON_HPP 1

// Include Files


#include "castor/server/BaseDaemon.hpp"

namespace castor     {
namespace tape       {
namespace rechandler {

    /**
     * Castor RecHandler daemon.
     */
    class RecHandlerDaemon : public castor::server::BaseDaemon{

      /**
       * DLF message strings.
       */
      static castor::dlf::Message s_dlfMessages[];

      /**
       * The time in seconds between two recall-policy database lookups.
       */
      u_signed64 m_sleepTime;

      /**
       * Logs the start of the daemon.
       */
      void logStart(const int argc, const char *const *const argv) throw();

      /**
       * Exception throwing main() function which basically implements the
       * non-exception throwing main() function except for the initialisation of
       * DLF and the "exception catch and log" logic.
       */
      int exceptionThrowingMain(const int argc, char **argv)
	throw(castor::exception::Exception);


    public:
    
      /**
       * constructor
       */
      RecHandlerDaemon() throw ();


      
      /**
       * Destructor
       */
      virtual ~RecHandlerDaemon() throw(){};

      /**
       * The main entry function of the mighunter daemon.
       *
       * Please not that this method must be called by the main thread of the
       * application.
       *
       * @param argc Argument count from the executable's entry function: main().
       * @param argv Argument vector from the executable's entry function: main().
       */
      int main(const int argc, char **argv);
      
      /**
       * Writes the command-line usage message of the daemon to standard out.
       */
      void usage();
      
      /**
       * Parses the command-line arguments.
       *
       * @param argc Argument count from the executable's entry function: main().
       * @param argv Argument vector from the executable's entry function: main().
       */
      void parseCommandLine(int argc, char* argv[]);


    };

} // end of namespace rechandler
} // end of namespace tape
} // end of namespace castor

#endif // RECHANDLER_DAEMON_HPP
