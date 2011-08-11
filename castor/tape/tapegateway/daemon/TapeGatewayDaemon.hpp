
/******************************************************************************
 *                      TapeGatewayDaemon.hpp
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
 * @(#)$RCSfile: TapeGatewayDaemon.hpp,v $ $ $Author: gtaur $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/


#ifndef TAPEGATEWAY_DAEMON_HPP
#define TAPEGATEWAY_DAEMON_HPP 1

// Include Files

#include "castor/server/BaseDaemon.hpp"


namespace castor {
  namespace tape{
    namespace tapegateway{

    /**
     * TapeGateway daemon.
     */
    class TapeGatewayDaemon : public castor::server::BaseDaemon{
      
      /**
       * port to accept connections (environment)     
       */

      int m_listenPort;
      
      /**
       * DLF message strings.
       */
      
      static castor::dlf::Message s_dlfMessages[];
     
    
      /**
       * Exception throwing main() function which basically implements the
       * non-exception throwing main() function except for the initialisation of
       * DLF and the "exception catch and log" logic.
       */


      int exceptionThrowingMain(int argc,char **argv) 
	throw(castor::exception::Exception);


      /**
       * Logs the start of the daemon.
       */
      void logStart(const int argc, const char *const *const argv) throw();

      
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
  
    public:

      /**
       * constructor
       */

      TapeGatewayDaemon();
      

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
       * destructor
       */
      virtual ~TapeGatewayDaemon() throw() {};


      /** to access the value of the port which is used  */
      inline int listenPort(){ return m_listenPort; }
         
    };

    } //end of namespace tapegateway
  } // end of namespace tape
} // end of namespace castor

#endif // TAPEGATEWAY_DAEMON_HPP
