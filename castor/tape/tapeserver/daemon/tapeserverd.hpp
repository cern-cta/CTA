/******************************************************************************
 *                      tapeserverd.hpp
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once
#include "../exception/Exception.hpp"
#include "castor/server/Daemon.hpp"

namespace castor {
namespace tape {
  namespace Server {
    class Daemon: public castor::server::Daemon {
    public:
      /** Wrapper for main */
      static int main (int argc, char ** argv) throw ();
      /** Constructor: parse the command line */
      Daemon (int argc, char ** argv, castor::log::Logger & logger) 
              throw (castor::tape::Exception);
      /** Run the daemon: daemonize if needed, and run the main loop. */
      void run();
      ~Daemon() throw () {}
    private:
      void parseCommandLineOptions(int argc, char ** argv) throw (castor::tape::Exception);
      /** This daemonize() version has to be merged into castor::server::Daemon::daemonize
       * which has a few things to check (error cases for fork are not handled correctly,
       * error cases for setsid are not looked at, there is a forced switch to stager
       * user, while taped (and children) and rtcpd (but not its children) run 
       * as root. There is some calls to signal in daemonize for just 2 signals */
      void daemonize();
      /** We block all signals, so that they can be handled synchronously with 
       * a sigtimedwait in the main loop (alternately with a poll using a
       * non-zero timeout. */
      void blockSignals();
      std::string m_option_run_directory;
    };
  }
}
}  
