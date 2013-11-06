// ----------------------------------------------------------------------
// File: Daemon/tapeserverd.hh
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "../Exception/Exception.hh"

namespace Tape {
  namespace Server {
    class Daemon {
    public:
      Daemon (int argc, char ** argv) throw (Tape::Exception);
      class options {
      public:
        options(): daemonize(true), runDirectory("/var/log/castor") {}
        bool daemonize;
        std::string runDirectory;
      };
    private:
      options getCommandLineOptions(int argc, char ** argv) throw (Tape::Exception);
      void daemonize();
      options m_options;
    };
  }
}       
