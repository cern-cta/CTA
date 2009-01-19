
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
 * @author Giulia Taurelli
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

      // The port to accept connections (environment)     
      int m_listenPort;

     
    public:

      /**
       * constructor
       */
      TapeGatewayDaemon();
      

      /**
       * destructor
       */
      virtual ~TapeGatewayDaemon() throw() {};
      void usage();
      void parseCommandLine(int argc, char* argv[]);

      /** Retrieves Port from castor.conf (if given) */
      inline int listenPort(){ return m_listenPort; }
         
    };

    } //end of namespace tapegateway
  } // end of namespace tape
} // end of namespace castor

#endif // TAPEGATEWAY_DAEMON_HPP
