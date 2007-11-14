
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

namespace castor {

  namespace rtcopy{
    namespace rechandler{

    /**
     * Castor RecHandler daemon.
     */
    class RecHandlerDaemon : public castor::server::BaseDaemon{
      u_signed64 m_timeSleep;
      
    public:
    
      /**
       * constructor
       */
      RecHandlerDaemon();
       
      inline u_signed64 timeSleep(){ return m_timeSleep;}
      inline void setTimeSleep(u_signed64 time){m_timeSleep=time;}

      /**
       * destructor
       */
      virtual ~RecHandlerDaemon() throw() {};
      void usage();
      void parseCommandLine(int argc, char* argv[]);

    };

    } // end of namespace rechandler

  } // end of namespace rtcopy

} // end of namespace castor

#endif // RECHANDLER_DAEMON_HPP
