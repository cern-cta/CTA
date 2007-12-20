/******************************************************************************
 *                      VdqmServer.hpp
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
 * @(#)RCSfile: VdqmServer.hpp  Revision: 1.0  Release Date: Apr 8, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef CASTOR_VDQM_VDQMSERVER_HPP
#define CASTOR_VDQM_VDQMSERVER_HPP 1

#include "castor/server/BaseDaemon.hpp"
#include <string>

namespace castor {

  namespace vdqm {
  	
    /**
     * The Volume and Drive Queue Manager.
     */
    class VdqmServer : public castor::server::BaseDaemon {
    public:
    
      /**
       * Default number of threads in the server thread pool
       */    
      static const int DEFAULT_THREAD_NUMBER = 20;

      /**
       * Constructor
       */
      VdqmServer() throw();

      /**
       * Parses the command line and sets the server options accordingly
       */    
      void parseCommandLine(int argc, char *argv[]) throw();

      /**
       * Returns the port on which the server will listen.
       */
      int getListenPort();

      /**
       * Returns the number of threads in the request handler thread pool.
       */
      int getRqstHandlerThreadNb();

      /**
       * Returns the number of threads in the drive dedication thread pool.
       */
      int getDedicationThreadNb();
      

    private:

      /**
       * Number of request handler threads
       */
      int m_rqstHandlerThreadNumber;

      /**
       * Number of drive dedication threads
       */
      int m_dedicationThreadNumber;
    
      /**
       * Initializes the DLF logging including the definition of the predefined
       * messages.
       */
      void initDlf() throw();

      /**
       * Prints out the online help
       */
      void help(std::string programName) throw();
    
    }; // class VdqmServer

  } // namespace vdqm

} // namespace castor


#endif // CASTOR_VDQM_VDQMSERVER_HPP
