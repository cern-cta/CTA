/******************************************************************************
 *                      Server.hpp
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
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef RH_SERVER_HPP
#define RH_SERVER_HPP 1

#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Exception.hpp"

#define CSP_MSG_MAGIC 0xCA001
#define CSP_RHSERVER_PORT 9002
#define CSP_RHSERVER_SEC_PORT 9007
#define CSP_NOTIFICATION_PORT 9001

namespace castor {

  namespace rh {

    /**
     * Constants
     */
    extern const char* PORT_ENV;
    extern const char* PORT_SEC_ENV;
    extern const char* CATEGORY_CONF;
    extern const char* PORT_CONF;
    extern const char* PORT_SEC_CONF;
    
    /**
     * The Request Handler server. This is where client requests
     * arrive. The main task of this component is to store them
     * for future processing, after checking authc/authz.
     */
    class Server : public castor::server::BaseDaemon {

    public:

      /**
       * Constructor
       */
      Server();

      /**
       * Destructor
       */
      virtual ~Server() throw();

      /**
       * Overloaded method from BaseDaemon for individual command line parser
       */
      virtual void parseCommandLine(int argc, char *argv[])
        throw(castor::exception::Exception);

      /// The listening port
      int m_port;

      /// Flag to indicate whether strong authentication is enabled or not
      bool m_secure;

      /**
       * Flag to indicate whether to wait for dispatching a request when
       * all threads are busy as opposed to discarding it
       */
      bool m_waitIfBusy;
      
    protected:

      /**
       * Prints out the online help
       */
      virtual void help(std::string programName);

    private:

      /**
       * the handle returned by dlopen when opening libCsec_plugin_KRB5.
       * for working around the bug related with the KRB5 and GSI mixed libraries
       */
      void *m_dlopenHandle;

    }; // class Server

  } // end of namespace rh

} // end of namespace castor

#endif // RH_SERVER_HPP

