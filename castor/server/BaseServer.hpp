/******************************************************************************
 *                      BaseServer.hpp
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
 * @(#)$RCSfile: BaseServer.hpp,v $ $Revision: 1.16 $ $Release$ $Date: 2009/08/18 09:42:54 $ $Author: waldron $
 *
 * A base multithreaded server for simple listening servers
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_SERVER_BASESERVER_HPP
#define CASTOR_SERVER_BASESERVER_HPP 1

#include <iostream>
#include <string>
#include <map>
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/dlf/Message.hpp"

namespace castor {

  namespace server {

    /**
     * Basic CASTOR multithreaded server; cf. also BaseDaemon.
     * Credits to Ben Couturier.
     */
    class BaseServer : public castor::BaseObject {

    public:

      /**
       * Constructor
       * @param serverName a friendly name for this daemon,
       * used to initialize the logging facility.
       */
      BaseServer(const std::string serverName);

      /*
       * Destructor
       */
      virtual ~BaseServer() throw();


      /**
       * Starts all the thread pools
       */
      virtual void start() throw (castor::exception::Exception);

      /**
       * Parses a command line to set the server options
       */
      virtual void parseCommandLine(int argc, char *argv[]);

      /**
       * Sets the foreground flag
       */
      void setForeground(bool value) { m_foreground = value; }

      /**
       * Sets the runAsStagerSuperuser flag
       */
      void runAsStagerSuperuser() { m_runAsStagerSuperuser = true; }

      /**
       * Initializes the DLF, both for streaming and regular messages
       * Does not create the DLF thread, this is created after daemonization
       * @param messages the messages to be passed to dlf_init
       */
      void dlfInit(castor::dlf::Message messages[])
        throw (castor::exception::Exception);

      /**
       * Adds a thread pool to this server
       */
      void addThreadPool(BaseThreadPool* tpool) throw();

      /// Gets a pool by its name initial
      BaseThreadPool* getThreadPool(const char nameIn) throw() {
        return m_threadPools[nameIn];
      };
      
      /// Returns this server's name
      std::string& getServerName() {
        return m_serverName;
      }

      /**
       * Sends a notification message to the given host,port
       * to wake up nbThreads threads to handle pending requests.
       * @param host the destination host
       * @param port the destination port
       * @param tpName the name of the thread pool to be signaled
       * @param nbThreads the number of threads to be signaled
       */
      static void sendNotification(std::string host, int port, char tpName, int nbThreads = 1)
        throw();

    protected:

      /**
       * Initializes the server as daemon
       */
      virtual void init() throw (castor::exception::Exception);

      /**
       * Prints out the online help
       */
      virtual void help(std::string programName);

      /**
       * Flag indicating whether the server should
       * run in foreground or background mode.
       */
      bool m_foreground;

      /**
       * Flag indicating whether the server should
       * change identity at startup and run as STAGERSUPERUSER
       * (normally defined as stage:st)
       */
      bool m_runAsStagerSuperuser;

      /**
       * Name of the server, for logging purposes.
       */
      std::string m_serverName;

      /**
       * Command line parameters. Includes by default a parameter
       * per each thread pool to specify the number of threads.
       */
      std::stringstream m_cmdLineParams;

      /**
       * List of thread pools running on this server,
       * identified by their name initials (= cmd line parameter).
       */
      std::map<const char, BaseThreadPool*> m_threadPools;

    };

  } // end of namespace server

} // end of namespace castor


#endif // CASTOR_SERVER_BASESERVER_HPP
