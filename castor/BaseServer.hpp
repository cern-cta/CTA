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
 * @(#)$RCSfile: BaseServer.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2005/04/05 14:25:43 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Ben Couturier
 *****************************************************************************/

#ifndef CASTOR_BASESERVER_HPP 
#define CASTOR_BASESERVER_HPP 1

#include <iostream>
#include <string>
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"

namespace castor {

  /**
   * Static method used to pass to Cpool_assign
   */
  void *staticProcessRequest(void *param);

  /**
   * Basic CASTOR multithreaded server.
   */
  class BaseServer : public BaseObject {
    
  public:
    
    /**
     * constructor
     */
    BaseServer(const std::string serverName, 
               const int nbThreads = DEFAULT_THREAD_NUMBER);

    /*
     * destructor
     */
    virtual ~BaseServer() throw();

    /**
     * User main function.
     */
    virtual int main() =0;


    /**
     * Method to process a request
     */
    virtual void *processRequest(void *param) =0;

    /**
     * Starts the server, as a daemon and execs the 
     * server main function.
     */
    virtual int start();

    /**
     * default number of threads in the server thread pool
     */    
    static const int DEFAULT_THREAD_NUMBER = 20;

    /**
     * Assigns work to a thread from the pool
     */    
    int threadAssign(void *param);

    /**
     * Sets the foreground flag
     */    
    void setForeground(bool value);

    /**
     * Sets the foreground flag
     */    
    void setSingleThreaded(bool value);

    /**
     * parses a command line to set the server oprions
     */    
    void parseCommandLine(int argc, char *argv[]);

    /**
     * Gets a pointer in thread local storage
     * Returns 0 if the void * could be allocated
     * -1 otherwise.
     */    
    static int gettls(void **thip);

    /**
     * gets the message service log stream
     * Note that the service has to be released after usage
     * @return a pointer to the message service or 0 if none
     * is available.
     */
    std::ostream& log() throw (castor::exception::Exception);

    /**
     * Output stream to logfile
     */    
    //std::ostream *m_log;
    
  private:

    /**
     * BaseServer main method called by start
     */
    int serverMain();


    /**
     * Flag indicating whether the server should 
     * run in foreground or background mode.
     */
    bool m_foreground;

    /**
     * Flag indicating whether the server should run in single threaded
     * or monothreaded mode
     */
    bool m_singleThreaded;

    /**
     * The id of the pool created
     */
    int m_threadPoolId;

    /**
     * Number of threads in the pool
     */
    int m_threadNumber;

    /**
     * Name of the server
     */
    std::string m_serverName;

  };
  
} // end of namespace castor

/**
 * Structure used to pass arguments to the method through Cpool_assign
 */
struct processRequestArgs {
  castor::BaseServer *handler;
  void *param;
};


#endif // CASTOR_BASESERVER_HPP
