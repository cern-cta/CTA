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

#ifndef RH_VDQMSERVER_HPP
#define RH_VDQMSERVER_HPP 1

#include "castor/BaseObject.hpp"

namespace castor {

  namespace vdqm {
  	
	  /**
	   * Static method used to pass to Cpool_assign
	   */
	  void *staticProcessRequest(void *param);
  	
		//Forward Declarations
  	class VdqmServerSocket;

    /**
     * The Request Handler server. This is were client requests
     * arrive. The main task of this component is to store them
     * for future use
     */
    class VdqmServer : public castor::BaseObject {

    public:

      /**
       * Constructor
       */
      VdqmServer();
      
      /**
       * Destructor
       */
      ~VdqmServer() throw();

      /**
       * Method called once per request, where all the code resides
       */
      virtual void *processRequest(void *param) throw();

      /**
       * Main Server loop, listening for the clients
       */
      virtual int main();

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

    private:
    
	    /**
	     * BaseServer main method called by start
	     */
	    int serverMain();
	    
	    /**
	     * Internal function to handle the different protocol versions.
	     * 
	     * @param sock The used socket connection
	     * @param cuuid The log id of the request
	     * @exception Throws an exception in case of errors
	     */
	    void handleProtocolVersion(VdqmServerSocket* sock, Cuuid_t cuuid)
     		throw (castor::exception::Exception);	    
	    
      /**
       * Internal function to handle the old Vdqm request. Puts the values into
       * the old structs and reads out the request typr number, to delegates
       * them to the right function.
       * 
       * @param sock The used socket connection
       * @exception Throws an exception in case of errors
       */
      void handleOldVdqmRequest(castor::vdqm::VdqmServerSocket* sock, 
      													unsigned int magicNumber,
      													Cuuid_t cuuid)
      	throw (castor::exception::Exception);	    
	
	    /**
	     * Flag indicating whether the server should 
	     * run in foreground or background mode.
	     */
	    bool m_foreground;
	
	    /**
	     * The id of the pool created
	     */
	    int m_threadPoolId;
	
	    /**
	     * Number of threads in the pool
	     */
	    int m_threadNumber;
	    
	    /**
	     * Number of threads in the tape to tape drive dedication pool
	     */
	    int m_dedicationThreadNumber;
	
	    /**
	     * Name of the server
	     */
	    std::string m_serverName;    

    }; // class VdqmServer

  } // end of namespace vdqm

} // end of namespace castor


/**
 * Structure used to pass arguments to the method through Cpool_assign
 */
struct processRequestArgs {
  castor::vdqm::VdqmServer *handler;
  void *param;
};

#endif // RH_VDQMSERVER_HPP
