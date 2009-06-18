/******************************************************************************
 *                      RepackServer.hpp
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
 * @(#)$RCSfile: RepackServer.hpp,v $ $Revision: 1.17 $ $Release$ $Date: 2009/06/18 15:30:29 $ $Author: gtaur $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef REPACKSERVER_HPP
#define REPACKSERVER_HPP 1

#include "castor/server/BaseDaemon.hpp"
#include "castor/repack/IRepackSvc.hpp"


namespace castor {

 namespace repack {
  
  
  /**
   * CASTOR Repack main daemon.
   */

  class RepackServer : public castor::server::BaseDaemon {

  public:

    /**
     * constructor
     */
    RepackServer();

    /**
     * destructor
     */
    virtual ~RepackServer() throw();
  
    /** Retrieves the Nameserver name */
    std::string getNsName() const {
      return (m_ns);
    }
    /** Retrieves the Stager name */
    std::string getStagerName() const {
      return (m_stager);
    }
    /** Retrieves the service class for repack process */
    std::string getServiceClass() const {
      return (m_serviceClass);
    }
    /** Retrieves the protocol for file transfer */
    std::string getProtocol() const {
      return (m_protocol);
    }

   /** Retrieves Port from env (if given) */
    int getListenPort() const {
      return m_listenPort;
    }

    /** Retrieves Port from env (if given) */
    int getPollTime() const {
      return m_pollingTime;
    }

    /** Retrieves max number of files **/
    int maxFiles() const {
      return m_maxFiles;
    }
    
    /** Retrieves max number of tapes ONGOING **/
    int maxTapes() const {
      return m_maxTapes;
    }
    
    /** Overloaded method from BaseServer for individual command line parser */
    virtual void parseCommandLine(int argc, char *argv[]);
 
    /** Overloaded method from BaseServer for help */
    virtual void help(std::string programName);


  private:
    
    // The Nameserver (this name is queries by RepackFileStager and RepackCleaner (config file)
      
    std::string  m_ns;
    
    // The Request Handler,RH to contact (config file)
      
    std::string m_stager;
     
    //The default service class to use (config file)
      
    std::string m_serviceClass;
    
    // The default transfer protocol to use (config file)
     
    std::string m_protocol;

    // The port to accept connections (environment)
     
    int m_listenPort;

    //The repack monitor DB polling interval (environment) 
 
    int m_pollingTime;
    
    // parameters to limitate the concurrent repack processes

    int m_maxFiles;
    
    int m_maxTapes;

  };

 } // end of namespace repack

} // end of namespace castor



#endif // REPACKSERVER_HPP
