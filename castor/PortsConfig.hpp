/******************************************************************************
*                      PortsConfig.hpp
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
 * @(#)$RCSfile: PortsConfig.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/11/29 14:43:31 $ $Author: itglp $
 *
 * A central place to provide notification hosts and ports of the Castor daemons
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_PORTSCONFIG_HPP
#define CASTOR_PORTSCONFIG_HPP 1

// Include files
#include <string>
#include <map>
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {
  
  static const int STAGER_DEFAULT_NOTIFYPORT = 55015;
  static const int JOBMANAGER_DEFAULT_NOTIFYPORT = 15011;
  static const int RTCPCLIENTD_DEFAULT_NOTIFYPORT = 5050;  // for future use
  
  enum castorDaemon {
    CASTOR_STAGER = 0,
    CASTOR_JOBMANAGER = 1,
    CASTOR_RTCPCLIENTD = 2
  };

  /**
   * PortsConfig singleton class
   */
  class PortsConfig : public castor::BaseObject {

  public:
  
    /**
     * Static method to get this singleton's instance
     */
    static castor::PortsConfig* getInstance() throw (castor::exception::Exception);

    /**
     * Get the hostname for a given daemon
     * @return the value of m_confFile
     */
    std::string getHostName(castorDaemon daemon) const {
      return m_hosts[daemon];
    }
    
    unsigned getNotifPort(castorDaemon daemon) const {
      return m_ports[daemon];
    }

  private:
  
    /// this singleton's instance
    static castor::PortsConfig* s_instance;
    
    /**
     * Default constructor, reads data from castor.conf
     * and initializes internal structures
     * @throw InvalidArgument exception when fails to read
     * the needed configuration from castor.conf
     */
    PortsConfig() throw (castor::exception::Exception);
    
    /**
     * virtual destructor
     */
    virtual ~PortsConfig() throw() {};

    /// retrieves the notification port for a given label
    int getConfigPort(const char* configLabel, unsigned defaultValue)
      throw (castor::exception::Exception);

    /// retrieves the notification host for a given label
    std::string getConfigHost(const char* configLabel)
      throw (castor::exception::Exception);

    /// the host names read from castor.conf
    std::vector<std::string> m_hosts;
    
    /// the notification ports read from castor.conf
    std::vector<unsigned> m_ports;

  };
  
} // end of namespace castor

#endif // CASTOR_PORTSCONFIG_HPP
