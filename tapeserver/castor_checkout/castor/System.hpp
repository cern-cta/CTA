/******************************************************************************
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
 * A class with static methods for system level utilities.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

// Include Files
#include <string>
#include "castor/exception/Exception.hpp"

namespace castor {

  class System {

  public:

    /**
     * Gets the host name. Handles all errors that may occur with 
     * the gethostname() API.  
     * @exception in case of an error 
     */
    static std::string getHostName() ;

    /**
     * Converts a string into a port number, checking
     * that the value is in range [0-65535]
     * @param str the string giving the port number
     * @return the port as an int
     * @exception in case of invalid value
     */
    static int porttoi(char* str) ;
    
    /**
     * Converts an ip address to a hostname
     * @param ipAddress the ip address to be converted.
     * @exception in case of an error
     */
    static std::string ipAddressToHostname
    (unsigned long long ipAddress)
      ;

    /**
     * Switches the current process to use the Castor superuser
     * (typically stage:st). 
     * @exception in case of an error
     */
    static void switchToCastorSuperuser() ;
     
  };

} // end of namespace castor

