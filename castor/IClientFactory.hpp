/******************************************************************************
 *                      ClientFactory.hpp
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
 * @(#)$RCSfile: IClientFactory.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/12/16 11:01:15 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RH_CLIENTFACTORY_HPP 
#define RH_CLIENTFACTORY_HPP 1

// Include Files
#include "castor/exception/Exception.hpp"
#include <string>

namespace castor {

  // Forward declarations
  class IClient;  

  /**
   * class IClientFactory
   * A factory for IClients allowing to convert them
   * to string and back
   */
  class IClientFactory {

  public:

    /**
     * converts a client to a human readable string
     * @param cl the Client to convert
     * @result the resulting string
     * @exception in case of error
     */
    static const std::string client2String
    (const castor::IClient &cl)
      throw (castor::exception::Exception);
    
    /**
     * creates a Client from its human readable
     * string representation. Note that the caller is
     * responsible for the deallocation of the returned
     * Client.
     * @param st the human readable string.
     * @result the newly allocated client
     * @exception in case of error
     */
    static castor::IClient* string2Client
    (const std::string &st)
      throw (castor::exception::Exception);

    }; // end of class IClientFactory

}; // end of namespace castor

#endif // RH_CLIENTFACTORY_HPP
