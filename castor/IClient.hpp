/******************************************************************************
 *                      castor/IClient.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_ICLIENT_HPP
#define CASTOR_ICLIENT_HPP

// Include Files
#include "castor/IObject.hpp"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class Request;

  }; // end of namespace stager

  /**
   * class IClient
   * A generic abstract base class for castor clients
   */
  class IClient : public virtual IObject {

  public:

    /**
     * Empty Destructor
     */
    virtual ~IClient() throw() {};

    /**
     * Reconnects to the client in case the connection was closed or lost
     */
    virtual void reconnect() = 0;

    /**
     * Disconnects from the client.
     */
    virtual void disconnect() = 0;

    /**
     */
    virtual void sendRep() = 0;

    /**
     * Outputs this object in a human readable format
     * @param stream The stream where to print this object
     * @param indent The indentation to use
     * @param alreadyPrinted The set of objects already printed.
     * This is to avoid looping when printing circular dependencies
     */
    virtual void print(std::ostream& stream,
                       std::string indent,
                       castor::ObjectSet& alreadyPrinted) const = 0;

    /**
     * Outputs this object in a human readable format
     */
    virtual void print() const = 0;

    /**
     * Get the value of m_request
     * @return the value of m_request
     */
    castor::stager::Request* request() const {
      return m_request;
    }

    /**
     * Set the value of m_request
     * @param new_var the new value of m_request
     */
    void setRequest(castor::stager::Request* new_var) {
      m_request = new_var;
    }

  private:

    castor::stager::Request* m_request;

  }; // end of class IClient

}; // end of namespace castor

#endif // CASTOR_ICLIENT_HPP
