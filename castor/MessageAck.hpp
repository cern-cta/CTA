/******************************************************************************
 *                      castor/MessageAck.hpp
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

#ifndef CASTOR_MESSAGEACK_HPP
#define CASTOR_MESSAGEACK_HPP

// Include Files
#include "castor/IObject.hpp"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  /**
   * class MessageAck
   * Acknowledgement message in the request handler protocol.
   */
  class MessageAck : public virtual IObject {

  public:

    /**
     * Empty Constructor
     */
    MessageAck() throw();

    /**
     * Empty Destructor
     */
    virtual ~MessageAck() throw();

    /**
     * Outputs this object in a human readable format
     * @param stream The stream where to print this object
     * @param indent The indentation to use
     * @param alreadyPrinted The set of objects already printed.
     * This is to avoid looping when printing circular dependencies
     */
    virtual void print(std::ostream& stream,
                       std::string indent,
                       castor::ObjectSet& alreadyPrinted) const;

    /**
     * Outputs this object in a human readable format
     */
    virtual void print() const;

    /**
     * Gets the type of this kind of objects
     */
    static int TYPE();

    /********************************************/
    /* Implementation of IObject abstract class */
    /********************************************/
    /**
     * Sets the id of the object
     * @param id The new id
     */
    virtual void setId(unsigned long id);

    /**
     * gets the id of the object
     */
    virtual unsigned long id() const;

    /**
     * Gets the type of the object
     */
    virtual int type() const;

    /*********************************/
    /* End of IObject abstract class */
    /*********************************/
    /**
     * Get the value of m_status
     * @return the value of m_status
     */
    bool status() const {
      return m_status;
    }

    /**
     * Set the value of m_status
     * @param new_var the new value of m_status
     */
    void setStatus(bool new_var) {
      m_status = new_var;
    }

    /**
     * Get the value of m_errorCode
     * @return the value of m_errorCode
     */
    int errorCode() const {
      return m_errorCode;
    }

    /**
     * Set the value of m_errorCode
     * @param new_var the new value of m_errorCode
     */
    void setErrorCode(int new_var) {
      m_errorCode = new_var;
    }

    /**
     * Get the value of m_errorMessage
     * @return the value of m_errorMessage
     */
    std::string errorMessage() const {
      return m_errorMessage;
    }

    /**
     * Set the value of m_errorMessage
     * @param new_var the new value of m_errorMessage
     */
    void setErrorMessage(std::string new_var) {
      m_errorMessage = new_var;
    }

  private:

  private:

    bool m_status;

    int m_errorCode;

    std::string m_errorMessage;

    /// The id of this object
    unsigned long m_id;

  }; // end of class MessageAck

}; // end of namespace castor

#endif // CASTOR_MESSAGEACK_HPP
