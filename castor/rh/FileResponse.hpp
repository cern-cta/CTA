/******************************************************************************
 *                      castor/rh/FileResponse.hpp
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

#ifndef CASTOR_RH_FILERESPONSE_HPP
#define CASTOR_RH_FILERESPONSE_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace rh {

    /**
     * class FileResponse
     * Response dealing with a unique file and given information on how to access it
     */
    class FileResponse : public virtual Response, public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      FileResponse() throw();

      /**
       * Empty Destructor
       */
      virtual ~FileResponse() throw();

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
      virtual void setId(u_signed64 id);

      /**
       * gets the id of the object
       */
      virtual u_signed64 id() const;

      /**
       * Gets the type of the object
       */
      virtual int type() const;

      /*********************************/
      /* End of IObject abstract class */
      /*********************************/
      /**
       * Get the value of m_status
       * The current status of the file
       * @return the value of m_status
       */
      int status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * The current status of the file
       * @param new_var the new value of m_status
       */
      void setStatus(int new_var) {
        m_status = new_var;
      }

      /**
       * Get the value of m_reqid
       * The request id
       * @return the value of m_reqid
       */
      std::string reqid() const {
        return m_reqid;
      }

      /**
       * Set the value of m_reqid
       * The request id
       * @param new_var the new value of m_reqid
       */
      void setReqid(std::string new_var) {
        m_reqid = new_var;
      }

      /**
       * Get the value of m_server
       * The server where to find the file
       * @return the value of m_server
       */
      std::string server() const {
        return m_server;
      }

      /**
       * Set the value of m_server
       * The server where to find the file
       * @param new_var the new value of m_server
       */
      void setServer(std::string new_var) {
        m_server = new_var;
      }

      /**
       * Get the value of m_port
       * The port where to find the file
       * @return the value of m_port
       */
      int port() const {
        return m_port;
      }

      /**
       * Set the value of m_port
       * The port where to find the file
       * @param new_var the new value of m_port
       */
      void setPort(int new_var) {
        m_port = new_var;
      }

      /**
       * Get the value of m_protocol
       * The protocol to use to retrieve the file
       * @return the value of m_protocol
       */
      std::string protocol() const {
        return m_protocol;
      }

      /**
       * Set the value of m_protocol
       * The protocol to use to retrieve the file
       * @param new_var the new value of m_protocol
       */
      void setProtocol(std::string new_var) {
        m_protocol = new_var;
      }

    private:

    private:

      /// The current status of the file
      int m_status;

      /// The request id
      std::string m_reqid;

      /// The server where to find the file
      std::string m_server;

      /// The port where to find the file
      int m_port;

      /// The protocol to use to retrieve the file
      std::string m_protocol;

      /// The id of this object
      u_signed64 m_id;

    }; // end of class FileResponse

  }; // end of namespace rh

}; // end of namespace castor

#endif // CASTOR_RH_FILERESPONSE_HPP
