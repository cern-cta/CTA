/******************************************************************************
 *                      castor/stager/ReqId.hpp
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
 * @(#)$RCSfile: ReqId.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/10/05 13:37:33 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_REQID_HPP
#define CASTOR_STAGER_REQID_HPP

// Include Files
#include "castor/IObject.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class ReqIdRequest;

    /**
     * class ReqId
     * A Reqid, as defined in the new castor
     */
    class ReqId : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      ReqId() throw();

      /**
       * Empty Destructor
       */
      virtual ~ReqId() throw();

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
       * Get the value of m_value
       * The reqid itself
       * @return the value of m_value
       */
      std::string value() const {
        return m_value;
      }

      /**
       * Set the value of m_value
       * The reqid itself
       * @param new_var the new value of m_value
       */
      void setValue(std::string new_var) {
        m_value = new_var;
      }

      /**
       * Get the value of m_request
       * @return the value of m_request
       */
      ReqIdRequest* request() const {
        return m_request;
      }

      /**
       * Set the value of m_request
       * @param new_var the new value of m_request
       */
      void setRequest(ReqIdRequest* new_var) {
        m_request = new_var;
      }

    private:

    private:

      /// The reqid itself
      std::string m_value;

      /// The id of this object
      u_signed64 m_id;

      ReqIdRequest* m_request;

    }; // end of class ReqId

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_REQID_HPP
