/******************************************************************************
 *                      castor/stager/QueryParameter.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_QUERYPARAMETER_HPP
#define CASTOR_STAGER_QUERYPARAMETER_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/RequestQueryType.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    /**
     * class QueryParameter
     * A parameter of a query request. Depending on its type, its value can contain a
     * fileName, a requestId, a fileId, ...
     */
    class QueryParameter : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      QueryParameter() throw();

      /**
       * Empty Destructor
       */
      virtual ~QueryParameter() throw();

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
       * Gets the type of the object
       */
      virtual int type() const;

      /**
       * virtual method to clone any object
       */
      virtual castor::IObject* clone();

      /*********************************/
      /* End of IObject abstract class */
      /*********************************/
      /**
       * Get the value of m_value
       * Value of this parameter. The meaning depends on the type.
       * @return the value of m_value
       */
      std::string value() const {
        return m_value;
      }

      /**
       * Set the value of m_value
       * Value of this parameter. The meaning depends on the type.
       * @param new_var the new value of m_value
       */
      void setValue(std::string new_var) {
        m_value = new_var;
      }

      /**
       * Get the value of m_id
       * The id of this object
       * @return the value of m_id
       */
      u_signed64 id() const {
        return m_id;
      }

      /**
       * Set the value of m_id
       * The id of this object
       * @param new_var the new value of m_id
       */
      void setId(u_signed64 new_var) {
        m_id = new_var;
      }

      /**
       * Get the value of m_queryType
       * @return the value of m_queryType
       */
      RequestQueryType queryType() const {
        return m_queryType;
      }

      /**
       * Set the value of m_queryType
       * @param new_var the new value of m_queryType
       */
      void setQueryType(RequestQueryType new_var) {
        m_queryType = new_var;
      }

    private:

      /// Value of this parameter. The meaning depends on the type.
      std::string m_value;

      /// The id of this object
      u_signed64 m_id;

      RequestQueryType m_queryType;

    }; // end of class QueryParameter

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_QUERYPARAMETER_HPP
