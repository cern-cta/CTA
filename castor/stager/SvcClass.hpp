/******************************************************************************
 *                      castor/stager/SvcClass.hpp
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

#ifndef CASTOR_STAGER_SVCCLASS_HPP
#define CASTOR_STAGER_SVCCLASS_HPP

// Include Files
#include "castor/IObject.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    /**
     * class SvcClass
     * A service, as seen by the user. A SvcClass is a container of resources and may be
     * given as parameter of the request.
     */
    class SvcClass : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      SvcClass() throw();

      /**
       * Empty Destructor
       */
      virtual ~SvcClass() throw();

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
       * Get the value of m_policy
       * @return the value of m_policy
       */
      std::string policy() const {
        return m_policy;
      }

      /**
       * Set the value of m_policy
       * @param new_var the new value of m_policy
       */
      void setPolicy(std::string new_var) {
        m_policy = new_var;
      }

      /**
       * Get the value of m_nbDrives
       * Number of drives to use for this service class. This is the default number, but
       * it could be that occasionnally more drives are used, if a resource is shared with
       * another service class using more drives
       * @return the value of m_nbDrives
       */
      unsigned int nbDrives() const {
        return m_nbDrives;
      }

      /**
       * Set the value of m_nbDrives
       * Number of drives to use for this service class. This is the default number, but
       * it could be that occasionnally more drives are used, if a resource is shared with
       * another service class using more drives
       * @param new_var the new value of m_nbDrives
       */
      void setNbDrives(unsigned int new_var) {
        m_nbDrives = new_var;
      }

      /**
       * Get the value of m_name
       * the name of this SvcClass
       * @return the value of m_name
       */
      std::string name() const {
        return m_name;
      }

      /**
       * Set the value of m_name
       * the name of this SvcClass
       * @param new_var the new value of m_name
       */
      void setName(std::string new_var) {
        m_name = new_var;
      }

    private:

    private:

      std::string m_policy;

      /*
       * Number of drives to use for this service class.
       * This is the default number, but it could be that occasionnally more drives are used, if a resource is shared with another service class using more drives
      */
      unsigned int m_nbDrives;

      /// the name of this SvcClass
      std::string m_name;

      /// The id of this object
      u_signed64 m_id;

    }; // end of class SvcClass

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_SVCCLASS_HPP
