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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_SVCCLASS_HPP
#define CASTOR_STAGER_SVCCLASS_HPP

// Include Files
#include "castor/IObject.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class TapePool;
    class DiskPool;

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

      /**
       * Get the value of m_defaultFileSize
       * Default size used for space allocation in the case of a stage put with no size
       * explicitely given (ie size given was 0)
       * @return the value of m_defaultFileSize
       */
      u_signed64 defaultFileSize() const {
        return m_defaultFileSize;
      }

      /**
       * Set the value of m_defaultFileSize
       * Default size used for space allocation in the case of a stage put with no size
       * explicitely given (ie size given was 0)
       * @param new_var the new value of m_defaultFileSize
       */
      void setDefaultFileSize(u_signed64 new_var) {
        m_defaultFileSize = new_var;
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
       * Add a TapePool* object to the m_tapePoolsVector list
       */
      void addTapePools(TapePool* add_object) {
        m_tapePoolsVector.push_back(add_object);
      }

      /**
       * Remove a TapePool* object from m_tapePoolsVector
       */
      void removeTapePools(TapePool* remove_object) {
        for (unsigned int i = 0; i < m_tapePoolsVector.size(); i++) {
          TapePool* item = m_tapePoolsVector[i];
          if (item == remove_object) {
            std::vector<TapePool*>::iterator it = m_tapePoolsVector.begin() + i;
            m_tapePoolsVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of TapePool* objects held by m_tapePoolsVector
       * @return list of TapePool* objects held by m_tapePoolsVector
       */
      std::vector<TapePool*>& tapePools() {
        return m_tapePoolsVector;
      }

      /**
       * Add a DiskPool* object to the m_diskPoolsVector list
       */
      void addDiskPools(DiskPool* add_object) {
        m_diskPoolsVector.push_back(add_object);
      }

      /**
       * Remove a DiskPool* object from m_diskPoolsVector
       */
      void removeDiskPools(DiskPool* remove_object) {
        for (unsigned int i = 0; i < m_diskPoolsVector.size(); i++) {
          DiskPool* item = m_diskPoolsVector[i];
          if (item == remove_object) {
            std::vector<DiskPool*>::iterator it = m_diskPoolsVector.begin() + i;
            m_diskPoolsVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of DiskPool* objects held by m_diskPoolsVector
       * @return list of DiskPool* objects held by m_diskPoolsVector
       */
      std::vector<DiskPool*>& diskPools() {
        return m_diskPoolsVector;
      }

    private:

      std::string m_policy;

      /*
       * Number of drives to use for this service class.
       * This is the default number, but it could be that occasionnally more drives are used, if a resource is shared with another service class using more drives
      */
      unsigned int m_nbDrives;

      /// the name of this SvcClass
      std::string m_name;

      /// Default size used for space allocation in the case of a stage put with no size explicitely given (ie size given was 0)
      u_signed64 m_defaultFileSize;

      /// The id of this object
      u_signed64 m_id;

      std::vector<TapePool*> m_tapePoolsVector;

      std::vector<DiskPool*> m_diskPoolsVector;

    }; // end of class SvcClass

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_SVCCLASS_HPP
