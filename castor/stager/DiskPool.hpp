/******************************************************************************
 *                      castor/stager/DiskPool.hpp
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

#ifndef CASTOR_STAGER_DISKPOOL_HPP
#define CASTOR_STAGER_DISKPOOL_HPP

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
    class SvcClass;
    class FileSystem;

    /**
     * class DiskPool
     * A Resource as seen by the Scheduler. Resources can be allocated to one or many
     * projects are are composed of a set of filesystems.
     */
    class DiskPool : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      DiskPool() throw();

      /**
       * Empty Destructor
       */
      virtual ~DiskPool() throw();

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
       * Get the value of m_name
       * Name of this pool
       * @return the value of m_name
       */
      std::string name() const {
        return m_name;
      }

      /**
       * Set the value of m_name
       * Name of this pool
       * @param new_var the new value of m_name
       */
      void setName(std::string new_var) {
        m_name = new_var;
      }

      /**
       * Add a FileSystem* object to the m_fileSystemsVector list
       */
      void addFileSystems(FileSystem* add_object) {
        m_fileSystemsVector.push_back(add_object);
      }

      /**
       * Remove a FileSystem* object from m_fileSystemsVector
       */
      void removeFileSystems(FileSystem* remove_object) {
        for (unsigned int i = 0; i < m_fileSystemsVector.size(); i++) {
          FileSystem* item = m_fileSystemsVector[i];
          if (item == remove_object) {
            std::vector<FileSystem*>::iterator it = m_fileSystemsVector.begin() + i;
            m_fileSystemsVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of FileSystem* objects held by m_fileSystemsVector
       * @return list of FileSystem* objects held by m_fileSystemsVector
       */
      std::vector<FileSystem*>& fileSystems() {
        return m_fileSystemsVector;
      }

      /**
       * Add a SvcClass* object to the m_svcClassesVector list
       */
      void addSvcClasses(SvcClass* add_object) {
        m_svcClassesVector.push_back(add_object);
      }

      /**
       * Remove a SvcClass* object from m_svcClassesVector
       */
      void removeSvcClasses(SvcClass* remove_object) {
        for (unsigned int i = 0; i < m_svcClassesVector.size(); i++) {
          SvcClass* item = m_svcClassesVector[i];
          if (item == remove_object) {
            std::vector<SvcClass*>::iterator it = m_svcClassesVector.begin() + i;
            m_svcClassesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of SvcClass* objects held by m_svcClassesVector
       * @return list of SvcClass* objects held by m_svcClassesVector
       */
      std::vector<SvcClass*>& svcClasses() {
        return m_svcClassesVector;
      }

    private:

    private:

      /// Name of this pool
      std::string m_name;

      /// The id of this object
      u_signed64 m_id;

      std::vector<FileSystem*> m_fileSystemsVector;

      std::vector<SvcClass*> m_svcClassesVector;

    }; // end of class DiskPool

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_DISKPOOL_HPP
