/******************************************************************************
 *                      castor/stager/CastorFile.hpp
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

#ifndef CASTOR_STAGER_CASTORFILE_HPP
#define CASTOR_STAGER_CASTORFILE_HPP

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
    class TapeCopy;
    class DiskCopy;

    /**
     * class CastorFile
     * A castor file.
     */
    class CastorFile : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      CastorFile() throw();

      /**
       * Empty Destructor
       */
      virtual ~CastorFile() throw();

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
       * Get the value of m_fileId
       * The id of this castor file. This identifies it uniquely
       * @return the value of m_fileId
       */
      u_signed64 fileId() const {
        return m_fileId;
      }

      /**
       * Set the value of m_fileId
       * The id of this castor file. This identifies it uniquely
       * @param new_var the new value of m_fileId
       */
      void setFileId(u_signed64 new_var) {
        m_fileId = new_var;
      }

      /**
       * Get the value of m_nsHost
       * The name server hosting this castor file
       * @return the value of m_nsHost
       */
      std::string nsHost() const {
        return m_nsHost;
      }

      /**
       * Set the value of m_nsHost
       * The name server hosting this castor file
       * @param new_var the new value of m_nsHost
       */
      void setNsHost(std::string new_var) {
        m_nsHost = new_var;
      }

      /**
       * Get the value of m_size
       * Size of the file
       * @return the value of m_size
       */
      u_signed64 size() const {
        return m_size;
      }

      /**
       * Set the value of m_size
       * Size of the file
       * @param new_var the new value of m_size
       */
      void setSize(u_signed64 new_var) {
        m_size = new_var;
      }

      /**
       * Add a DiskCopy* object to the m_diskFileCopiesVector list
       */
      void addDiskFileCopies(DiskCopy* add_object) {
        m_diskFileCopiesVector.push_back(add_object);
      }

      /**
       * Remove a DiskCopy* object from m_diskFileCopiesVector
       */
      void removeDiskFileCopies(DiskCopy* remove_object) {
        for (unsigned int i = 0; i < m_diskFileCopiesVector.size(); i++) {
          DiskCopy* item = m_diskFileCopiesVector[i];
          if (item == remove_object) {
            std::vector<DiskCopy*>::iterator it = m_diskFileCopiesVector.begin() + i;
            m_diskFileCopiesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of DiskCopy* objects held by m_diskFileCopiesVector
       * @return list of DiskCopy* objects held by m_diskFileCopiesVector
       */
      std::vector<DiskCopy*>& diskFileCopies() {
        return m_diskFileCopiesVector;
      }

      /**
       * Add a TapeCopy* object to the m_copiesVector list
       */
      void addCopies(TapeCopy* add_object) {
        m_copiesVector.push_back(add_object);
      }

      /**
       * Remove a TapeCopy* object from m_copiesVector
       */
      void removeCopies(TapeCopy* remove_object) {
        for (unsigned int i = 0; i < m_copiesVector.size(); i++) {
          TapeCopy* item = m_copiesVector[i];
          if (item == remove_object) {
            std::vector<TapeCopy*>::iterator it = m_copiesVector.begin() + i;
            m_copiesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of TapeCopy* objects held by m_copiesVector
       * @return list of TapeCopy* objects held by m_copiesVector
       */
      std::vector<TapeCopy*>& copies() {
        return m_copiesVector;
      }

    private:

    private:

      /// The id of this castor file. This identifies it uniquely
      u_signed64 m_fileId;

      /// The name server hosting this castor file
      std::string m_nsHost;

      /// Size of the file
      u_signed64 m_size;

      /// The id of this object
      unsigned long m_id;

      std::vector<DiskCopy*> m_diskFileCopiesVector;

      std::vector<TapeCopy*> m_copiesVector;

    }; // end of class CastorFile

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_CASTORFILE_HPP
