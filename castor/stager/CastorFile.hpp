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
 * @author Castor Dev team, castor-dev@cern.ch
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
    class SvcClass;
    class FileClass;
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
      virtual void setId(u_signed64 id);

      /**
       * gets the id of the object
       */
      virtual u_signed64 id() const;

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
       * Get the value of m_fileSize
       * Size of the file
       * @return the value of m_fileSize
       */
      u_signed64 fileSize() const {
        return m_fileSize;
      }

      /**
       * Set the value of m_fileSize
       * Size of the file
       * @param new_var the new value of m_fileSize
       */
      void setFileSize(u_signed64 new_var) {
        m_fileSize = new_var;
      }

      /**
       * Get the value of m_svcClass
       * @return the value of m_svcClass
       */
      SvcClass* svcClass() const {
        return m_svcClass;
      }

      /**
       * Set the value of m_svcClass
       * @param new_var the new value of m_svcClass
       */
      void setSvcClass(SvcClass* new_var) {
        m_svcClass = new_var;
      }

      /**
       * Get the value of m_fileClass
       * @return the value of m_fileClass
       */
      FileClass* fileClass() const {
        return m_fileClass;
      }

      /**
       * Set the value of m_fileClass
       * @param new_var the new value of m_fileClass
       */
      void setFileClass(FileClass* new_var) {
        m_fileClass = new_var;
      }

      /**
       * Add a DiskCopy* object to the m_diskCopiesVector list
       */
      void addDiskCopies(DiskCopy* add_object) {
        m_diskCopiesVector.push_back(add_object);
      }

      /**
       * Remove a DiskCopy* object from m_diskCopiesVector
       */
      void removeDiskCopies(DiskCopy* remove_object) {
        for (unsigned int i = 0; i < m_diskCopiesVector.size(); i++) {
          DiskCopy* item = m_diskCopiesVector[i];
          if (item == remove_object) {
            std::vector<DiskCopy*>::iterator it = m_diskCopiesVector.begin() + i;
            m_diskCopiesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of DiskCopy* objects held by m_diskCopiesVector
       * @return list of DiskCopy* objects held by m_diskCopiesVector
       */
      std::vector<DiskCopy*>& diskCopies() {
        return m_diskCopiesVector;
      }

      /**
       * Add a TapeCopy* object to the m_tapeCopiesVector list
       */
      void addTapeCopies(TapeCopy* add_object) {
        m_tapeCopiesVector.push_back(add_object);
      }

      /**
       * Remove a TapeCopy* object from m_tapeCopiesVector
       */
      void removeTapeCopies(TapeCopy* remove_object) {
        for (unsigned int i = 0; i < m_tapeCopiesVector.size(); i++) {
          TapeCopy* item = m_tapeCopiesVector[i];
          if (item == remove_object) {
            std::vector<TapeCopy*>::iterator it = m_tapeCopiesVector.begin() + i;
            m_tapeCopiesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of TapeCopy* objects held by m_tapeCopiesVector
       * @return list of TapeCopy* objects held by m_tapeCopiesVector
       */
      std::vector<TapeCopy*>& tapeCopies() {
        return m_tapeCopiesVector;
      }

    private:

    private:

      /// The id of this castor file. This identifies it uniquely
      u_signed64 m_fileId;

      /// The name server hosting this castor file
      std::string m_nsHost;

      /// Size of the file
      u_signed64 m_fileSize;

      /// The id of this object
      u_signed64 m_id;

      SvcClass* m_svcClass;

      FileClass* m_fileClass;

      std::vector<DiskCopy*> m_diskCopiesVector;

      std::vector<TapeCopy*> m_tapeCopiesVector;

    }; // end of class CastorFile

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_CASTORFILE_HPP
