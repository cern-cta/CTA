/******************************************************************************
 *                      castor/stager/DiskCopy.hpp
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

#ifndef CASTOR_STAGER_DISKCOPY_HPP
#define CASTOR_STAGER_DISKCOPY_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/DiskCopyStatusCode.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class FileSystem;
    class CastorFile;

    /**
     * class DiskCopy
     * A copy of a file in the disk pool
     */
    class DiskCopy : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      DiskCopy() throw();

      /**
       * Empty Destructor
       */
      virtual ~DiskCopy() throw();

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
       * Get the value of m_path
       * path of this copy in the filesystem
       * @return the value of m_path
       */
      std::string path() const {
        return m_path;
      }

      /**
       * Set the value of m_path
       * path of this copy in the filesystem
       * @param new_var the new value of m_path
       */
      void setPath(std::string new_var) {
        m_path = new_var;
      }

      /**
       * Get the value of m_fileSystem
       * @return the value of m_fileSystem
       */
      FileSystem* fileSystem() const {
        return m_fileSystem;
      }

      /**
       * Set the value of m_fileSystem
       * @param new_var the new value of m_fileSystem
       */
      void setFileSystem(FileSystem* new_var) {
        m_fileSystem = new_var;
      }

      /**
       * Get the value of m_castorFile
       * @return the value of m_castorFile
       */
      CastorFile* castorFile() const {
        return m_castorFile;
      }

      /**
       * Set the value of m_castorFile
       * @param new_var the new value of m_castorFile
       */
      void setCastorFile(CastorFile* new_var) {
        m_castorFile = new_var;
      }

      /**
       * Get the value of m_status
       * @return the value of m_status
       */
      DiskCopyStatusCode status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * @param new_var the new value of m_status
       */
      void setStatus(DiskCopyStatusCode new_var) {
        m_status = new_var;
      }

    private:

    private:

      /// path of this copy in the filesystem
      std::string m_path;

      /// The id of this object
      u_signed64 m_id;

      FileSystem* m_fileSystem;

      CastorFile* m_castorFile;

      DiskCopyStatusCode m_status;

    }; // end of class DiskCopy

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_DISKCOPY_HPP
