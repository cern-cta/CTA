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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_DISKCOPY_HPP
#define CASTOR_STAGER_DISKCOPY_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class SubRequest;
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
       * Get the value of m_diskcopyId
       * The Cuuid identifying the DiskCopy, stored as a human readable string
       * @return the value of m_diskcopyId
       */
      std::string diskcopyId() const {
        return m_diskcopyId;
      }

      /**
       * Set the value of m_diskcopyId
       * The Cuuid identifying the DiskCopy, stored as a human readable string
       * @param new_var the new value of m_diskcopyId
       */
      void setDiskcopyId(std::string new_var) {
        m_diskcopyId = new_var;
      }

      /**
       * Get the value of m_gcWeight
       * Weight possibly used by the garbage collector to decide who should be removed.
       * @return the value of m_gcWeight
       */
      float gcWeight() const {
        return m_gcWeight;
      }

      /**
       * Set the value of m_gcWeight
       * Weight possibly used by the garbage collector to decide who should be removed.
       * @param new_var the new value of m_gcWeight
       */
      void setGcWeight(float new_var) {
        m_gcWeight = new_var;
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
       * Add a SubRequest* object to the m_subRequestsVector list
       */
      void addSubRequests(SubRequest* add_object) {
        m_subRequestsVector.push_back(add_object);
      }

      /**
       * Remove a SubRequest* object from m_subRequestsVector
       */
      void removeSubRequests(SubRequest* remove_object) {
        for (unsigned int i = 0; i < m_subRequestsVector.size(); i++) {
          SubRequest* item = m_subRequestsVector[i];
          if (item == remove_object) {
            std::vector<SubRequest*>::iterator it = m_subRequestsVector.begin() + i;
            m_subRequestsVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of SubRequest* objects held by m_subRequestsVector
       * @return list of SubRequest* objects held by m_subRequestsVector
       */
      std::vector<SubRequest*>& subRequests() {
        return m_subRequestsVector;
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
      DiskCopyStatusCodes status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * @param new_var the new value of m_status
       */
      void setStatus(DiskCopyStatusCodes new_var) {
        m_status = new_var;
      }

    private:

      /// path of this copy in the filesystem
      std::string m_path;

      /// The Cuuid identifying the DiskCopy, stored as a human readable string
      std::string m_diskcopyId;

      /// Weight possibly used by the garbage collector to decide who should be removed.
      float m_gcWeight;

      /// The id of this object
      u_signed64 m_id;

      std::vector<SubRequest*> m_subRequestsVector;

      FileSystem* m_fileSystem;

      CastorFile* m_castorFile;

      DiskCopyStatusCodes m_status;

    }; // end of class DiskCopy

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_DISKCOPY_HPP
