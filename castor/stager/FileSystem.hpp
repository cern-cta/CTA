/******************************************************************************
 *                      castor/stager/FileSystem.hpp
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

#ifndef CASTOR_STAGER_FILESYSTEM_HPP
#define CASTOR_STAGER_FILESYSTEM_HPP

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
    class DiskServer;
    class DiskPool;
    class DiskCopy;

    /**
     * class FileSystem
     * A file system used in a disk pool
     */
    class FileSystem : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      FileSystem() throw();

      /**
       * Empty Destructor
       */
      virtual ~FileSystem() throw();

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
       * Get the value of m_free
       * Free space on the filesystem
       * @return the value of m_free
       */
      u_signed64 free() const {
        return m_free;
      }

      /**
       * Set the value of m_free
       * Free space on the filesystem
       * @param new_var the new value of m_free
       */
      void setFree(u_signed64 new_var) {
        m_free = new_var;
      }

      /**
       * Get the value of m_weight
       * Weight of the filesystem, as computed by the expert system
       * @return the value of m_weight
       */
      float weight() const {
        return m_weight;
      }

      /**
       * Set the value of m_weight
       * Weight of the filesystem, as computed by the expert system
       * @param new_var the new value of m_weight
       */
      void setWeight(float new_var) {
        m_weight = new_var;
      }

      /**
       * Get the value of m_fsDeviation
       * @return the value of m_fsDeviation
       */
      float fsDeviation() const {
        return m_fsDeviation;
      }

      /**
       * Set the value of m_fsDeviation
       * @param new_var the new value of m_fsDeviation
       */
      void setFsDeviation(float new_var) {
        m_fsDeviation = new_var;
      }

      /**
       * Get the value of m_randomize
       * @return the value of m_randomize
       */
      int randomize() const {
        return m_randomize;
      }

      /**
       * Set the value of m_randomize
       * @param new_var the new value of m_randomize
       */
      void setRandomize(int new_var) {
        m_randomize = new_var;
      }

      /**
       * Get the value of m_mountPoint
       * Mount point of this file system on the disk server
       * @return the value of m_mountPoint
       */
      std::string mountPoint() const {
        return m_mountPoint;
      }

      /**
       * Set the value of m_mountPoint
       * Mount point of this file system on the disk server
       * @param new_var the new value of m_mountPoint
       */
      void setMountPoint(std::string new_var) {
        m_mountPoint = new_var;
      }

      /**
       * Get the value of m_diskPool
       * @return the value of m_diskPool
       */
      DiskPool* diskPool() const {
        return m_diskPool;
      }

      /**
       * Set the value of m_diskPool
       * @param new_var the new value of m_diskPool
       */
      void setDiskPool(DiskPool* new_var) {
        m_diskPool = new_var;
      }

      /**
       * Add a DiskCopy* object to the m_copiesVector list
       */
      void addCopies(DiskCopy* add_object) {
        m_copiesVector.push_back(add_object);
      }

      /**
       * Remove a DiskCopy* object from m_copiesVector
       */
      void removeCopies(DiskCopy* remove_object) {
        for (unsigned int i = 0; i < m_copiesVector.size(); i++) {
          DiskCopy* item = m_copiesVector[i];
          if (item == remove_object) {
            std::vector<DiskCopy*>::iterator it = m_copiesVector.begin() + i;
            m_copiesVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of DiskCopy* objects held by m_copiesVector
       * @return list of DiskCopy* objects held by m_copiesVector
       */
      std::vector<DiskCopy*>& copies() {
        return m_copiesVector;
      }

      /**
       * Get the value of m_diskserver
       * @return the value of m_diskserver
       */
      DiskServer* diskserver() const {
        return m_diskserver;
      }

      /**
       * Set the value of m_diskserver
       * @param new_var the new value of m_diskserver
       */
      void setDiskserver(DiskServer* new_var) {
        m_diskserver = new_var;
      }

    private:

    private:

      /// Free space on the filesystem
      u_signed64 m_free;

      /// Weight of the filesystem, as computed by the expert system
      float m_weight;

      float m_fsDeviation;

      int m_randomize;

      /// Mount point of this file system on the disk server
      std::string m_mountPoint;

      /// The id of this object
      u_signed64 m_id;

      DiskPool* m_diskPool;

      std::vector<DiskCopy*> m_copiesVector;

      DiskServer* m_diskserver;

    }; // end of class FileSystem

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_FILESYSTEM_HPP
