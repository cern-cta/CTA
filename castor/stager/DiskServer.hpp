/******************************************************************************
 *                      castor/stager/DiskServer.hpp
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

#ifndef CASTOR_STAGER_DISKSERVER_HPP
#define CASTOR_STAGER_DISKSERVER_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class FileSystem;

    /**
     * class DiskServer
     * A disk server is a physical device handling some filesystems
     */
    class DiskServer : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      DiskServer() throw();

      /**
       * Empty Destructor
       */
      virtual ~DiskServer() throw();

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
       * Get the value of m_name
       * Name of the DiskServer
       * @return the value of m_name
       */
      std::string name() const {
        return m_name;
      }

      /**
       * Set the value of m_name
       * Name of the DiskServer
       * @param new_var the new value of m_name
       */
      void setName(std::string new_var) {
        m_name = new_var;
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
       * Get the value of m_status
       * @return the value of m_status
       */
      DiskServerStatusCode status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * @param new_var the new value of m_status
       */
      void setStatus(DiskServerStatusCode new_var) {
        m_status = new_var;
      }

    private:

      /// Name of the DiskServer
      std::string m_name;

      /// The id of this object
      u_signed64 m_id;

      std::vector<FileSystem*> m_fileSystemsVector;

      DiskServerStatusCode m_status;

    }; // end of class DiskServer

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_DISKSERVER_HPP
