/******************************************************************************
 *                      castor/stager/TapeCopyForMigration.hpp
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

#ifndef CASTOR_STAGER_TAPECOPYFORMIGRATION_HPP
#define CASTOR_STAGER_TAPECOPYFORMIGRATION_HPP

// Include Files
#include "castor/stager/TapeCopy.hpp"
#include "osdep.h"
#include <string>

namespace castor {

  namespace stager {

    /**
     * class TapeCopyForMigration
     * This class is a wrapper around a Tape Copy that represents a TapCopy ready for
     * Migration. It thus has information about the physical file to be copied.
     */
    class TapeCopyForMigration : public TapeCopy {

    public:

      /**
       * Empty Constructor
       */
      TapeCopyForMigration() throw();

      /**
       * Empty Destructor
       */
      virtual ~TapeCopyForMigration() throw();

      /**
       * Get the value of m_diskServer
       * The disk server on which the file to be migrated resides
       * @return the value of m_diskServer
       */
      std::string diskServer() const {
        return m_diskServer;
      }

      /**
       * Set the value of m_diskServer
       * The disk server on which the file to be migrated resides
       * @param new_var the new value of m_diskServer
       */
      void setDiskServer(std::string new_var) {
        m_diskServer = new_var;
      }

      /**
       * Get the value of m_path
       * The path of the file to be migrated on the disk server
       * @return the value of m_path
       */
      std::string path() const {
        return m_path;
      }

      /**
       * Set the value of m_path
       * The path of the file to be migrated on the disk server
       * @param new_var the new value of m_path
       */
      void setPath(std::string new_var) {
        m_path = new_var;
      }

      /**
       * Get the value of m_castorFileID
       * The castorFile ID of the file to migrate
       * @return the value of m_castorFileID
       */
      u_signed64 castorFileID() const {
        return m_castorFileID;
      }

      /**
       * Set the value of m_castorFileID
       * The castorFile ID of the file to migrate
       * @param new_var the new value of m_castorFileID
       */
      void setCastorFileID(u_signed64 new_var) {
        m_castorFileID = new_var;
      }

      /**
       * Get the value of m_nsHost
       * The name server dealing with the file to migrate
       * @return the value of m_nsHost
       */
      std::string nsHost() const {
        return m_nsHost;
      }

      /**
       * Set the value of m_nsHost
       * The name server dealing with the file to migrate
       * @param new_var the new value of m_nsHost
       */
      void setNsHost(std::string new_var) {
        m_nsHost = new_var;
      }

      /**
       * Get the value of m_fileSize
       * The size of the file to migrate
       * @return the value of m_fileSize
       */
      u_signed64 fileSize() const {
        return m_fileSize;
      }

      /**
       * Set the value of m_fileSize
       * The size of the file to migrate
       * @param new_var the new value of m_fileSize
       */
      void setFileSize(u_signed64 new_var) {
        m_fileSize = new_var;
      }

    private:

      /// The disk server on which the file to be migrated resides
      std::string m_diskServer;

      /// The path of the file to be migrated on the disk server
      std::string m_path;

      /// The castorFile ID of the file to migrate
      u_signed64 m_castorFileID;

      /// The name server dealing with the file to migrate
      std::string m_nsHost;

      /// The size of the file to migrate
      u_signed64 m_fileSize;

    }; // end of class TapeCopyForMigration

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_TAPECOPYFORMIGRATION_HPP
