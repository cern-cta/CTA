/******************************************************************************
 *                      castor/rh/FileQueryResponse.hpp
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

#ifndef CASTOR_RH_FILEQUERYRESPONSE_HPP
#define CASTOR_RH_FILEQUERYRESPONSE_HPP

// Include Files
#include "castor/rh/Response.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace rh {

    /**
     * class FileQueryResponse
     * Response to the FileQueryRequest
     */
    class FileQueryResponse : public virtual Response {

    public:

      /**
       * Empty Constructor
       */
      FileQueryResponse() throw();

      /**
       * Empty Destructor
       */
      virtual ~FileQueryResponse() throw();

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
       * Get the value of m_fileName
       * Name of the file
       * @return the value of m_fileName
       */
      std::string fileName() const {
        return m_fileName;
      }

      /**
       * Set the value of m_fileName
       * Name of the file
       * @param new_var the new value of m_fileName
       */
      void setFileName(std::string new_var) {
        m_fileName = new_var;
      }

      /**
       * Get the value of m_fileId
       * Castor FileId for this file
       * @return the value of m_fileId
       */
      u_signed64 fileId() const {
        return m_fileId;
      }

      /**
       * Set the value of m_fileId
       * Castor FileId for this file
       * @param new_var the new value of m_fileId
       */
      void setFileId(u_signed64 new_var) {
        m_fileId = new_var;
      }

      /**
       * Get the value of m_status
       * Status of the file
       * @return the value of m_status
       */
      unsigned int status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * Status of the file
       * @param new_var the new value of m_status
       */
      void setStatus(unsigned int new_var) {
        m_status = new_var;
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
       * Get the value of m_poolName
       * Name of the pool containing the file
       * @return the value of m_poolName
       */
      std::string poolName() const {
        return m_poolName;
      }

      /**
       * Set the value of m_poolName
       * Name of the pool containing the file
       * @param new_var the new value of m_poolName
       */
      void setPoolName(std::string new_var) {
        m_poolName = new_var;
      }

      /**
       * Get the value of m_creationTime
       * Time of the file creation
       * @return the value of m_creationTime
       */
      u_signed64 creationTime() const {
        return m_creationTime;
      }

      /**
       * Set the value of m_creationTime
       * Time of the file creation
       * @param new_var the new value of m_creationTime
       */
      void setCreationTime(u_signed64 new_var) {
        m_creationTime = new_var;
      }

      /**
       * Get the value of m_accessTime
       * Time of the last access to the file
       * @return the value of m_accessTime
       */
      u_signed64 accessTime() const {
        return m_accessTime;
      }

      /**
       * Set the value of m_accessTime
       * Time of the last access to the file
       * @param new_var the new value of m_accessTime
       */
      void setAccessTime(u_signed64 new_var) {
        m_accessTime = new_var;
      }

      /**
       * Get the value of m_nbAccesses
       * Number of accesses to the file
       * @return the value of m_nbAccesses
       */
      unsigned int nbAccesses() const {
        return m_nbAccesses;
      }

      /**
       * Set the value of m_nbAccesses
       * Number of accesses to the file
       * @param new_var the new value of m_nbAccesses
       */
      void setNbAccesses(unsigned int new_var) {
        m_nbAccesses = new_var;
      }

    private:

    private:

      /// Name of the file
      std::string m_fileName;

      /// Castor FileId for this file
      u_signed64 m_fileId;

      /// Status of the file
      unsigned int m_status;

      /// Size of the file
      u_signed64 m_size;

      /// Name of the pool containing the file
      std::string m_poolName;

      /// Time of the file creation
      u_signed64 m_creationTime;

      /// Time of the last access to the file
      u_signed64 m_accessTime;

      /// Number of accesses to the file
      unsigned int m_nbAccesses;

      /// The id of this object
      u_signed64 m_id;

    }; // end of class FileQueryResponse

  }; // end of namespace rh

}; // end of namespace castor

#endif // CASTOR_RH_FILEQUERYRESPONSE_HPP
