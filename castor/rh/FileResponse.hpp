/******************************************************************************
 *                      castor/rh/FileResponse.hpp
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

#ifndef CASTOR_RH_FILERESPONSE_HPP
#define CASTOR_RH_FILERESPONSE_HPP

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
     * class FileResponse
     * A response dealing with a castor file
     */
    class FileResponse : public virtual Response {

    public:

      /**
       * Empty Constructor
       */
      FileResponse() throw();

      /**
       * Empty Destructor
       */
      virtual ~FileResponse() throw();

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
       * Get the value of m_status
       * The status a the file we are considering
       * @return the value of m_status
       */
      unsigned int status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * The status a the file we are considering
       * @param new_var the new value of m_status
       */
      void setStatus(unsigned int new_var) {
        m_status = new_var;
      }

      /**
       * Get the value of m_castorFileName
       * The name of the file considered
       * @return the value of m_castorFileName
       */
      std::string castorFileName() const {
        return m_castorFileName;
      }

      /**
       * Set the value of m_castorFileName
       * The name of the file considered
       * @param new_var the new value of m_castorFileName
       */
      void setCastorFileName(std::string new_var) {
        m_castorFileName = new_var;
      }

      /**
       * Get the value of m_fileSize
       * The size of the file considered
       * @return the value of m_fileSize
       */
      u_signed64 fileSize() const {
        return m_fileSize;
      }

      /**
       * Set the value of m_fileSize
       * The size of the file considered
       * @param new_var the new value of m_fileSize
       */
      void setFileSize(u_signed64 new_var) {
        m_fileSize = new_var;
      }

      /**
       * Get the value of m_errorCode
       * The error code in case of error
       * @return the value of m_errorCode
       */
      unsigned int errorCode() const {
        return m_errorCode;
      }

      /**
       * Set the value of m_errorCode
       * The error code in case of error
       * @param new_var the new value of m_errorCode
       */
      void setErrorCode(unsigned int new_var) {
        m_errorCode = new_var;
      }

      /**
       * Get the value of m_errorMessage
       * The error message in case of error
       * @return the value of m_errorMessage
       */
      std::string errorMessage() const {
        return m_errorMessage;
      }

      /**
       * Set the value of m_errorMessage
       * The error message in case of error
       * @param new_var the new value of m_errorMessage
       */
      void setErrorMessage(std::string new_var) {
        m_errorMessage = new_var;
      }

      /**
       * Get the value of m_fileId
       * The castor file id identifying the file considered
       * @return the value of m_fileId
       */
      u_signed64 fileId() const {
        return m_fileId;
      }

      /**
       * Set the value of m_fileId
       * The castor file id identifying the file considered
       * @param new_var the new value of m_fileId
       */
      void setFileId(u_signed64 new_var) {
        m_fileId = new_var;
      }

      /**
       * Get the value of m_subreqId
       * The Cuuid of the SubRequest that dealt with this file, given as a human readable
       * string
       * @return the value of m_subreqId
       */
      std::string subreqId() const {
        return m_subreqId;
      }

      /**
       * Set the value of m_subreqId
       * The Cuuid of the SubRequest that dealt with this file, given as a human readable
       * string
       * @param new_var the new value of m_subreqId
       */
      void setSubreqId(std::string new_var) {
        m_subreqId = new_var;
      }

    private:

    private:

      /// The status a the file we are considering
      unsigned int m_status;

      /// The name of the file considered
      std::string m_castorFileName;

      /// The size of the file considered
      u_signed64 m_fileSize;

      /// The error code in case of error
      unsigned int m_errorCode;

      /// The error message in case of error
      std::string m_errorMessage;

      /// The castor file id identifying the file considered
      u_signed64 m_fileId;

      /// The Cuuid of the SubRequest that dealt with this file, given as a human readable string
      std::string m_subreqId;

      /// The id of this object
      u_signed64 m_id;

    }; // end of class FileResponse

  }; // end of namespace rh

}; // end of namespace castor

#endif // CASTOR_RH_FILERESPONSE_HPP
