/******************************************************************************
 *                      castor/stager/FileResult.hpp
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
 * Description of the result of the processing of a file within a bulk request
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_FILERESULT_HPP
#define CASTOR_STAGER_FILERESULT_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    /**
     * class FileResult
     * Describes the result of a bulk request
     */
    class FileResult : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      FileResult() throw();

      /**
       * Empty Destructor
       */
      virtual ~FileResult() throw() {};

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
       * Get the value of m_fileId
       * fileId of the underlying castorfile
       * @return the value of m_fileId
       */
      u_signed64 fileId() const {
        return m_fileId;
      }

      /**
       * Set the value of m_fileId
       * fileId of the underlying castorfile
       * @param new_var the new value of m_fileId
       */
      void setFileId(u_signed64 new_var) {
        m_fileId = new_var;
      }

      /**
       * Get the value of m_nsHost
       * name server host of the underlying castorfile
       * @return the value of m_nsHost
       */
      std::string nsHost() const {
        return m_nsHost;
      }

      /**
       * Set the value of m_nsHost
       * name server host of the underlying castorfile
       * @param new_var the new value of m_nsHost
       */
      void setNsHost(std::string new_var) {
        m_nsHost = new_var;
      }

      /**
       * Get the value of m_errorCode
       * error code associated with this subrequest. Has a meaning only for subrequests
       * in status SUBREQUEST_FAILED
       * @return the value of m_errorCode
       */
      int errorCode() const {
        return m_errorCode;
      }

      /**
       * Set the value of m_errorCode
       * error code associated with this subrequest. Has a meaning only for subrequests
       * in status SUBREQUEST_FAILED
       * @param new_var the new value of m_errorCode
       */
      void setErrorCode(int new_var) {
        m_errorCode = new_var;
      }

      /**
       * Get the value of m_errorMessage
       * error message associated with this subrequest. Has a meaning only for
       * subrequests in status SUBREQUEST_FAILED
       * @return the value of m_errorMessage
       */
      std::string errorMessage() const {
        return m_errorMessage;
      }

      /**
       * Set the value of m_errorMessage
       * error message associated with this subrequest. Has a meaning only for
       * subrequests in status SUBREQUEST_FAILED
       * @param new_var the new value of m_errorMessage
       */
      void setErrorMessage(std::string new_var) {
        m_errorMessage = new_var;
      }

    private:

      /// fileId of the underlying castorfile
      u_signed64 m_fileId;

      /// name server host of the underlying castorfile
      std::string m_nsHost;

      /// error code associated with this subrequest. Has a meaning only for subrequests in status SUBREQUEST_FAILED
      int m_errorCode;

      /// error message associated with this subrequest. Has a meaning only for subrequests in status SUBREQUEST_FAILED
      std::string m_errorMessage;

      /// The id of this object
      u_signed64 m_id;

    }; /* end of class FileResult */

  } /* end of namespace stager */

} /* end of namespace castor */

#endif // CASTOR_STAGER_FILERESULT_HPP
