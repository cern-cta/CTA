/******************************************************************************
 *                      castor/stager/SubRequest.hpp
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

#ifndef CASTOR_STAGER_SUBREQUEST_HPP
#define CASTOR_STAGER_SUBREQUEST_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class Request;
    class DiskCopy;
    class CastorFile;

    /**
     * class SubRequest
     * A subpart of a request delaing with a single castor file
     */
    class SubRequest : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      SubRequest() throw();

      /**
       * Empty Destructor
       */
      virtual ~SubRequest() throw();

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
       * Get the value of m_retryCounter
       * @return the value of m_retryCounter
       */
      unsigned int retryCounter() const {
        return m_retryCounter;
      }

      /**
       * Set the value of m_retryCounter
       * @param new_var the new value of m_retryCounter
       */
      void setRetryCounter(unsigned int new_var) {
        m_retryCounter = new_var;
      }

      /**
       * Get the value of m_fileName
       * Name of the file this SubRequest deals with. When stored in the catalog, this is
       * redundant with the link to the CastorFile table. However, this is needed in the
       * client
       * @return the value of m_fileName
       */
      std::string fileName() const {
        return m_fileName;
      }

      /**
       * Set the value of m_fileName
       * Name of the file this SubRequest deals with. When stored in the catalog, this is
       * redundant with the link to the CastorFile table. However, this is needed in the
       * client
       * @param new_var the new value of m_fileName
       */
      void setFileName(std::string new_var) {
        m_fileName = new_var;
      }

      /**
       * Get the value of m_protocol
       * The protocol that will be used to access the file this SubRequest deals with
       * @return the value of m_protocol
       */
      std::string protocol() const {
        return m_protocol;
      }

      /**
       * Set the value of m_protocol
       * The protocol that will be used to access the file this SubRequest deals with
       * @param new_var the new value of m_protocol
       */
      void setProtocol(std::string new_var) {
        m_protocol = new_var;
      }

      /**
       * Get the value of m_poolName
       * The name of the TapePool to use for the file this SubRequest deals with
       * @return the value of m_poolName
       */
      std::string poolName() const {
        return m_poolName;
      }

      /**
       * Set the value of m_poolName
       * The name of the TapePool to use for the file this SubRequest deals with
       * @param new_var the new value of m_poolName
       */
      void setPoolName(std::string new_var) {
        m_poolName = new_var;
      }

      /**
       * Get the value of m_xsize
       * The size of the file. This gives how many bytes should be allocated rather than
       * the default.
       * @return the value of m_xsize
       */
      u_signed64 xsize() const {
        return m_xsize;
      }

      /**
       * Set the value of m_xsize
       * The size of the file. This gives how many bytes should be allocated rather than
       * the default.
       * @param new_var the new value of m_xsize
       */
      void setXsize(u_signed64 new_var) {
        m_xsize = new_var;
      }

      /**
       * Get the value of m_diskcopy
       * @return the value of m_diskcopy
       */
      DiskCopy* diskcopy() const {
        return m_diskcopy;
      }

      /**
       * Set the value of m_diskcopy
       * @param new_var the new value of m_diskcopy
       */
      void setDiskcopy(DiskCopy* new_var) {
        m_diskcopy = new_var;
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
       * Add a SubRequest* object to the m_parentVector list
       */
      void addParent(SubRequest* add_object) {
        m_parentVector.push_back(add_object);
      }

      /**
       * Remove a SubRequest* object from m_parentVector
       */
      void removeParent(SubRequest* remove_object) {
        for (unsigned int i = 0; i < m_parentVector.size(); i++) {
          SubRequest* item = m_parentVector[i];
          if (item == remove_object) {
            std::vector<SubRequest*>::iterator it = m_parentVector.begin() + i;
            m_parentVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of SubRequest* objects held by m_parentVector
       * @return list of SubRequest* objects held by m_parentVector
       */
      std::vector<SubRequest*>& parent() {
        return m_parentVector;
      }

      /**
       * Add a SubRequest* object to the m_childVector list
       */
      void addChild(SubRequest* add_object) {
        m_childVector.push_back(add_object);
      }

      /**
       * Remove a SubRequest* object from m_childVector
       */
      void removeChild(SubRequest* remove_object) {
        for (unsigned int i = 0; i < m_childVector.size(); i++) {
          SubRequest* item = m_childVector[i];
          if (item == remove_object) {
            std::vector<SubRequest*>::iterator it = m_childVector.begin() + i;
            m_childVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of SubRequest* objects held by m_childVector
       * @return list of SubRequest* objects held by m_childVector
       */
      std::vector<SubRequest*>& child() {
        return m_childVector;
      }

      /**
       * Get the value of m_request
       * @return the value of m_request
       */
      Request* request() const {
        return m_request;
      }

      /**
       * Set the value of m_request
       * @param new_var the new value of m_request
       */
      void setRequest(Request* new_var) {
        m_request = new_var;
      }

      /**
       * Get the value of m_status
       * @return the value of m_status
       */
      SubRequestStatusCodes status() const {
        return m_status;
      }

      /**
       * Set the value of m_status
       * @param new_var the new value of m_status
       */
      void setStatus(SubRequestStatusCodes new_var) {
        m_status = new_var;
      }

    private:

    private:

      unsigned int m_retryCounter;

      /*
       * Name of the file this SubRequest deals with.
       * When stored in the catalog, this is redundant with the link to the CastorFile table. However, this is needed in the client
      */
      std::string m_fileName;

      /// The protocol that will be used to access the file this SubRequest deals with
      std::string m_protocol;

      /// The name of the TapePool to use for the file this SubRequest deals with
      std::string m_poolName;

      /// The size of the file. This gives how many bytes should be allocated rather than the default.
      u_signed64 m_xsize;

      /// The id of this object
      u_signed64 m_id;

      DiskCopy* m_diskcopy;

      CastorFile* m_castorFile;

      std::vector<SubRequest*> m_parentVector;

      std::vector<SubRequest*> m_childVector;

      Request* m_request;

      SubRequestStatusCodes m_status;

    }; // end of class SubRequest

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_SUBREQUEST_HPP
