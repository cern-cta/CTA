/******************************************************************************
 *                      castor/stager/Request.hpp
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

#ifndef CASTOR_STAGER_REQUEST_HPP
#define CASTOR_STAGER_REQUEST_HPP

// Include Files
#include "castor/IObject.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;
  class IClient;

  namespace stager {

    // Forward declarations
    class SvcClass;
    class SubRequest;

    /**
     * class Request
     * This is a common base class for all requests in the stager request queue.
     */
    class Request : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      Request() throw();

      /**
       * Empty Destructor
       */
      virtual ~Request() throw();

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
       * Get the value of m_flags
       * @return the value of m_flags
       */
      u_signed64 flags() const {
        return m_flags;
      }

      /**
       * Set the value of m_flags
       * @param new_var the new value of m_flags
       */
      void setFlags(u_signed64 new_var) {
        m_flags = new_var;
      }

      /**
       * Get the value of m_userName
       * Name of the user that submitted the request
       * @return the value of m_userName
       */
      std::string userName() const {
        return m_userName;
      }

      /**
       * Set the value of m_userName
       * Name of the user that submitted the request
       * @param new_var the new value of m_userName
       */
      void setUserName(std::string new_var) {
        m_userName = new_var;
      }

      /**
       * Get the value of m_euid
       * Id of the user that submitted the request
       * @return the value of m_euid
       */
      unsigned long euid() const {
        return m_euid;
      }

      /**
       * Set the value of m_euid
       * Id of the user that submitted the request
       * @param new_var the new value of m_euid
       */
      void setEuid(unsigned long new_var) {
        m_euid = new_var;
      }

      /**
       * Get the value of m_egid
       * Id of the group of the user that submitted the request
       * @return the value of m_egid
       */
      unsigned long egid() const {
        return m_egid;
      }

      /**
       * Set the value of m_egid
       * Id of the group of the user that submitted the request
       * @param new_var the new value of m_egid
       */
      void setEgid(unsigned long new_var) {
        m_egid = new_var;
      }

      /**
       * Get the value of m_mask
       * Mask for accessing files in the user space
       * @return the value of m_mask
       */
      unsigned long mask() const {
        return m_mask;
      }

      /**
       * Set the value of m_mask
       * Mask for accessing files in the user space
       * @param new_var the new value of m_mask
       */
      void setMask(unsigned long new_var) {
        m_mask = new_var;
      }

      /**
       * Get the value of m_pid
       * Process id of the user process
       * @return the value of m_pid
       */
      unsigned long pid() const {
        return m_pid;
      }

      /**
       * Set the value of m_pid
       * Process id of the user process
       * @param new_var the new value of m_pid
       */
      void setPid(unsigned long new_var) {
        m_pid = new_var;
      }

      /**
       * Get the value of m_machine
       * The machine that submitted the request
       * @return the value of m_machine
       */
      std::string machine() const {
        return m_machine;
      }

      /**
       * Set the value of m_machine
       * The machine that submitted the request
       * @param new_var the new value of m_machine
       */
      void setMachine(std::string new_var) {
        m_machine = new_var;
      }

      /**
       * Get the value of m_svcClassName
       * @return the value of m_svcClassName
       */
      std::string svcClassName() const {
        return m_svcClassName;
      }

      /**
       * Set the value of m_svcClassName
       * @param new_var the new value of m_svcClassName
       */
      void setSvcClassName(std::string new_var) {
        m_svcClassName = new_var;
      }

      /**
       * Get the value of m_userTag
       * This is a string that the user is free to use. It can be useful to classify and
       * select requests.
       * @return the value of m_userTag
       */
      std::string userTag() const {
        return m_userTag;
      }

      /**
       * Set the value of m_userTag
       * This is a string that the user is free to use. It can be useful to classify and
       * select requests.
       * @param new_var the new value of m_userTag
       */
      void setUserTag(std::string new_var) {
        m_userTag = new_var;
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
       * Get the value of m_client
       * @return the value of m_client
       */
      castor::IClient* client() const {
        return m_client;
      }

      /**
       * Set the value of m_client
       * @param new_var the new value of m_client
       */
      void setClient(castor::IClient* new_var) {
        m_client = new_var;
      }

    private:

      u_signed64 m_flags;

      /// Name of the user that submitted the request
      std::string m_userName;

      /// Id of the user that submitted the request
      unsigned long m_euid;

      /// Id of the group of the user that submitted the request
      unsigned long m_egid;

      /// Mask for accessing files in the user space
      unsigned long m_mask;

      /// Process id of the user process
      unsigned long m_pid;

      /// The machine that submitted the request
      std::string m_machine;

      std::string m_svcClassName;

      /// This is a string that the user is free to use. It can be useful to classify and select requests.
      std::string m_userTag;

      SvcClass* m_svcClass;

      std::vector<SubRequest*> m_subRequestsVector;

      castor::IClient* m_client;

    }; // end of class Request

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_REQUEST_HPP
