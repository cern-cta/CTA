/******************************************************************************
 *                      castor/stager/FileRequest.hpp
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

#ifndef CASTOR_STAGER_FILEREQUEST_HPP
#define CASTOR_STAGER_FILEREQUEST_HPP

// Include Files
#include "castor/stager/Request.hpp"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    // Forward declarations
    class SubRequest;

    /**
     * class FileRequest
     * An abstract ancester for all file requests
     */
    class FileRequest : public virtual Request {

    public:

      /**
       * Empty Constructor
       */
      FileRequest() throw();

      /**
       * Empty Destructor
       */
      virtual ~FileRequest() throw();

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

    private:

      std::vector<SubRequest*> m_subRequestsVector;

    }; // end of class FileRequest

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_FILEREQUEST_HPP
