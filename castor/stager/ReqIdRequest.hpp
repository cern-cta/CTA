/******************************************************************************
 *                      castor/stager/ReqIdRequest.hpp
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

#ifndef CASTOR_STAGER_REQIDREQUEST_HPP
#define CASTOR_STAGER_REQIDREQUEST_HPP

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
    class ReqId;

    /**
     * class ReqIdRequest
     * A request dealing with a set of reqids
     */
    class ReqIdRequest : public virtual Request {

    public:

      /**
       * Empty Constructor
       */
      ReqIdRequest() throw();

      /**
       * Empty Destructor
       */
      virtual ~ReqIdRequest() throw();

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
       * Add a ReqId* object to the m_reqidsVector list
       */
      void addReqids(ReqId* add_object) {
        m_reqidsVector.push_back(add_object);
      }

      /**
       * Remove a ReqId* object from m_reqidsVector
       */
      void removeReqids(ReqId* remove_object) {
        for (unsigned int i = 0; i < m_reqidsVector.size(); i++) {
          ReqId* item = m_reqidsVector[i];
          if (item == remove_object) {
            std::vector<ReqId*>::iterator it = m_reqidsVector.begin() + i;
            m_reqidsVector.erase(it);
            return;
          }
        }
      }

      /**
       * Get the list of ReqId* objects held by m_reqidsVector
       * @return list of ReqId* objects held by m_reqidsVector
       */
      std::vector<ReqId*>& reqids() {
        return m_reqidsVector;
      }

    private:

      std::vector<ReqId*> m_reqidsVector;

    }; // end of class ReqIdRequest

  }; // end of namespace stager

}; // end of namespace castor

#endif // CASTOR_STAGER_REQIDREQUEST_HPP
