/******************************************************************************
 *                      castor/stager/BulkRequestResult.hpp
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
 *
 * Description of the result of the processing of a bulk request
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_BULKREQUESTRESULT_HPP
#define CASTOR_STAGER_BULKREQUESTRESULT_HPP

// Include Files
#include "castor/IObject.hpp"
#include "castor/Constants.hpp"
#include "castor/rh/Client.hpp"
#include "castor/stager/FileResult.hpp"
#include <iostream>
#include <string>
#include <vector>

namespace castor {

  // Forward declarations
  class ObjectSet;

  namespace stager {

    /**
     * class BulkRequestResult
     * Describes the result of a bulk request
     */
    class BulkRequestResult : public virtual castor::IObject {

    public:

      /**
       * Empty Constructor
       */
      BulkRequestResult() throw();

      /**
       * Empty Destructor
       */
      virtual ~BulkRequestResult() throw() {};

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
       * Get the value of m_reqType
       * @return the value of m_reqType
       */
      ObjectsIds reqType() const {
        return m_reqType;
      }

      /**
       * Set the value of m_reqType
       * @param new_var the new value of m_reqType
       */
      void setReqType(ObjectsIds new_var) {
        m_reqType = new_var;
      }

      /**
       * Get the value of m_reqId
       * @return the value of m_reqId
       */
      const std::string& reqId() const {
        return m_reqId;
      }

      /**
       * Set the value of m_reqId
       * @param new_var the new value of m_reqId
       */
      void setReqId(std::string new_var) {
        m_reqId = new_var;
      }

      /**
       * Get the value of m_client
       * @return the value of m_client
       */
      castor::rh::Client& client() {
        return m_client;
      }

      /**
       * Get the value of m_subResults
       * @return the value of m_subResults
       */
      std::vector<castor::stager::FileResult>& subResults() {
        return m_subResults;
      }

    private:

      /// the type of request we are dealing with
      ObjectsIds m_reqType;

      /// the uuid of the request we are dealing with
      std::string m_reqId;

      /// the client that sent this request
      castor::rh::Client m_client;

      /// the list of subresults for each file
      std::vector<castor::stager::FileResult> m_subResults;

      /// The id of this object
      u_signed64 m_id;

    }; /* end of class BulkRequestResult */

  } /* end of namespace stager */

} /* end of namespace castor */

#endif // CASTOR_STAGER_BULKREQUESTRESULT_HPP
