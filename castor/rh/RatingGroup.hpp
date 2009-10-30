/******************************************************************************
 *                      RatingGroup.hpp
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
 * @(#)$RCSfile: ManagementThread.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2009/03/23 15:26:20 $ $Author: sponcec3 $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef CASTOR_RH_RATINGGROUP_HPP
#define CASTOR_RH_RATINGGROUP_HPP 1

// Include files
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace rh {

    /**
     * RatingGroup
     */
    class RatingGroup {

    public:

      /**
       * Constructor
       * @param name The name of the rating group
       * @exception Exception in case of error
       */
      RatingGroup(const std::string name)
        throw(castor::exception::Exception);

      /**
       * Default destructor
       */
      ~RatingGroup() throw() {};

      /**
       * Get the value of m_nbRequests
       * @return the value of m_nbRequests
       */
      unsigned int nbRequests() const {
        return m_nbRequests;
      }

      /**
       * Get the value of m_interval
       * @return the value of m_interval
       */
      unsigned int interval() const {
	return m_interval;
      }

      /**
       * Get the value of m_response
       * @return the value of m_response
       */
      std::string response() const {
        return m_response;
      }

      /**
       * Get the value of m_groupName
       * @return the value of m_groupName
       */
      std::string groupName() const {
        return m_groupName;
      }

    private:

      /// The maximum number of allowed requests for a given time interval
      unsigned int m_nbRequests;

      /// The time interval to be considered
      unsigned int m_interval;

      /// The response/action that should be taken when a user exceeds the
      /// configured thresholds defined on the group. One of: always-accept,
      /// close-connection, reject-with-error.
      std::string m_response;

      /// The name of the rating group
      std::string m_groupName;

    };

  }  // End of namespace rh

}  // End of namespace castor

#endif // CASTOR_RH_RATINGGROUP_HPP
