/******************************************************************************
 *                      RateLimiter.hpp
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

#ifndef CASTOR_RH_RATELIMITER_HPP
#define CASTOR_RH_RATELIMITER_HPP 1

// Include files
#include "castor/exception/Exception.hpp"
#include "castor/rh/RatingGroup.hpp"
#include <map>

// We use the C version of the API as opposed to C++ as we have move
// flexibility and control over handling errors
extern "C" {
#include <libmemcached/memcached.h>
}

namespace castor {

  namespace rh {

    /**
     * RateLimiter
     */
    class RateLimiter {

    public:

      /**
       * Default constructor
       */
      RateLimiter()
      throw(castor::exception::Exception);

      /**
       * Default destructor
       */
      ~RateLimiter() throw();

      /**
       * Init method. Responsible for the creation of all the memcached
       * specific data structure and the parsing of configuration options
       * in castor.conf
       */
      void init()
        throw(castor::exception::Exception);

      /**
       * Method used to check if a user has exceeded their maximum number
       * of allowed requests within a given time interval.
       *
       * @param user  The name of the user
       * @param group The name of the group to which the user belongs
       * @return NULL if the user has not exceeded the number of requests
       *         they are allowed to perform or a castor::rh::RatingGroup
       *         object describing the configuration information which caused
       *         the user to be rejected.
       * @exception Exception in case of error
       */
      castor::rh::RatingGroup *checkAndUpdateLimit(const std::string user,
                                                   const std::string group)
      throw(castor::exception::Exception);

      /**
       * Refer to previous method
       * @param euid The effective user id of the user
       * @param egid The effective group if to which the user belongs
       * @exception Exception in case of error
       */
      castor::rh::RatingGroup *checkAndUpdateLimit(const int euid,
                                                   const int egid)
      throw(castor::exception::Exception);

    private:

      /// Memcached structure
      memcached_st *m_memc;

      /// Memcached server structure
      memcached_server_st *m_servers;

      /// A container to hold the rating group configuration information
      std::map<std::string, castor::rh::RatingGroup *> m_config;

    };

  }  // End of namespace rh

}  // End of namespace castor

#endif // CASTOR_RH_RATELIMITER_HPP
