/******************************************************************************
 *                      CastorConfiguration.hpp
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
 * Implementation of the handling of the CASTOR configuration file
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef H_CASTOR_COMMON_HPP
#define H_CASTOR_COMMON_HPP 1

// Include Files
#include <string>
#include <map>
#include "castor/exception/Exception.hpp"

namespace castor {
  
  namespace common {
    
    /**
     * represents a category from the CASTOR configuration file
     */
    typedef std::map<std::string, std::string> ConfCategory;

    /**
     * a class representing the configuration of castor.
     * This configurations is obtained from the local file given in the
     * constructor and will be updated regularly. The time between two
     * updates is taken from the Config/ExpirationDelay entry of the
     * configuration itself and defaults to 5mn if no such entry is found
     */
    class CastorConfiguration {

    public:

      /**
       * static method for getting a given configuration
       * @param fileName the name of the file to be used for filling this configuration
       */
      static CastorConfiguration& getConfig(const std::string filename = "/etc/castor/castor.conf")
        throw (castor::exception::Exception);
        
    public:
      
      /**
       * constructor
       * @param fileName the file that should be used to build the configuration
       */
      CastorConfiguration(std::string fileName = "/etc/castor/castor.conf")
      throw (castor::exception::Exception);

      /**
       * copy constructor
       * @param other instance of CastorConfiguration class
       */
      CastorConfiguration(const CastorConfiguration & other)
        throw (castor::exception::Exception);

      /**
       * destrcutor
       */
      virtual ~CastorConfiguration();

      /**
       * assignment operator
       * @param other instance of CastorConfiguration class
       */
      CastorConfiguration & operator=(const CastorConfiguration & other)
        throw (castor::exception::Exception);

      /**
       * retrieves a configuration entry
       * @param category the category of the entry
       * @param key the key of the entry
       */
      const std::string& getConfEnt(const std::string &category,
                                    const std::string &key)
        throw (castor::exception::Exception);

    private:

      /**
       * check whether the configuration should be renewed
       */
      bool isStale() throw (castor::exception::Exception);

      /**
       * tries to renew the configuration.
       * That is : take the write lock to do it, check whether it's needed
       * and do it only if needed before releasing the lock
       */
      void tryToRenewConfig() throw (castor::exception::Exception);

      /**
       * gets current timeout value (in seconds)
       * this function does not take any lock while reading the
       * configuration. So it should never be called without holding
       * a read or a write lock
       */
      int getTimeoutNolock() throw (castor::exception::Exception);

      /**
       * renews the configuration
       * this function does not take any lock while renewing the
       * configuration. So it should never be called without holding
       * the write lock
       */
      void renewConfigNolock() throw (castor::exception::Exception);

    private:

      /**
       * fileName to be used when updating the configuration
       */
      std::string m_fileName;

      /**
       * last time we've updated the configuration
       */
      time_t m_lastUpdateTime;

      /**
       * the dictionnary of configuration items
       * actually a dictionnary of ConfCategories, which are dictionnaries of entries
       */
      std::map<std::string, ConfCategory> m_config;

      /**
       * lock to garantee safe access to the configuration, lastUpdateTime and timeout
       */
      pthread_rwlock_t m_lock;

    };

  } // namespace common
} // namespace castor  

#endif /* H_CASTOR_COMMON_HPP */
