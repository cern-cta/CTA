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
     * represents a CASTOR configuration
     */
    typedef std::map<std::string, ConfCategory> Configuration;

    /**
     * a class representing the configuration of castor.
     * This configurations is obtained from the local file given in the
     * constructor and will be updated regularly. The time between two
     * updates is taken from the Config/ExpirationDelay entry of the
     * configuration itself and defaults to 5mn if no such entry is found
     *
     * Objects of this class behave like duoble level dictionnaries. This
     * means that you can look for entry A/B using obj["A"]["B"] or loop
     * over the entries of a given category using an iterator in obj["A"]
     */
    class CastorConfiguration : public Configuration {

    public:

      /**
       * static method for getting a given configuration
       * @param fileName the name of the file to be used for filling this configuration
       */
      static CastorConfiguration& getConfig(const std::string filename)
        throw (castor::exception::Exception);
        
    public:
      
      /**
       * constructor
       * @param fileName the file that should be used to build the configuration
       */
      CastorConfiguration(std::string fileName = "/etc/castor/castor.conf")
      throw (castor::exception::Exception);

      /**
       * destrcutor
       */
      virtual ~CastorConfiguration();

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
       * checks whether we should update our configuration.
       * If yes, do the update
       */
      void checkAndRenewConfig() throw (castor::exception::Exception);

      /**
       * renew the configuration. Should be called only while
       * holding the write lock
       */
      void renewConfig() throw (castor::exception::Exception);

    private:

      /**
       * the configuration itself
       */
      Configuration m_config;

      /**
       * fileName to be used when updating the configuration
       */
      std::string m_fileName;

      /**
       * last time we've updated the configuration
       */
      time_t m_lastUpdateTime;

      /**
       * lock to garantee safe access to the configuration
       */
      pthread_rwlock_t m_lock;

    };

  } // namespace common
} // namespace castor  

#endif /* H_CASTOR_COMMON_HPP */
