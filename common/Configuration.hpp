/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>
#include <map>

#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/utils/utils.hpp"

namespace cta::common {

    /**
     * represents a category from the CTA configuration file
     */
    typedef std::map<std::string, std::string> ConfCategory;

    /**
     * a class representing the configuration of castor.
     * This configurations is obtained from the local file given in the
     * constructor and will be updated regularly. The time between two
     * updates is taken from the Config/ExpirationDelay entry of the
     * configuration itself and defaults to 5mn if no such entry is found
     */
    class Configuration {

    public:

      /**
       * Private exceptions for this
       */
      CTA_GENERATE_EXCEPTION_CLASS(InvalidConfigEntry);
      CTA_GENERATE_EXCEPTION_CLASS(NoEntry);

    public:

      /**
       * constructor
       * @param fileName the file that should be used to build the configuration
       */
      explicit Configuration(const std::string& fileName);

      /**
       * copy constructor
       * @param other instance of CastorConfiguration class
       */
      Configuration(const Configuration & other);

      /**
       * destructor
       */
      virtual ~Configuration();

      /**
       * assignment operator
       * @param other instance of CastorConfiguration class
       */
      Configuration & operator=(const Configuration & other);

      /**
       * Retrieves a configuration entry.
       *
       * If this method is passed a logger object then it will log the value
       * of the configuration entry together with an indication of whether the
       * value was found in the castor configuration file or whether the
       * specified default value was used instead.
       *
       * @param category the category of the entry
       * @param key the key of the entry
       * @param defaultValue the value to be returned if the configuration entry
       * is not in the configuration file
       * @param log pointer to nullptr or an optional logger object
       */
      const std::string& getConfEntString(const std::string &category,
        const std::string &key, const std::string &defaultValue,
        cta::log::Logger *const log = nullptr);

      /**
       * Retrieves a configuration entry.
       *
       * Besides other possible exceptions, this method throws a
       * cta::exception::NoEntry exception if the specified configuration
       * entry is not in the configuration file.
       *
       * If this method is passed a logger object then this method will log the
       * the value of the configuration entry.
       *
       * @param category the category of the entry
       * @param key the key of the entry
       * @param log pointer to nullptr or an optional logger object
       */
      const std::string& getConfEntString(const std::string &category,
        const std::string &key, cta::log::Logger *const log = nullptr);

      /**
       * Retrieves a configuration entry as an integer.
       *
       * If this method is passed a logger object then it will log the value
       * of the configuration entry together with an indication of whether the
       * value was found in the castor configuration file or whether the
       * specified default value was used instead.
       *
       * @param category category of the configuration parameter
       * @param name category of the configuration parameter
       * @param defaultValue the value to be returned if the configuration entry
       * is not in the configuration file
       * @param log pointer to nullptr or an optional logger object
       * @return the integer value
       */
      template<typename T> T getConfEntInt(const std::string &category,
        const std::string &key, const T defaultValue,
        cta::log::Logger *const log = nullptr)  {
        std::string strValue;
        try {
          strValue = getConfEntString(category, key);
        } catch(cta::exception::Exception &ex) {
          if(nullptr != log) {
            std::list<cta::log::Param> params = {
              cta::log::Param("category", category),
              cta::log::Param("key", key),
              cta::log::Param("value", defaultValue),
              cta::log::Param("source", "DEFAULT")};
            (*log)(log::INFO, "Configuration entry", params);
          }
          return defaultValue;
        }

        if (!utils::isValidUInt(strValue.c_str())) {
          InvalidConfigEntry ex;
          ex.getMessage() << "Failed to get configuration entry " << category <<
            ":" << key << ": Value is not a valid unsigned integer: value=" <<
            strValue;
          throw ex;
        }

        T value;
        std::stringstream ss;
        ss << strValue;
        ss >> value;

        if(nullptr != log) {
          std::list<cta::log::Param> params = {
            cta::log::Param("category", category),
            cta::log::Param("key", key),
            cta::log::Param("value", value),
            cta::log::Param("source", m_fileName)};
          (*log)(log::INFO, "Configuration entry", params);
        }

        return value;
      }

      /**
       * Retrieves a configuration entry as an integer.
       *
       * Besides other possible exceptions, this method throws a
       * cta::exception::NoEntry exception if the specified configuration
       * entry is not in the configuration file.
       *
       * @param category category of the configuration parameter
       * @param name category of the configuration parameter
       * @param log pointer to nullptr or an optional logger object
       * @return the integer value
       */
      template<typename T> T getConfEntInt(const std::string &category,
        const std::string &key, cta::log::Logger *const log = nullptr)  {
        const std::string strValue = getConfEntString(category, key);

        if (!utils::isValidUInt(strValue.c_str())) {
          InvalidConfigEntry ex;
          ex.getMessage() << "Failed to get configuration entry " << category <<
            ":" << key << ": Value is not a valid unsigned integer: value=" <<
            strValue;
          throw ex;
        }

        T value;
        std::stringstream ss;
        ss << strValue;
        ss >> value;

        if(nullptr != log) {
          std::list<cta::log::Param> params = {
            cta::log::Param("category", category),
            cta::log::Param("key", key),
            cta::log::Param("value", value),
            cta::log::Param("source", m_fileName)};
          (*log)(log::INFO, "Configuration entry", params);
        }

        return value;
      }

      /**
       * Retrieves a configuration entry as a boolean.
       *
       * @param category category of the configuration parameter
       * @param name category of the configuration parameter
       * @param defaultValue the value to be returned if the configuration entry
       * is not in the configuration file
       * @return the bool value
       */
      bool getConfEntBool(const std::string &category,
        const std::string &key, bool defaultValue)  {
        std::string strValue;
        try {
          strValue = getConfEntString(category, key);
        } catch(cta::exception::Exception &ex) {
          return defaultValue;
        }

        // check if it's true/false
        if (strValue != "true" && strValue != "false") {
          InvalidConfigEntry ex;
          ex.getMessage() << "Failed to get configuration entry " << category <<
            ":" << key << ": Value is not a valid bool (\"true\" or \"false\"): value=" <<
            strValue;
          throw ex;
        }

        bool value;
        std::istringstream iss(strValue);
        iss >> std::boolalpha >> value;

        return value;
      }


    private:

      /**
       * check whether the configuration should be renewed
       */
      bool isStale() ;

      /**
       * tries to renew the configuration.
       * That is : take the write lock to do it, check whether it's needed
       * and do it only if needed before releasing the lock
       */
      void tryToRenewConfig() ;

      /**
       * gets current timeout value (in seconds)
       * this function does not take any lock while reading the
       * configuration. So it should never be called without holding
       * a read or a write lock
       */
      int getTimeoutNolock() ;

      /**
       * renews the configuration
       * this function does not take any lock while renewing the
       * configuration. So it should never be called without holding
       * the write lock
       */
      void renewConfigNolock() ;

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

} // namespace cta::common
