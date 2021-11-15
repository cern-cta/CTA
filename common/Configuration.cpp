/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "Configuration.hpp"
#include "common/exception/Errnum.hpp"

#include <algorithm>
#include <fstream>
#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::Configuration::Configuration(const std::string& fileName)
  : m_fileName(fileName),
    m_lastUpdateTime(0) {
  // create internal r/w lock
  int rc = pthread_rwlock_init(&m_lock, NULL);
  if (0 != rc) {
    cta::exception::Errnum e(rc);
    e.getMessage() << "CastorConfiguration constructor Failed"
      ": Failed to create internal r/w lock";
    throw e;
  }
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
cta::common::Configuration::Configuration(
  const Configuration & other) :
  m_fileName(other.m_fileName), m_lastUpdateTime(other.m_lastUpdateTime),
  m_config(other.m_config) {
  // create a new internal r/w lock
  int rc = pthread_rwlock_init(&m_lock, NULL);
  if (0 != rc) {
    cta::exception::Errnum e(rc);
    e.getMessage() << "CastorConfiguration copy constructor failed"
      ": Failed to create a new internal r/w lock";
    throw e;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::Configuration::~Configuration() {
  // relase read write lock
  pthread_rwlock_destroy(&m_lock);
}

//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
cta::common::Configuration &
  cta::common::Configuration::operator=(
    const cta::common::Configuration & other)
   {
  m_fileName = other.m_fileName;
  m_lastUpdateTime = other.m_lastUpdateTime;
  m_config = other.m_config;
  // create a new internal r/w lock
  int rc = pthread_rwlock_init(&m_lock, NULL);
  if (0 != rc) {
    cta::exception::Errnum e(rc);
    e.getMessage() << "Assignment operator of CastorConfiguration object failed"
      ": Failed to create a new internal r/w lock";
    throw e;
  }
  return *this;
}

//------------------------------------------------------------------------------
// getConfEntString
//------------------------------------------------------------------------------
const std::string& cta::common::Configuration::getConfEntString(
  const std::string &category, const std::string &key,
  const std::string &defaultValue, cta::log::Logger *const log) {
  try {
    if (isStale()) {
      tryToRenewConfig();
    }
    // get read lock
    int rc = pthread_rwlock_rdlock(&m_lock);
    if (0 != rc) {
      cta::exception::Errnum e(rc);
      e.getMessage() << "Failed to get configuration entry " << category << ":"
                     << key << ": Failed to get read lock";
      throw e;
    }
    // get the entry
    std::map<std::string, ConfCategory>::const_iterator catIt = m_config.find(category);
    if (m_config.end() != catIt) {
      // get the entry
      ConfCategory::const_iterator entIt = catIt->second.find(key);
      if (catIt->second.end() != entIt) {
        // release the lock
        pthread_rwlock_unlock(&m_lock);
        if(NULL != log) {
          std::list<cta::log::Param> params = {
            cta::log::Param("category", category),
            cta::log::Param("key", key),
            cta::log::Param("value", entIt->second),
            cta::log::Param("source", m_fileName)};
          (*log)(log::INFO, "Configuration entry", params);
        }
        return entIt->second;
      }
    }
    // no entry found
    if(NULL != log) {
      std::list<cta::log::Param> params = {
        cta::log::Param("category", category),
        cta::log::Param("key", key),
        cta::log::Param("value", defaultValue),
        cta::log::Param("source", "DEFAULT")};
      (*log)(log::INFO, "Configuration entry", params);
    }
    // Unlock and return default
    pthread_rwlock_unlock(&m_lock);
  } catch (cta::exception::Exception &) {
    // exception caught : Unlock and return default
    pthread_rwlock_unlock(&m_lock);
    // log the exception
    if(NULL != log) {
      std::list<cta::log::Param> params = {
        cta::log::Param("category", category),
        cta::log::Param("key", key),
        cta::log::Param("value", defaultValue),
        cta::log::Param("source", "DEFAULT")};
      (*log)(log::INFO, "Configuration entry", params);
    }
  } catch (...) {
    // release the lock
    pthread_rwlock_unlock(&m_lock);
    throw;
  }
  return defaultValue;
}

//------------------------------------------------------------------------------
// getConfEntString
//------------------------------------------------------------------------------
const std::string& cta::common::Configuration::getConfEntString(
  const std::string &category, const std::string &key, cta::log::Logger *const log) {
  // check whether we need to reload the configuration
  if (isStale()) {
    tryToRenewConfig();
  }
  // get read lock
  int rc = pthread_rwlock_rdlock(&m_lock);
  if (0 != rc) {
    cta::exception::Errnum e(rc);
    e.getMessage() << "Failed to get configuration entry " << category << ":"
      << key << ": Failed to get read lock";
    throw e;
  }
  // get the entry
  try {
    std::map<std::string, ConfCategory>::const_iterator catIt =
      m_config.find(category);
    if (m_config.end() == catIt) {
      NoEntry e;
      e.getMessage() << "Failed to get configuration entry " << category << ":"
        << key << ": Failed to find " << category << " category";
      throw e;
    }
    // get the entry
    ConfCategory::const_iterator entIt = catIt->second.find(key);
    if (catIt->second.end() == entIt) {
      NoEntry e;
      e.getMessage() << "Failed to get configuration entry " << category << ":"
        << key << ": Failed to find " << key << " key";
      throw e;
    }

    if(NULL != log) {
      std::list<cta::log::Param> params = {
        cta::log::Param("category", category),
        cta::log::Param("key", key),
        cta::log::Param("value", entIt->second),
        cta::log::Param("source", m_fileName)};
      (*log)(log::INFO, "Configuration entry", params);
    }

    // release the lock
    pthread_rwlock_unlock(&m_lock);
    return entIt->second;
  } catch (...) {
    // release the lock
    pthread_rwlock_unlock(&m_lock);
    throw;
  }
}

//------------------------------------------------------------------------------
// isStale
//------------------------------------------------------------------------------
bool cta::common::Configuration::isStale()
   {
  // get read lock
  int rc = pthread_rwlock_rdlock(&m_lock);
  if (0 != rc) {
    cta::exception::Errnum e(rc);
    e.getMessage() << "Failed to determine if CASTOR configuration is stale"
      ": Failed to get read lock";
    throw e;
  }
  try {
    // get the timeout
    int timeout = getTimeoutNolock();
    // release the lock
    pthread_rwlock_unlock(&m_lock);
    // return whether we should renew
    return time(0) > m_lastUpdateTime + timeout;
  } catch (...) {
    // release the lock
    pthread_rwlock_unlock(&m_lock);
    throw;
  }
}

//------------------------------------------------------------------------------
// tryToRenewConfig
//------------------------------------------------------------------------------
void cta::common::Configuration::tryToRenewConfig()
   {
  // we should probably renew. First take the write lock.
  int rc = pthread_rwlock_wrlock(&m_lock);
  if (0 != rc) {
    cta::exception::Errnum e(rc);
    e.getMessage() << "Failed to renew configuration cache"
      ": Failed to take write lock";
    throw e;
  }
  // now check that we should really renew, because someone may have done it
  // while we waited for the lock
  try {
    if (time(0) > m_lastUpdateTime + getTimeoutNolock()) {
      // now we should really renew
      renewConfigNolock();
    }
  } catch (...) {
    // release the lock
    pthread_rwlock_unlock(&m_lock);
    throw;
  }
  // release the lock
  pthread_rwlock_unlock(&m_lock);
  return;
}

//------------------------------------------------------------------------------
// getTimeoutNolock
//------------------------------------------------------------------------------
int cta::common::Configuration::getTimeoutNolock()
   {
  // start with the default (300s = 5mn)
  int timeout = 300;
  // get value from config
  std::map<std::string, ConfCategory>::const_iterator catIt =
    m_config.find("Config");
  if (m_config.end() != catIt) {
    ConfCategory::const_iterator entIt = catIt->second.find("ExpirationDelay");
    if (catIt->second.end() != entIt) {
      // parse the timeout into an integer
      timeout = atoi(entIt->second.c_str());
    }
  }
  // return timeout
  return timeout;
}

//------------------------------------------------------------------------------
// renewConfigNolock
//------------------------------------------------------------------------------
void cta::common::Configuration::renewConfigNolock()
   {
  // reset the config
  m_config.clear();

  // try to open the configuration file, throwing an exception if there is a
  // failure
  std::ifstream file(m_fileName.c_str());
  if(file.fail()) {
    cta::exception::Errnum ex(EIO);
    ex.getMessage() << __FUNCTION__ << " failed"
      ": Failed to open file"
      ": m_fileName=" << m_fileName;
    throw ex;
  }

  std::string line;
  while(std::getline(file, line)) {
    // get rid of potential tabs
    std::replace(line.begin(),line.end(),'\t',' ');
    // get the category
    std::istringstream sline(line);
    std::string category;
    if (!(sline >> category)) continue; // empty line
    if (category[0] == '#') continue;   // comment
    // get the key
    std::string key;
    if (!(sline >> key)) continue;      // no key on line
    if (key[0] == '#') continue;        // key commented
    // get and store value
    while (sline.get() == ' '){}; sline.unget(); // skip spaces
    std::string value;
    std::getline(sline, value, '#');
    value.erase(value.find_last_not_of(" \n\r\t")+1); // right trim
    m_config[category][key] = value;
  }
  m_lastUpdateTime = time(0);
}
