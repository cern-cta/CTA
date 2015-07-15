/******************************************************************************
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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/exception/NoEntry.hpp"

#include <algorithm>
#include <fstream>
#include <errno.h>

// Global configurations, indexed by original file name
std::map<std::string, castor::common::CastorConfiguration> s_castorConfigs;

// Lock for the global configuration
static pthread_mutex_t s_globalConfigLock = PTHREAD_MUTEX_INITIALIZER;

//------------------------------------------------------------------------------
// getConfig
//------------------------------------------------------------------------------
castor::common::CastorConfiguration&
castor::common::CastorConfiguration::getConfig(std::string fileName)
   {
  // This method is non thread safe, and is protected by the s_globalConfigLock
  // lock
  int rc = pthread_mutex_lock(&s_globalConfigLock);
  if (0 != rc) {
    castor::exception::Exception e(rc);
    e.getMessage() << "Failed to get CASTOR configuration:"
      " Failed to take a lock on s_globalConfigLock";
    throw e;
  }
  // take care to catch all exceptions so that the lock is not leaked
  try {
    // do we have this configuration already in cache ?
    if (s_castorConfigs.end() == s_castorConfigs.find(fileName)) {
      // no such configuration. Create it
      s_castorConfigs.insert(std::make_pair(fileName,
        CastorConfiguration(fileName)));
    }
    // we can now release the lock. Concurrent read only access is ok.
    pthread_mutex_unlock(&s_globalConfigLock);
    return s_castorConfigs[fileName];
  } catch (...) {
    // release the lock
    pthread_mutex_unlock(&s_globalConfigLock);
    // rethrow
    throw;
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::common::CastorConfiguration::CastorConfiguration(std::string fileName)
   : m_fileName(fileName),
  m_lastUpdateTime(0) {
  // create internal r/w lock
  int rc = pthread_rwlock_init(&m_lock, NULL);
  if (0 != rc) {
    castor::exception::Exception e(rc);
    e.getMessage() << "CastorConfiguration constructor Failed"
      ": Failed to create internal r/w lock";
    throw e;
  }
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
castor::common::CastorConfiguration::CastorConfiguration(
  const CastorConfiguration & other) :
  m_fileName(other.m_fileName), m_lastUpdateTime(other.m_lastUpdateTime),
  m_config(other.m_config) {
  // create a new internal r/w lock
  int rc = pthread_rwlock_init(&m_lock, NULL);
  if (0 != rc) {
    castor::exception::Exception e(rc);
    e.getMessage() << "CastorConfiguration copy constructor failed"
      ": Failed to create a new internal r/w lock";
    throw e;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::common::CastorConfiguration::~CastorConfiguration() {
  // relase read write lock
  pthread_rwlock_destroy(&m_lock);
}

//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
castor::common::CastorConfiguration &
  castor::common::CastorConfiguration::operator=(
    const castor::common::CastorConfiguration & other)
   {
  m_fileName = other.m_fileName;
  m_lastUpdateTime = other.m_lastUpdateTime;
  m_config = other.m_config;
  // create a new internal r/w lock
  int rc = pthread_rwlock_init(&m_lock, NULL);
  if (0 != rc) {
    castor::exception::Exception e(rc);
    e.getMessage() << "Assignment operator of CastorConfiguration object failed"
      ": Failed to create a new internal r/w lock";
    throw e;
  }
  return *this;
}

//------------------------------------------------------------------------------
// getConfEntString
//------------------------------------------------------------------------------
const std::string& castor::common::CastorConfiguration::getConfEntString(
  const std::string &category, const std::string &key,
  const std::string &defaultValue, log::Logger *const log) {
  try {
    if (isStale()) {
      tryToRenewConfig();
    }
    // get read lock
    int rc = pthread_rwlock_rdlock(&m_lock);
    if (0 != rc) {
      castor::exception::Exception e(rc);
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
          log::Param params[] = {   
            log::Param("category", category),
            log::Param("key", key),
            log::Param("value", entIt->second),
            log::Param("source", m_fileName)};
          (*log)(LOG_INFO, "Configuration entry", params);
        }
        return entIt->second;
      }
    }
    // no entry found
    if(NULL != log) {
      log::Param params[] = {
        log::Param("category", category),
        log::Param("key", key),
        log::Param("value", defaultValue),
        log::Param("source", "DEFAULT")};
      (*log)(LOG_INFO, "Configuration entry", params);
    }
    // Unlock and return default
    pthread_rwlock_unlock(&m_lock);
  } catch (castor::exception::Exception ex) {
    // exception caught : Unlock and return default
    pthread_rwlock_unlock(&m_lock);
    // log the exception
    if(NULL != log) {
      log::Param params[] = {
        log::Param("category", category),
        log::Param("key", key),
        log::Param("value", defaultValue),
        log::Param("source", "DEFAULT")};
      (*log)(LOG_INFO, "Configuration entry", params);
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
const std::string& castor::common::CastorConfiguration::getConfEntString(
  const std::string &category, const std::string &key, log::Logger *const log) {
  // check whether we need to reload the configuration
  if (isStale()) {
    tryToRenewConfig();
  }
  // get read lock
  int rc = pthread_rwlock_rdlock(&m_lock);
  if (0 != rc) {
    castor::exception::Exception e(rc);
    e.getMessage() << "Failed to get configuration entry " << category << ":"
      << key << ": Failed to get read lock";
    throw e;
  }
  // get the entry
  try {
    std::map<std::string, ConfCategory>::const_iterator catIt =
      m_config.find(category);
    if (m_config.end() == catIt) {
      castor::exception::NoEntry e;
      e.getMessage() << "Failed to get configuration entry " << category << ":"
        << key << ": Failed to find " << category << " category";
      throw e;
    }
    // get the entry
    ConfCategory::const_iterator entIt = catIt->second.find(key);
    if (catIt->second.end() == entIt) {
      castor::exception::NoEntry e;
      e.getMessage() << "Failed to get configuration entry " << category << ":"
        << key << ": Failed to find " << key << " key";
      throw e;
    }

    if(NULL != log) {
      log::Param params[] = {
        log::Param("category", category),
        log::Param("key", key),
        log::Param("value", entIt->second),
        log::Param("source", m_fileName)};
      (*log)(LOG_INFO, "Configuration entry", params);
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
bool castor::common::CastorConfiguration::isStale()
   {
  // get read lock
  int rc = pthread_rwlock_rdlock(&m_lock);
  if (0 != rc) {
    castor::exception::Exception e(rc);
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
void castor::common::CastorConfiguration::tryToRenewConfig()
   {
  // we should probably renew. First take the write lock.
  int rc = pthread_rwlock_wrlock(&m_lock);
  if (0 != rc) {
    castor::exception::Exception e(rc);
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
int castor::common::CastorConfiguration::getTimeoutNolock()
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
void castor::common::CastorConfiguration::renewConfigNolock()
   {
  // reset the config
  m_config.clear();

  // try to open the configuration file, throwing an exception if there is a
  // failure
  std::ifstream file(m_fileName.c_str());
  if(file.fail()) {
    castor::exception::Exception ex(EIO);
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
