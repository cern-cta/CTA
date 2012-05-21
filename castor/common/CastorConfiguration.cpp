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

// includes
#include <fstream>
#include <algorithm>
#include "castor/common/CastorConfiguration.hpp"
#include "castor/exception/NoEntry.hpp"

// Global configurations, indexed by original file name
std::map<std::string, castor::common::CastorConfiguration> s_castorConfigs;

//------------------------------------------------------------------------------
// getConfig
//------------------------------------------------------------------------------
castor::common::CastorConfiguration&
castor::common::CastorConfiguration::getConfig(std::string fileName)
  throw (castor::exception::Exception) {
  // do we have this configuration already in cache ?
  if (s_castorConfigs.end() == s_castorConfigs.find(fileName)) {
    // no such configuration. Create it
    s_castorConfigs[fileName] = CastorConfiguration(fileName);
  }
  return s_castorConfigs[fileName];
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::common::CastorConfiguration::CastorConfiguration(std::string fileName)
  throw (castor::exception::Exception) : m_fileName(fileName), m_lastUpdateTime(-1) {
  // create internal read write lock
  int rc = pthread_rwlock_init(&m_lock, NULL);
  if (0 != rc) {
    castor::exception::Exception e(rc);
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
// getConfEnt
//------------------------------------------------------------------------------
const std::string&
castor::common::CastorConfiguration::getConfEnt(const std::string &category,
                                                const std::string &key)
  throw (castor::exception::Exception) {
  // check whether we need to reload the configuration
  checkAndRenewConfig();
  // get read lock
  pthread_rwlock_rdlock(&m_lock);
  // get the category
  Configuration::const_iterator catIt = find(category);
  if (end() == catIt) {
    castor::exception::NoEntry e;
    throw e;
  }
  // get the entry
  ConfCategory::const_iterator entIt = catIt->second.find(key);
  if (catIt->second.end() == entIt) {
    castor::exception::NoEntry e;
    throw e;
  }
  // release read lock
  pthread_rwlock_unlock(&m_lock);
  // return
  return entIt->second;
}

//------------------------------------------------------------------------------
// checkAndRenewConfig
//------------------------------------------------------------------------------
void castor::common::CastorConfiguration::checkAndRenewConfig()
  throw (castor::exception::Exception) {
  // get the timeout
  time_t timeout = 300; // default to 300s = 5mn
  std::string &stimeout = Configuration::operator[]("Config")["ExpirationDelay"];
  if ("" != stimeout) {
    // parse the timeout into an integer
    timeout = atoi(stimeout.c_str());
  } else {
    // set default to 300s = 5mn into the configuration
    Configuration::operator[]("Config")["ExpirationDelay"] = "300";
  }
  // check whether we should renew  
  time_t currentTime = time(0);
  if (currentTime < m_lastUpdateTime + timeout) {
    // no need to renew
    return;  
  }
  // we should probably renew. First take the write lock
  pthread_rwlock_wrlock(&m_lock);
  // now check that we should really renew, because someone may have done it
  // while we waited for the lock
  if (currentTime < m_lastUpdateTime + timeout) {
    // indeed, someone renew it for us, so give up
    pthread_rwlock_unlock(&m_lock);
    return;  
  }
  // now we should really renew
  renewConfig();
  // release the write lock
  pthread_rwlock_unlock(&m_lock);
}

//------------------------------------------------------------------------------
// renewConfig
//------------------------------------------------------------------------------
void castor::common::CastorConfiguration::renewConfig()
  throw (castor::exception::Exception) {
  // reset the config
  clear();
  // open config file
  std::ifstream file(m_fileName.c_str());
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
    Configuration::operator[](category)[key] = value;
  }
  m_lastUpdateTime = time(0);
}
