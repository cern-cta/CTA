/******************************************************************************
 *                      RateLimiter.cpp
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
 * @(#)$RCSfile: JobManagerDaemon.cpp,v $ $Revision: 1.24 $ $Release$ $Date: 2009/08/18 09:42:52 $ $Author: waldron $
 *
 * A basic class capable of configuring the maximum number of requests a user
 * can perform. Note: This is a very basic, very simple first implementation
 * based around memcache, there is lots of scope for optimization and
 * improvement! This is ***NOT*** DOS protection.....
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/exception/Internal.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "Cgrp.h"
#include "Cpwd.h"
#include "dlfcn.h"
#include "getconfent.h"
#include "RateLimiter.hpp"
#include "RatingGroup.hpp"
#include <errno.h>
#include <iostream>
#include <math.h>
#include <string>
#include <vector>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::rh::RateLimiter::RateLimiter()
  throw(castor::exception::Exception) :
  m_memc(0),
  m_servers(0) {}

//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::rh::RateLimiter::~RateLimiter()
  throw() {

  // Free server list
  if (m_servers != NULL)
    memcached_server_list_free(m_servers);

  // Free memcached structure
  if (m_memc != NULL)
    memcached_free(m_memc);

  // Free configuration map
  for (std::map<std::string, castor::rh::RatingGroup *>::const_iterator it =
         m_config.begin();
       it != m_config.end();
       it++) {
    delete (*it).second;
  }
}

//-----------------------------------------------------------------------------
// Init
//-----------------------------------------------------------------------------
void castor::rh::RateLimiter::init()
  throw(castor::exception::Exception) {

  // Extract the RH/RateLimitServerList from castor.conf. This variable defines
  // a string of servers that the memcached library should use when storing
  // and retrieving information. Note: If the value is not set then the rate
  // limiting functionality is said to be disabled, we do not assume a default!
  const char *value = getconfent("RH", "RateLimitServerList", 0);
  if (value == NULL) {
    return;
  }

  // Parse the server list extracted above
  m_servers = memcached_servers_parse(value);

  // Create a memcached_st structure, this is essentially a handle for other
  // memcached based functions to use in the future.
  m_memc = memcached_create(NULL);
  if (m_memc == NULL) {
    castor::exception::OutOfMemory ex;
    ex.getMessage() << "Failure in call to memcached_create: "
                    << strerror(ENOMEM);
    throw ex;
  }

  // Push the list of servers into the memcached structure.
  memcached_return rc = memcached_server_push(m_memc, m_servers);
  if (rc != MEMCACHED_SUCCESS) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failure in call to memcached_server_push: "
                    << memcached_strerror(m_memc, rc);
    throw ex;
  }

  // Set the protocol to binary mode, this is necessary to use the
  // memcached increment and decrement methods later.
  rc = memcached_behavior_set(m_memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
  if (rc != MEMCACHED_SUCCESS) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failure in call to "
                    << "memcached_behavior_set(BINARY_PROTOCOL, 1): "
                    << memcached_strerror(m_memc, rc);
    throw ex;
  }

  // The configuration of the thresholds for various users is done using rating
  // groups. A rating group is a collection of users with a threshold limit
  // and a configured response. A user may exist in one and only one rating
  // group!

  // Extract the list of rating group names. Note: we put the results in a
  // vector to be safe from memory leaks!
  std::vector<std::string> groups;
  char **results;
  int  count, i;
  if (!getconfent_multi("RH", "RateLimitGroups", 1, &results, &count)) {
    for (i = 0; i < count; i++) {
      groups.push_back(results[i]);
      free(results[i]);
    }
    free(results);
  }

  // Loop over the users associated to the group. Note: A user must be defined
  // by its username and group represented as strings not numbers!
  unsigned int j;
  for (j = 0; j < groups.size(); j++) {

    // Extract the users value for the rating group
    std::ostringstream option;
    option << groups[j] << "Users";

    if (getconfent_multi("RH", option.str().c_str(), 1, &results, &count)) {
      castor::exception::Exception ex(EINVAL);
      ex.getMessage() << "Missing configuration option RH/" << option.str();
      throw ex;
    }

    // Loop over the users
    for (i = 0; i < count; i++) {
      // Check if the user and group is already listed in another rating group
      std::map<std::string, castor::rh::RatingGroup *>::const_iterator it =
        m_config.find(results[i]);
      if (it != m_config.end()) {
        castor::exception::Exception ex(EINVAL);
        ex.getMessage() << "";
        throw ex;
      }

      // Note: I should really be using a shared_ptr here as it would reduce
      // the amount of needed memory and avoid the need to clean up the map in
      // the destructor!
      m_config[results[i]] = new castor::rh::RatingGroup(groups[j]);
      free(results[i]);
    }
    free(results);
  }
}

//-----------------------------------------------------------------------------
// CheckAndUpdateLimit
//-----------------------------------------------------------------------------
castor::rh::RatingGroup *
castor::rh::RateLimiter::checkAndUpdateLimit(const std::string user,
                                             const std::string group)
  throw (castor::exception::Exception) {

  // Extract the rating group configuration information associated to the user.
  // We first check to see if we have an explicit user:group entry, then
  // *:group, then 'all'. If nothing is found then the user had no rate
  // limiting restrictions.
  std::ostringstream groupName;
  groupName << user << ":" << group;

  std::map<std::string, castor::rh::RatingGroup *>::const_iterator it =
    m_config.find(groupName.str());
  if (it == m_config.end()) {
    groupName.str("");
    groupName << "*:" << group;
    it = m_config.find(groupName.str());
    if (it == m_config.end()) {
      it = m_config.find("all");
    }
  }

  // No entry found?
  if (it == m_config.end()) {
    return NULL;
  }

  // If the rating group response is 'always-accept' then the user is
  // effectively exempt from the checks.
  if ((*it).second->response() == "always-accept") {
    return NULL;
  }

  // Generate a list of keys to lookup in memcache using the multiple key
  // lookup functionality. This is more efficient then looking up keys one by
  // one.
  u_signed64 start = (int)(round((time(NULL) / 10)) * 10);
  uint64_t totalRequests = 0;

  std::vector<std::string> keys;
  unsigned int i;
  for (i = 0; i < ((*it).second->interval() / 10); i++) {
    std::ostringstream key;
    key << "castor.rh.ratelimit."
        << user  << ":"
        << group << ":"
        << start - (i * 10);
    keys.push_back(key.str());
  }

  // If we have no keys then there is no need to query memcache so we take our
  // final decision.
  if (keys.size()) {

    // Construct an array which will contain the length of each of the strings
    // in the keys vector. Also, to interface with the memcached C API, we need
    // to convert the vector of std::string's to a vector of char *.
    // Refer to: memcached.hpp
    std::vector<const char *> keylist;
    std::vector<size_t> lengths;

    for (std::vector<std::string>::const_iterator iter = keys.begin();
         iter != keys.end();
         iter++) {
      keylist.push_back(const_cast<char *>((*iter).c_str()));
      lengths.push_back((*iter).length());
    }

    // Retrieve the keys from memcache
    memcached_return rc =
      memcached_mget(m_memc, &keylist[0], &lengths[0], keylist.size());
    if (rc != MEMCACHED_SUCCESS) {
      castor::exception::Exception ex(SEINTERNAL);
      ex.getMessage() << "Failure in call to memcached_mget: "
                      << memcached_strerror(m_memc, rc);
      throw ex;
    }

    // Allocate memory for the results structures
    memcached_result_st results_obj;
    memcached_result_st *results
      = memcached_result_create(m_memc, &results_obj);
    if (results == NULL) {
      castor::exception::OutOfMemory ex;
      ex.getMessage() << "Failure in call to memcached_result_create";
      throw ex;
    }

    // Loop over all results until no results are returned or an error is
    // encountered.
    while ((results = memcached_fetch_result(m_memc, &results_obj, &rc))) {
      totalRequests += atoi(memcached_result_value(results));
      if (totalRequests > (*it).second->nbRequests()) {
        rc = MEMCACHED_END;
        break;
      }
    }
    if (rc != MEMCACHED_END) {
      castor::exception::Exception ex(SEINTERNAL);
      ex.getMessage() << "Failure in call to memcached_result_value: "
                      << memcached_strerror(m_memc, rc);
      throw ex;
    }
    memcached_result_free(&results_obj);

    if (totalRequests > (*it).second->nbRequests()) {
      return (*it).second;
    }
  }

  // Generate the initial key to lookup in memcache. i.e. the bucket that
  // represents the time period referring to now.
  std::ostringstream key;
  key << "castor.rh.ratelimit."
      << user  << ":"
      << group << ":"
      << start;

  // Update the cache to reflect the new request, this call to memcache
  // requires the BINARY protocol support and memcached >= 1.3
  uint64_t currentRequests = 0;
  memcached_return rc =
    memcached_increment_with_initial(m_memc, key.str().c_str(),
                                     strlen(key.str().c_str()), 1, 1,
                                     (*it).second->interval(), &currentRequests);
  if (rc != MEMCACHED_SUCCESS) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Failure in call to memcached_increment_with_initial: "
                    << memcached_strerror(m_memc, rc);
    throw ex;
  }

  totalRequests += currentRequests;
  if (totalRequests > (*it).second->nbRequests()) {
    return (*it).second;
  }

  return NULL;
}

//-----------------------------------------------------------------------------
// checkAndUpdateLimit
//-----------------------------------------------------------------------------
castor::rh::RatingGroup *
castor::rh::RateLimiter::checkAndUpdateLimit(const int euid,
                                             const int egid)
  throw (castor::exception::Exception) {

  // Resolve the uid to a user name
  passwd *pwd = Cgetpwuid(euid);
  if (pwd == NULL) {
    return NULL;
  }

  // Resolve the gid to a group name
  group *grp = Cgetgrgid(egid);
  if (grp == NULL) {
    return NULL;
  }

  return checkAndUpdateLimit(pwd->pw_name, grp->gr_name);
}
