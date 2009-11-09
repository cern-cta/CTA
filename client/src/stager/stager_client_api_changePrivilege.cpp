/******************************************************************************
 *                      stager_client_api_changePrivilege.cpp
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
 * @(#)$RCSfile: stager_client_api_changePrivilege.cpp,v $ $Revision: 1.7 $ $Release$ $Date: 2009/07/13 06:22:08 $ $Author: waldron $
 *
 * api to handle privileges i.e. modify the black and white list
 *
 * @author sponcec3
 *****************************************************************************/

// includes
#include "serrno.h"
#include "stager_client_api.h"
#include "stager_client_api_common.hpp"
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/BasicResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/bwlist/ChangePrivilege.hpp"
#include "castor/bwlist/BWUser.hpp"
#include "castor/bwlist/RequestType.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include <sstream>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <string.h>

//-----------------------------------------------------------------------------
// getUserId
//-----------------------------------------------------------------------------
void getUserId(std::string name, castor::bwlist::BWUser *u)
  throw (castor::exception::InvalidArgument) {
  // empty user name means any and is thus mapped to -1
  if (name.size() == 0) {
    u->setEuid(-1);
    u->setEgid(-1);
    return;
  }
  // if the user is specified as a number use it as is.
  char *dp  = NULL;
  long euid = strtol(name.c_str(), &dp, 10);
  if (*dp == 0) {
    u->setEuid(euid);
    u->setEgid(-1);
    return;
  }
  // get user id
  struct passwd *pass = getpwnam(name.c_str());
  if (0 == pass) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unknown user " << name;
    throw e;
  }
  u->setEuid(pass->pw_uid);
  u->setEgid(pass->pw_gid);
}

//-----------------------------------------------------------------------------
// getGroupId
//-----------------------------------------------------------------------------
void getGroupId(std::string name, castor::bwlist::BWUser *u)
  throw (castor::exception::InvalidArgument) {
  // empty group name, just ignore
  if (name.size() == 0) {
    return;
  }
  // if the group is specified as a number use it as is.
  char *dp  = NULL;
  long egid = strtol(name.c_str(), &dp, 10);
  if (*dp == 0) {
    u->setEgid(egid);
    return;
  }
  // get group id
  struct group *grp = getgrnam(name.c_str());
  if (0 == grp) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unknown group " << name;
    throw e;
  }
  u->setEgid(grp->gr_gid);
}

//-----------------------------------------------------------------------------
// getRequestTypeId
//-----------------------------------------------------------------------------
int getRequestTypeId(std::string type)
  throw (castor::exception::InvalidArgument) {
  // empty type name means any and is thus mapped to 0
  if (type.size() == 0) return 0;
  // get type id, here we go for a dummy O(n) listing
  for (unsigned int i = 1; i < castor::ObjectsIdsNb; i++) {
    if (!strcasecmp(type.c_str(), castor::ObjectsIdStrings[i])) {
      return i;
    }
  }
  castor::exception::InvalidArgument e;
  e.getMessage() << "Unknown requestType " << type;
  throw e;
}

//-----------------------------------------------------------------------------
// parseUsers
//-----------------------------------------------------------------------------
void parseUsers(char *susers,
                castor::bwlist::ChangePrivilege * req,
                std::vector<castor::bwlist::BWUser*> &users)
  throw (castor::exception::Exception) {
  // check for empty user list
  if (strlen(susers) == 0) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Empty list of users, please specify one\nIn order target all users, please use ':'";
    throw e;
  }
  // parse the list
  std::stringstream s(susers);
  while (!s.eof() && ! s.bad()) {
    // get next couple user[:group]
    std::string couple;
    getline(s, couple, ',');
    if (s.bad()) break;
    // default values
    castor::bwlist::BWUser *u = new castor::bwlist::BWUser();
    // split it
    try {
      std::string::size_type colonPos = couple.find_first_of(':');
      if (colonPos == std::string::npos) {
        getUserId(couple, u);
      } else {
        getUserId(couple.substr(0, colonPos), u);
        getGroupId(couple.substr(colonPos + 1), u);
      }
      if (u->euid() && (u->egid() == -1)) {
        castor::exception::InvalidArgument e;
        e.getMessage() << "Unable to determine group for uid: " << u->euid();
        throw e;
      }
      u->setRequest(req);
      users.push_back(u);
    } catch (castor::exception::InvalidArgument e) {
      delete u;
      throw e;
    }
  }
  // cleanup in case things went bad
  if (s.bad()) {
    for (std::vector<castor::bwlist::BWUser*>::const_iterator it =
           users.begin();
         it != users.end();
         it++) {
      delete (*it);
    }
    users.clear();
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unable to parse users string";
    throw e;
  }
}

//-----------------------------------------------------------------------------
// parseRequestTypes
//-----------------------------------------------------------------------------
void parseRequestTypes(char *sreqTypes,
                       castor::bwlist::ChangePrivilege * req,
                       std::vector<castor::bwlist::RequestType*> &reqTypes)
  throw (castor::exception::Exception) {
  std::stringstream s(sreqTypes);
  while (!s.eof() && ! s.bad()) {
    // get next type
    std::string type;
    getline(s, type, ',');
    if (s.bad()) break;
    // convert it to integer
    castor::bwlist::RequestType *r = new castor::bwlist::RequestType();
    try {
      r->setReqType(getRequestTypeId(type));
      r->setRequest(req);
      reqTypes.push_back(r);
    } catch (castor::exception::InvalidArgument e) {
      delete r;
      throw e;
    }
  }
  // cleanup in case things went bad
  if (s.bad()) {
    for (std::vector<castor::bwlist::RequestType*>::const_iterator it =
           reqTypes.begin();
         it != reqTypes.end();
         it++) {
      delete (*it);
    }
    reqTypes.clear();
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unable to parse request types string";
    throw e;
  }
}

//-----------------------------------------------------------------------------
// stage_changePrivilege
//-----------------------------------------------------------------------------
void stage_changePrivilege(char* users,
                           char* requestTypes,
                           struct stage_options* opts,
                           bool isAdd) {
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(stage_getClientTimeout());
  int ret = setDefaultOption(opts);
  client.setOptions(opts);
  client.setAuthorizationId();
  if (ret == -1) {
    free(opts);
  }

  // create request
  castor::bwlist::ChangePrivilege req;
  req.setIsGranted(isAdd);
  parseUsers(users, &req, req.users());
  parseRequestTypes(requestTypes, &req, req.requestTypes());

  // Send request
  castor::client::BasicResponseHandler rh;
  client.sendRequest(&req, &rh);
}

//-----------------------------------------------------------------------------
// stage_addPrivilege
//-----------------------------------------------------------------------------
EXTERN_C int DLL_DECL stage_addPrivilege(char* users,
                                         char* requestTypes,
                                         struct stage_options* opts) {
  try {
    stage_changePrivilege(users, requestTypes, opts, true);
  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stager_errmsg("stage_addPrivilege", (e.getMessage().str().c_str()));
    return -1;
  }
  return 0;
}

//-----------------------------------------------------------------------------
// stage_removePrivilege
//-----------------------------------------------------------------------------
EXTERN_C int DLL_DECL stage_removePrivilege(char* users,
                                            char* requestTypes,
                                            struct stage_options* opts) {
  try {
    stage_changePrivilege(users, requestTypes, opts, false);
  } catch (castor::exception::Exception e) {
    serrno = e.code();
    stager_errmsg("stage_removePrivilege", (e.getMessage().str().c_str()));
    return -1;
  }
  return 0;
}
