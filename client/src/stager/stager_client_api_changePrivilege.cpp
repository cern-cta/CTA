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
 * @(#)$RCSfile: stager_client_api_changePrivilege.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/05/29 07:47:01 $ $Author: sponcec3 $
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

//-----------------------------------------------------------------------------
// >> operator for BWUsers
//-----------------------------------------------------------------------------
std::istream& operator>>(std::istream &s, castor::bwlist::BWUser &u) {
  // set default values
  u.setEuid(-1);
  u.setEgid(-1);
  // Do we have a user id ?
  char c = 0;
  s >> c;
  if (c == ':') {
    // No user id, do we have a group id ?
    c = 0;
    s >> c;
    s.putback(c);
    if (c != ',' and c != 0) {
      int gid = -1;
      s >> gid;
      if (gid == -1) {
        s.clear(std::ios_base::badbit);
      } else {
        u.setEgid(gid);
      }
    }
  } else {
    // Yes, get the uid
    s.putback(c);
    int uid = -1;
    s >> uid;
    if (uid == -1) {
      s.clear(std::ios_base::badbit);
    } else {
      u.setEuid(uid);
      // And do we have a group id ?
      c = 0;
      s >> c;
      if (c == ':') {
        // at least we have a ':', let's see for a value
        c = 0;
        s >> c;
        s.putback(c);
        if (c != ',' && c != 0) {
          int gid = -1;
          s >> gid;
          if (gid == -1) {
            s.clear(std::ios_base::badbit);
          } else {
            u.setEgid(gid);
          }
        }
      }
    }
  }
  return s;
}

//-----------------------------------------------------------------------------
// >> operator for a vector of BWUsers
//-----------------------------------------------------------------------------
std::istream& operator>>(std::istream &s,
                         std::vector<castor::bwlist::BWUser*> &users) {
  while (!s.eof() && !s.bad()) {
    // get one user
    castor::bwlist::BWUser *u = new castor::bwlist::BWUser();
    s >> *u;
    users.push_back(u);
    // check whether there is more
    char c = 0;
    s >> c;
    if (c != ',' && c != 0) {
      s.clear(std::ios_base::badbit);
    }
  }
  return s;
}

//-----------------------------------------------------------------------------
// parseUsers
//-----------------------------------------------------------------------------
void parseUsers(char *susers,
                std::vector<castor::bwlist::BWUser*> &users)
  throw (castor::exception::Exception) {
  std::stringstream s(susers);
  s >> users;
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
// >> operator for a vector of RequestTypes
//-----------------------------------------------------------------------------
std::istream& operator>>(std::istream &s,
                         std::vector<castor::bwlist::RequestType*> &reqTypes) {
  while (!s.eof() && !s.bad()) {
    unsigned int rt = 0;
    s >> rt;
    if (rt == 0) {
      s.clear(std::ios_base::badbit);
      break;
    }
    castor::bwlist::RequestType *r = new castor::bwlist::RequestType();
    r->setReqType(rt);
    reqTypes.push_back(r);
    char c = 0;
    s >> c;
    if (c != ',' && c != 0) {
      s.clear(std::ios_base::badbit);
    }
  }
  return s;
}

//-----------------------------------------------------------------------------
// parseRequestTypes
//-----------------------------------------------------------------------------
void parseRequestTypes(char *sreqTypes,
                       std::vector<castor::bwlist::RequestType*> &reqTypes)
  throw (castor::exception::Exception) {
  std::stringstream s(sreqTypes);
  s >> reqTypes;
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
    castor::BaseObject::initLog("", castor::SVC_NOMSG);

    // Uses a BaseClient to handle the request
    castor::client::BaseClient client(stage_getClientTimeout());
    int ret = setDefaultOption(opts);
    client.setOptions(opts);
    client.setAuthorizationId(); 
    if (ret==-1) { free(opts); }

    // create request
    castor::bwlist::ChangePrivilege req;
    req.setIsGranted(true);
    parseUsers(users, req.users());
    parseRequestTypes(requestTypes, req.requestTypes());

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
