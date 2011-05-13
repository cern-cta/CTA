/******************************************************************************
 *                      enterPriority.cpp
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
 * @(#)$RCSfile: enterPriority.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2009/07/13 06:22:09 $ $Author: waldron $
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <iostream>
#include <castor/stager/SvcClass.hpp>
#include <castor/stager/IStagerSvc.hpp>
#include <castor/Services.hpp>
#include <castor/BaseAddress.hpp>
#include <castor/Constants.h>
#include <Cgetopt.h>
#include <castor/BaseObject.hpp>
#include <pwd.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>

static struct Coptions longopts[] = {
  { "help",     NO_ARGUMENT,       NULL, 'h'},
  { "uid",      REQUIRED_ARGUMENT, NULL, 'u'},
  { "user",      REQUIRED_ARGUMENT, NULL, 'U'},
  { "gid",      REQUIRED_ARGUMENT, NULL, 'g'},
  { "group",      REQUIRED_ARGUMENT, NULL, 'G'},
  { "priority", REQUIRED_ARGUMENT, NULL, 'p'},
  { NULL,       0,                 NULL,  0 }
};

void usage(char *cmd) {
  std::cout << "Usage : \n"
	    << cmd << " [-h] -u/--uid uid -g/--gid gid -p/--priority priority"<<std::endl;
  std::cout << cmd << " [-h] -U/--user username -G/--group groupname -p/--priority priority"<< std::endl;
}

int main(int argc, char *argv[]) {
  int muid = -1;
  int mgid = -1;
  int muser=-1;
  int mgroup=-1;
  int mpriority = -1;

  char* progName = argv[0];

  // Deal with options
  Coptind = 1;
  Copterr = 1;
  int ch;
  char *invptr;

  while ((ch = Cgetopt_long(argc, argv, "hu:U:g:G:p:", longopts, NULL)) != EOF) {
    switch (ch) {
    case 'h':
      usage(progName);
      return 1;
    case 'u':
      muid = strtol (Coptarg, &invptr, 10);
      if ('\0' != *invptr) {
        std::cerr <<" Invalid uid: " << Coptarg << std::endl;
        usage(progName);
        return 1;
      } 
      if ( muid < 500 ) {
        std::cerr <<" uid <500 is not allowed: " << muid << std::endl;
        usage(progName);
        return 1;
      }
      break;
    case 'U':
    {
      struct passwd *pass = getpwnam(Coptarg);
      if (0 == pass) {
	  std::cerr <<" Not existing user" << std::endl;
	  usage(progName);
	  return 1;
      }
      muser = pass->pw_uid;
      if ( muser < 500 ) {
        std::cerr <<" uid <500 is not allowed: " << muser << std::endl;
        usage(progName);
        return 1;
      }
      break;
    }
    case 'g':
      mgid = strtol (Coptarg, &invptr, 10);
      if ('\0' != *invptr) {
        std::cerr <<" Invalid gid: " << Coptarg << std::endl;
        usage(progName);
        return 1;
      }
      if ( mgid < 500 ) {
        std::cerr <<" gid <500 is not allowed: " << mgid << std::endl;
        usage(progName);
        return 1;
      }
      break;
    case 'G':
    {
      struct group *grp = getgrnam(Coptarg);
      if (grp == 0){
        std::cerr << " Not existing group." << std::endl;
	usage(progName);
	return 1;
      }
      mgroup = grp->gr_gid;
      if ( mgroup < 500 ) {
        std::cerr <<" gid <500 is not allowed: " << mgroup << std::endl;
        usage(progName);
        return 1;
      }

      break;
    }
    case 'p':
      mpriority = strtol (Coptarg, &invptr, 10);
      if ('\0' != *invptr) {
        std::cerr <<" Invalid priority: " << Coptarg << std::endl;
        usage(progName);
        return 1; 
      }
      break;
    default:
      usage(progName);
      return 1;
    }
  }

  // Check parameters

  if ((mpriority < 0) || (muid < 0 && muser < 0 )|| (mgid < 0 && mgroup < 0)) {
    std::cerr << " Uid, gid and/or priority options missing" << std::endl;
    usage(progName);
    return 1;
  }
  if ((muid>=0 && muser>=0) || (mgid>=0 && mgroup>=0)){
    std::cerr << " Invalid syntax"<<std::endl;
    usage(progName);
    return 1;
  }

  muid=muid>0?muid:muser;
  mgid=mgid>0?mgid:mgroup;

  try {
    // retrieve a Services object
    castor::Services* svcs = castor::BaseObject::services();
    // retrieve the DB service
    castor::IService* isvc = svcs->service("DB", castor::SVC_DBSTAGERSVC);
    if (0 == isvc) {
      std::cerr << "Unable to retrieve Stager Service." << std::endl
                << "Please check your configuration." << std::endl;
      exit(1);
    }

    castor::stager::IStagerSvc* stgsvc =
      dynamic_cast<castor::stager::IStagerSvc*>(isvc);

    // set the priority
    stgsvc->enterPriority(muid, mgid, mpriority);

  } catch (castor::exception::Exception& e) {
    if (e.code() == 1) {
      std::cerr << "Priority for uid: " << muid << " and gid: " << mgid
		<< " already exists" << std::endl;
    } else {
      std::cerr << "Caught exception :\n"
		<< e.getMessage().str() << std::endl;
    }
    exit(1);
  }
  return 0;
}
