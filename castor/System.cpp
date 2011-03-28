/******************************************************************************
 *                      System.cpp
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
 * @(#)BaseClient.cpp,v 1.37 $Release$ 2006/02/16 15:56:58 sponcec3
 *
 * A class with static methods for system level utilities.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <arpa/inet.h>

// Local includes
#include "System.hpp"
#include "Cgrp.h"
#include "Cnetdb.h"
#include "common.h"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string castor::System::getHostName() throw (castor::exception::Exception)
{
  // All this to get the hostname, thanks to C !
  int len = 64;
  char* hostname;
  hostname = (char*) calloc(len, 1);
  if (gethostname(hostname, len) < 0) {
    // Test whether error is due to a name too long
    // The errno depends on the glibc version
    if (EINVAL != errno &&
        ENAMETOOLONG != errno) {
      free(hostname);
      castor::exception::Exception e(errno);
      e.getMessage() << "gethostname error";
      throw e;

    }
    // So the name was too long
    while (hostname[len - 1] != 0) {
      len *= 2;
      char *hostnameLonger = (char*) realloc(hostname, len);
      if (0 == hostnameLonger) {
        free(hostname);
        castor::exception::Exception e(ENOMEM);
        e.getMessage() << "Could not allocate memory for hostname";
        throw e;

      }
      hostname = hostnameLonger;
      memset(hostname, 0, len);
      if (gethostname(hostname, len) < 0) {
        // Test whether error is due to a name too long
        // The errno depends on the glibc version
        if (EINVAL != errno &&
            ENAMETOOLONG != errno) {
          free(hostname);
          castor::exception::Exception e(errno);
          e.getMessage() << "Could not get hostname"
                         <<  strerror(errno);
          throw e;
        }
      }
    }
  }
  std::string res(hostname);   // copy the string
  free(hostname);
  return res;
}

//------------------------------------------------------------------------------
// porttoi
//------------------------------------------------------------------------------
int castor::System::porttoi(char* str)
  throw (castor::exception::Exception) {
  char* dp = str;
  errno = 0;
  int iport = strtoul(str, &dp, 0);
  if (*dp != 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "Bad port value." << std::endl;
    throw e;
  }
  if ((iport > 65535) || (iport < 0)) {
    castor::exception::Exception e(errno);
    e.getMessage()
      << "Invalid port value : " << iport
      << ". Must be < 65535 and > 0." << std::endl;
    throw e;
  }
  return iport;
}

//------------------------------------------------------------------------------
// ipAddressToHostName
//------------------------------------------------------------------------------
std::string castor::System::ipAddressToHostname
(unsigned long long ipAddress)
  throw (castor::exception::Exception) {
  std::ostringstream res;
  res << ((ipAddress & 0xFF000000) >> 24) << "."
      << ((ipAddress & 0x00FF0000) >> 16) << "."
      << ((ipAddress & 0x0000FF00) >> 8)  << "."
      << ((ipAddress & 0x000000FF));

  // Resolve the ip address to a hostname
  in_addr_t addr = inet_addr(res.str().c_str());
  hostent *hp = Cgethostbyaddr((char *)&addr, sizeof(addr), AF_INET);
  if (hp == NULL) {
    castor::exception::Exception e(serrno);
    e.getMessage() << "Failed to resolve ipAddress: " << res.str()
		   << " to a hostname" << std::endl;
    throw e;
  }

  return hp->h_name;
}

//------------------------------------------------------------------------------
// switchToCastorSuperuser
//------------------------------------------------------------------------------
void castor::System::switchToCastorSuperuser()
  throw (castor::exception::Exception) {
  struct passwd *stage_passwd;    // password structure pointer
  struct group  *stage_group;     // group structure pointer

  uid_t ruid, euid;               // Original uid/euid
  gid_t rgid, egid;               // Original gid/egid

  // Save original values
  ruid = getuid();
  euid = geteuid();
  rgid = getgid();
  egid = getegid();

  // Get information on generic stage account from password file
  if ((stage_passwd = Cgetpwnam(STAGERSUPERUSER)) == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Castor super user " << STAGERSUPERUSER
                   << " not found in password file";
    throw e;
  }
  // verify existence of its primary group id
  if (Cgetgrgid(stage_passwd->pw_gid) == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Castor super user group does not exist";
    throw e;
  }
  // Get information on generic stage account from group file
  if ((stage_group = Cgetgrnam(STAGERSUPERGROUP)) == NULL) {
    castor::exception::Internal e;
    e.getMessage() << "Castor super user group " << STAGERSUPERGROUP
                   << " not found in group file";
    throw e;
  }
  // Verify consistency
  if (stage_group->gr_gid != stage_passwd->pw_gid) {
    castor::exception::Internal e;
    e.getMessage() << "Inconsistent password file. The group of the "
                   << "castor superuser " << STAGERSUPERUSER
                   << " should be " << stage_group->gr_gid
                   << "(" << STAGERSUPERGROUP << "), but is "
                   << stage_passwd->pw_gid;
    throw e;
  }
  // Undo group privilege
  if (setregid (egid, rgid) < 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to undo group privilege";
    throw e;
  }
  // Undo user privilege
  if (setreuid (euid, ruid) < 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to undo user privilege";
    throw e;
  }
  // set the effective privileges to superuser
  if (setegid(stage_passwd->pw_gid) < 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to set group privileges of Castor Superuser. "
                   << "You may want to check that the suid bit is set properly";
    throw e;
  }
  if (seteuid(stage_passwd->pw_uid) < 0) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to set privileges of Castor Superuser.";
    throw e;
  }
}
