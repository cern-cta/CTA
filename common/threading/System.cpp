/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>

// Local includes
#include "System.hpp"
#include "common/exception/Errnum.hpp"

#define STAGERSUPERGROUP "st"
#define STAGERSUPERUSER "stage"


//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string cta::System::getHostName() 
{
  // All this to get the hostname, thanks to C !
  int len = 64;
  char* hostname;
  hostname = (char*) calloc(len, 1);
  if (0 == hostname) {
    OutOfMemory ex;
    ex.getMessage() << "Could not allocate hostname with length " << len;
    throw ex;
  }
  if (gethostname(hostname, len) < 0) {
    // Test whether error is due to a name too long
    // The errno depends on the glibc version
    if (EINVAL != errno &&
        ENAMETOOLONG != errno) {
      free(hostname);
      cta::exception::Errnum e(errno);
      e.getMessage() << "gethostname error";
      throw e;
    }
    // So the name was too long
    while (hostname[len - 1] != 0) {
      len *= 2;
      char *hostnameLonger = (char*) realloc(hostname, len);
      if (0 == hostnameLonger) {
        free(hostname);
        cta::exception::Errnum e(ENOMEM);
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
          cta::exception::Errnum e(errno);
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
int cta::System::porttoi(char* str)
   {
  char* dp = str;
  errno = 0;
  int iport = strtoul(str, &dp, 0);
  if (*dp != 0) {
    cta::exception::Errnum e(errno);
    e.getMessage() << "Bad port value." << std::endl;
    throw e;
  }
  if ((iport > 65535) || (iport < 0)) {
    cta::exception::Errnum e(errno);
    e.getMessage()
      << "Invalid port value : " << iport
      << ". Must be < 65535 and > 0." << std::endl;
    throw e;
  }
  return iport;
}

//------------------------------------------------------------------------------
// switchToCastorSuperuser
//------------------------------------------------------------------------------
void cta::System::switchToCtaSuperuser()
   {
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
  if ((stage_passwd = getpwnam(STAGERSUPERUSER)) == NULL) {
    cta::exception::Exception e;
    e.getMessage() << "CTA super user " << STAGERSUPERUSER
                   << " not found in password file";
    throw e;
  }
  // verify existence of its primary group id
  if (getgrgid(stage_passwd->pw_gid) == NULL) {
    cta::exception::Exception e;
    e.getMessage() << "CTA super user group does not exist";
    throw e;
  }
  // Get information on generic stage account from group file
  if ((stage_group = getgrnam(STAGERSUPERGROUP)) == NULL) {
    cta::exception::Exception e;
    e.getMessage() << "CTA super user group " << STAGERSUPERGROUP
                   << " not found in group file";
    throw e;
  }
  // Verify consistency
  if (stage_group->gr_gid != stage_passwd->pw_gid) {
    cta::exception::Exception e;
    e.getMessage() << "Inconsistent password file. The group of the "
                   << "CTA superuser " << STAGERSUPERUSER
                   << " should be " << stage_group->gr_gid
                   << "(" << STAGERSUPERGROUP << "), but is "
                   << stage_passwd->pw_gid;
    throw e;
  }
  // Undo group privilege
  if (setregid (egid, rgid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Unable to undo group privilege";
    throw e;
  }
  // Undo user privilege
  if (setreuid (euid, ruid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Unable to undo user privilege";
    throw e;
  }
  // set the effective privileges to superuser
  if (setegid(stage_passwd->pw_gid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Unable to set group privileges of CTA Superuser. "
                   << "You may want to check that the suid bit is set properly";
    throw e;
  }
  if (seteuid(stage_passwd->pw_uid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Unable to set privileges of CTA Superuser.";
    throw e;
  }
}
