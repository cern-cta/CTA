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
// setUserAndGroup
//------------------------------------------------------------------------------
void cta::System::setUserAndGroup(const std::string &userName, const std::string &groupName) {
  const std::string task = std::string("set user name of process to ") + userName + " and group name of process to " +
    groupName;
  struct passwd *pwd = nullptr; // password structure pointer
  struct group  *grp = nullptr; // group structure pointer

  // Save original values
  const uid_t ruid = getuid();
  const uid_t euid = geteuid();
  const gid_t rgid = getgid();
  const gid_t egid = getegid();

  // Get information on generic stage account from password file
  if ((pwd = getpwnam(userName.c_str())) == nullptr) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": User name not found in password file";
    throw e;
  }
  // verify existence of its primary group id
  if (getgrgid(pwd->pw_gid) == nullptr) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": User does not have a primary group";
    throw e;
  }
  // Get information about group name from group file
  if ((grp = getgrnam(groupName.c_str())) == nullptr) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Group name not found in group file";
    throw e;
  }
  // Verify consistency
  if (grp->gr_gid != pwd->pw_gid) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Inconsistent password file. The group ID of user " << userName <<
      " should be " << grp->gr_gid << "(" << groupName << "), but is " << pwd->pw_gid;
    throw e;
  }
  // Undo group privilege
  if (setregid (egid, rgid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to undo group privilege";
    throw e;
  }
  // Undo user privilege
  if (setreuid (euid, ruid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to undo user privilege";
    throw e;
  }
  // set the effective privileges
  if (setegid(pwd->pw_gid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to set effective group ID to " << pwd->pw_gid << ". "
      "You may want to check that the suid bit is set properly";
    throw e;
  }
  if (seteuid(pwd->pw_uid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to set effective user ID to " << pwd->pw_uid;
    throw e;
  }
}
