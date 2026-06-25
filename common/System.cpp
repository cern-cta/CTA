/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <arpa/inet.h>
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Local includes
#include "System.hpp"
#include "common/exception/Errnum.hpp"

//------------------------------------------------------------------------------
// setUserAndGroup
//------------------------------------------------------------------------------
void cta::System::setUserAndGroup(const std::string& userName, const std::string& groupName) {
  const std::string task =
    std::string("set user name of process to ") + userName + " and group name of process to " + groupName;

  // Save original values
  const uid_t ruid = getuid();
  const uid_t euid = geteuid();
  const gid_t rgid = getgid();
  const gid_t egid = getegid();

  struct passwd* pwd = nullptr;  // password structure pointer
  struct group* grp = nullptr;   // group structure pointer
  struct group grp_buf;
  struct passwd pwd_buf;
  char pw_buffer[1024];
  char gr_buffer[1024];

  // Get information on generic stage account from password file
  if (int getpwnamRet = getpwnam_r(userName.c_str(), &pwd_buf, pw_buffer, sizeof(pw_buffer), &pwd);
      getpwnamRet != 0 || pwd == nullptr) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": User name not found in password file";
    throw e;
  }
  // Verify existence of its primary group id
  if (int getgrgidRet = getgrgid_r(pwd->pw_gid, &grp_buf, gr_buffer, sizeof(gr_buffer), &grp);
      getgrgidRet != 0 || grp == nullptr) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": User does not have a primary group";
    throw e;
  }
  // Get information about group name from group file
  if (int getgrnam = getgrnam_r(groupName.c_str(), &grp_buf, gr_buffer, sizeof(gr_buffer), &grp);
      getgrnam != 0 || grp == nullptr) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Group name not found in group file";
    throw e;
  }
  // Verify consistency
  if (grp->gr_gid != pwd->pw_gid) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Inconsistent password file. The group ID of user " << userName
                   << " should be " << grp->gr_gid << "(" << groupName << "), but is " << pwd->pw_gid;
    throw e;
  }
  // Undo group privilege
  if (setregid(egid, rgid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to undo group privilege";
    throw e;
  }
  // Undo user privilege
  if (setreuid(euid, ruid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to undo user privilege";
    throw e;
  }
  // set the effective privileges
  if (setegid(pwd->pw_gid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to set effective group ID to " << pwd->pw_gid
                   << ". "
                      "You may want to check that the suid bit is set properly";
    throw e;
  }
  if (seteuid(pwd->pw_uid) < 0) {
    cta::exception::Exception e;
    e.getMessage() << "Failed to " << task << ": Unable to set effective user ID to " << pwd->pw_uid;
    throw e;
  }
}
