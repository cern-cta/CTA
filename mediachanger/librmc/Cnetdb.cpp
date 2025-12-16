/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1990-2025 CERN
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

/*
 * Cnetdb.c - CASTOR MT-safe wrappers on netdb routines.
 */

#include "Cnetdb.hpp"

#include "Cglobals.hpp"

#include <netdb.h>
#include <netinet/in.h>
#include <stddef.h>
#include <sys/socket.h>
#include <sys/types.h>

struct hostent* Cgethostbyname(const char* name) {
  static int hostent_key = -1;
  static int hostdata_key = -1;
  int rc;
  struct hostent* hp;
  void* result = nullptr;
  void* buffer = nullptr;
  int bufsize = 1024;
  int h_errnoop = 0;

  Cglobals_get(&hostent_key, &result, sizeof(struct hostent));
  Cglobals_get(&hostdata_key, &buffer, bufsize);

  if (result == nullptr || buffer == nullptr) {
    h_errno = NO_RECOVERY;
    return nullptr;
  }
  rc = gethostbyname_r(name,
                       reinterpret_cast<hostent*>(result),
                       reinterpret_cast<char*>(buffer),
                       bufsize,
                       &hp,
                       &h_errnoop);
  h_errno = h_errnoop;
  if (rc == -1) {
    hp = nullptr;
  }
  return hp;
}

struct hostent* Cgethostbyaddr(const void* addr, size_t len, int type) {
  static int hostent_key = -1;
  static int hostdata_key = -1;
  int rc;
  struct hostent* hp;
  void* result = nullptr;
  void* buffer = nullptr;
  int bufsize = 1024;
  int h_errnoop = 0;

  Cglobals_get(&hostent_key, &result, sizeof(struct hostent));
  Cglobals_get(&hostdata_key, &buffer, bufsize);

  if (result == nullptr || buffer == nullptr) {
    h_errno = NO_RECOVERY;
    return nullptr;
  }
  rc = gethostbyaddr_r(addr,
                       len,
                       type,
                       reinterpret_cast<hostent*>(result),
                       reinterpret_cast<char*>(buffer),
                       bufsize,
                       &hp,
                       &h_errnoop);
  h_errno = h_errnoop;
  if (rc == -1) {
    hp = nullptr;
  }
  return hp;
}

struct servent* Cgetservbyname(const char* name, const char* proto) {
  static int servent_key = -1;
  static int servdata_key = -1;
  int rc;
  struct servent* sp;
  void* result = nullptr;
  void* buffer = nullptr;
  int bufsize = 1024;

  Cglobals_get(&servent_key, &result, sizeof(struct servent));
  Cglobals_get(&servdata_key, &buffer, bufsize);

  if (result == nullptr || buffer == nullptr) {
    return nullptr;
  }
  rc = getservbyname_r(name, proto, reinterpret_cast<servent*>(result), reinterpret_cast<char*>(buffer), bufsize, &sp);
  if (rc == -1) {
    sp = nullptr;
  }
  return sp;
}
