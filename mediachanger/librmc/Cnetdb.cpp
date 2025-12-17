/*
 * SPDX-FileCopyrightText: 1990 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
