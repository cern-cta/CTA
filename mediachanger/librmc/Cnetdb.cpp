/*
 * SPDX-FileCopyrightText: 1990 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/*
 * Cnetdb.c - CASTOR MT-safe wrappers on netdb routines.
 */

#include <sys/types.h>
#include <stddef.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include "Cglobals.hpp"
#include "Cnetdb.hpp"

struct hostent* Cgethostbyname(const char* name) {
  static int hostent_key = -1;
  static int hostdata_key = -1;
  int rc;
  struct hostent* hp;
  void* result = NULL;
  void* buffer = NULL;
  int bufsize = 1024;
  int h_errnoop = 0;

  Cglobals_get(&hostent_key, &result, sizeof(struct hostent));
  Cglobals_get(&hostdata_key, &buffer, bufsize);

  if (result == NULL || buffer == NULL) {
    h_errno = NO_RECOVERY;
    return (NULL);
  }
  rc = gethostbyname_r(name,
                       reinterpret_cast<hostent*>(result),
                       reinterpret_cast<char*>(buffer),
                       bufsize,
                       &hp,
                       &h_errnoop);
  h_errno = h_errnoop;
  if (rc == -1) {
    hp = NULL;
  }
  return (hp);
}

struct hostent* Cgethostbyaddr(const void* addr, size_t len, int type) {
  static int hostent_key = -1;
  static int hostdata_key = -1;
  int rc;
  struct hostent* hp;
  void* result = NULL;
  void* buffer = NULL;
  int bufsize = 1024;
  int h_errnoop = 0;

  Cglobals_get(&hostent_key, &result, sizeof(struct hostent));
  Cglobals_get(&hostdata_key, &buffer, bufsize);

  if (result == NULL || buffer == NULL) {
    h_errno = NO_RECOVERY;
    return (NULL);
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
    hp = NULL;
  }
  return (hp);
}

struct servent* Cgetservbyname(const char* name, const char* proto) {
  static int servent_key = -1;
  static int servdata_key = -1;
  int rc;
  struct servent* sp;
  void* result = NULL;
  void* buffer = NULL;
  int bufsize = 1024;

  Cglobals_get(&servent_key, &result, sizeof(struct servent));
  Cglobals_get(&servdata_key, &buffer, bufsize);

  if (result == NULL || buffer == NULL) {
    return (NULL);
  }
  rc = getservbyname_r(name, proto, reinterpret_cast<servent*>(result), reinterpret_cast<char*>(buffer), bufsize, &sp);
  if (rc == -1) {
    sp = NULL;
  }
  return (sp);
}
