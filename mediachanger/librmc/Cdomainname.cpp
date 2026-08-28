/**
 * SPDX-FileCopyrightText: 2002 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Cdomainname.hpp"

#include "Cnetdb.hpp"
#include "serrno.hpp"

#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

int Cdomainname(char* name, int namelen) {
  char hostname[CA_MAXHOSTNAMELEN + 1];
  struct hostent* hp;
  char* p;

  if (name == nullptr || namelen <= 0) {
    serrno = EINVAL;
    return -1;
  }

  // namelen includes the terminator; reject oversized names before writing anything.
  const auto copyDomain = [name, namelen](const char* domain) {
    const size_t length = strlen(domain);
    if (length >= static_cast<size_t>(namelen)) {
      serrno = EINVAL;
      return -1;
    }
    memcpy(name, domain, length + 1);
    return 0;
  };

  /*
	 * try looking in /etc/resolv.conf
	 * putting this here and assuming that it is correct, eliminates
	 * calls to gethostbyname, and therefore DNS lookups. This helps
	 * those on dialup systems.
	 */
  if (FILE* fd; (fd = fopen("/etc/resolv.conf", "r")) != nullptr) {
    char line[300];
    while (fgets(line, sizeof(line), fd) != nullptr) {
      if ((strncmp(line, "domain", 6) == 0 || strncmp(line, "search", 6) == 0) && line[6] == ' ') {
        fclose(fd);
        p = line + 6;
        while (*p == ' ') {
          p++;
        }
        if (*p != '\0') {
          *(p + strlen(p) - 1) = '\0';
        }
        return copyDomain(p);
      }
    }
    fclose(fd);
  }

  /* Try gethostname */

  gethostname(hostname, CA_MAXHOSTNAMELEN + 1);

  if ((hp = Cgethostbyname(hostname)) != nullptr) {
    struct in_addr* haddrarray;
    struct in_addr** haddrlist;
    struct hostent* hp2;
    int i;
    int naddr = 0;

    /* need to save list (at least on Digital Unix) */

    haddrlist = (struct in_addr**) hp->h_addr_list;
    while (*haddrlist) {
      naddr++;
      haddrlist++;
    }
    if ((haddrarray = (struct in_addr*) malloc(naddr * sizeof(struct in_addr))) == nullptr) {
      serrno = ENOMEM;
      return -1;
    }
    haddrlist = (struct in_addr**) hp->h_addr_list;
    for (i = 0; i < naddr; i++) {
      memcpy(haddrarray + i, *haddrlist, sizeof(struct in_addr));
      haddrlist++;
    }

    for (i = 0; i < naddr; i++) {
      if ((hp2 = Cgethostbyaddr(haddrarray + i, sizeof(struct in_addr), AF_INET)) != nullptr) {
        char** hal;
        if ((p = strchr(hp2->h_name, '.')) != nullptr) {
          free(haddrarray);
          p++;
          return copyDomain(p);
        }

        /* Look for aliases */

        hal = hp2->h_aliases;
        while (*hal) {
          if ((p = strchr(*hal, '.')) != nullptr) {
            free(haddrarray);
            p++;
            return copyDomain(p);
          }
          hal++;
        }
      }
    }
    free(haddrarray);
  }
  serrno = SEINTERNAL;
  return -1;
}
