/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1991-2025 CERN
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Cglobals.hpp"
#include "serrno.hpp"
#include "Castor_limits.hpp"
#include "getconfent.hpp"

#ifndef PATH_CONFIG
#define PATH_CONFIG "/etc/castor/castor.conf"
#endif /* PATH_CONFIG */

#define strtok(X, Y) strtok_r(X, Y, &last)

static char* getconfent_r(const char*, const char*, const char*, int, char*, int);

static char*
getconfent_r(const char* filename, const char* category, const char* name, int flags, char* buffer, int bufsiz) {
  FILE* fp;
  char *p, *cp;
  int found = 0;
  char path_config[CA_MAXPATHLEN + 1];
  char* last = nullptr;

  if (filename == nullptr) {
    /* Use default config file is not in the parameters */
    filename = PATH_CONFIG;
    /* But give precedence to $PATH_CONFIG environment variable */
    if ((p = getenv("PATH_CONFIG")) != nullptr) {
      filename = p;
    }
  }

  strncpy(path_config, filename, CA_MAXPATHLEN + 1);
  /* Who knows */
  path_config[CA_MAXPATHLEN] = '\0';

  if ((fp = fopen(path_config, "r")) == nullptr) {
    serrno = SENOCONFIG;
    return nullptr;
  }

  for (;;) {
    p = fgets(buffer, bufsiz - 1, fp);
    if (p == nullptr) {
      break;
    }

    if ((cp = strtok(p, " \t")) == nullptr) {
      continue; /* empty line */
    }
    if (*cp == '#') {
      continue; /* comment */
    }
    if (strcmp(cp, category) == 0) { /* A category match */
      if ((cp = strtok(nullptr, " \t")) == nullptr) {
        continue;
      }
      if (*cp == '#') {
        continue; /* comment, invalid */
      }
      if (strcmp(cp, name) == 0) { /* A match */
        if (flags != 0) {
          /* Don't tokenize next arg */
          cp = strtok(nullptr, "#\n");
        } else {
          cp = strtok(nullptr, "#\t \n");
        }
        if (cp == nullptr) {
          continue;
        }
        if (*cp == '#') {
          continue;
        }
        found++;
        break;
      } else {
        continue;
      }
    } else {
      continue;
    }
  }
  if (fclose(fp)) {
    return nullptr;
  }
  if (found == 0) {
    return nullptr;
  } else {
    return cp;
  }
}

static int value_key = -1;

char* getconfent(const char* category, const char* name, int flags) {
  char* value = nullptr;

  Cglobals_get(&value_key, (void**) &value, BUFSIZ + 1);
  if (value == nullptr) {
    return nullptr;
  }

  return getconfent_r(nullptr, category, name, flags, value, BUFSIZ + 1);
}

char* getconfent_fromfile(const char* filename, const char* category, const char* name, int flags) {
  char* value = nullptr;

  Cglobals_get(&value_key, (void**) &value, BUFSIZ + 1);
  if (value == nullptr) {
    return nullptr;
  }

  return getconfent_r(filename, category, name, flags, value, BUFSIZ + 1);
}
