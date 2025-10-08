/*
 * SPDX-FileCopyrightText: 1991 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  char* last = NULL;

  if (filename == NULL) {
    /* Use default config file is not in the parameters */
    filename = PATH_CONFIG;
    /* But give precedence to $PATH_CONFIG environment variable */
    if ((p = getenv("PATH_CONFIG")) != NULL) {
      filename = p;
    }
  }

  strncpy(path_config, filename, CA_MAXPATHLEN + 1);
  /* Who knows */
  path_config[CA_MAXPATHLEN] = '\0';

  if ((fp = fopen(path_config, "r")) == NULL) {
    serrno = SENOCONFIG;
    return (NULL);
  }

  for (;;) {
    p = fgets(buffer, bufsiz - 1, fp);
    if (p == NULL) {
      break;
    }

    if ((cp = strtok(p, " \t")) == NULL) {
      continue; /* empty line */
    }
    if (*cp == '#') {
      continue; /* comment */
    }
    if (strcmp(cp, category) == 0) { /* A category match */
      if ((cp = strtok(NULL, " \t")) == NULL) {
        continue;
      }
      if (*cp == '#') {
        continue; /* comment, invalid */
      }
      if (strcmp(cp, name) == 0) { /* A match */
        if (flags != 0) {
          /* Don't tokenize next arg */
          cp = strtok(NULL, "#\n");
        } else {
          cp = strtok(NULL, "#\t \n");
        }
        if (cp == NULL) {
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
    return (NULL);
  }
  if (found == 0) {
    return (NULL);
  } else {
    return (cp);
  }
}

static int value_key = -1;

char* getconfent(const char* category, const char* name, int flags) {
  char* value = NULL;

  Cglobals_get(&value_key, (void**) &value, BUFSIZ + 1);
  if (value == NULL) {
    return (NULL);
  }

  return (getconfent_r(NULL, category, name, flags, value, BUFSIZ + 1));
}

char* getconfent_fromfile(const char* filename, const char* category, const char* name, int flags) {
  char* value = NULL;

  Cglobals_get(&value_key, (void**) &value, BUFSIZ + 1);
  if (value == NULL) {
    return (NULL);
  }

  return (getconfent_r(filename, category, name, flags, value, BUFSIZ + 1));
}
