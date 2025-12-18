/*
 * SPDX-FileCopyrightText: 1991 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "getconfent.hpp"

#include "Cglobals.hpp"
#include "serrno.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define strtok(X, Y) strtok_r(X, Y, &last)

static char* getconfent_r(const char* category, const char* name, int flags, char* buffer, int bufsiz) {
  FILE* fp;
  char *p, *cp;
  int found = 0;
  char* last = nullptr;
  constexpr const char* path_config = "/etc/cta/cta-rmcd.conf";

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

char* getconfent_fromfile(const char* category, const char* name, int flags) {
  char* value = nullptr;

  Cglobals_get(&value_key, (void**) &value, BUFSIZ + 1);
  if (value == nullptr) {
    return nullptr;
  }

  return getconfent_r(category, name, flags, value, BUFSIZ + 1);
}
