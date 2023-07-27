/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1991-2023 CERN
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
#include "Cglobals.h"
#include "serrno.h"
#include "getconfent.h"
#include "Csnprintf.h"
#include "Castor_limits.h"

#ifndef PATH_CONFIG
#define PATH_CONFIG "/etc/castor/castor.conf"
#endif /* PATH_CONFIG */

#define strtok(X,Y) strtok_r(X,Y,&last)


static char *getconfent_r (const char *, const char *, const char *, int, char *, int);

static char *getconfent_r(const char *filename,
                          const char *category,
                          const char *name,
                          int flags,
                          char *buffer,
                          int bufsiz)
{
    FILE    *fp;
    char    *p, *cp;
    char    *getenv();
    int     found = 0;
    char    path_config[CA_MAXPATHLEN+1];
    char    *separator;
    char *last = NULL;

    if (filename == NULL) {
      /* Use default config file is not in the parameters */
      filename = PATH_CONFIG;
      /* But give precedence to $PATH_CONFIG environment variable */
      if ((p = getenv("PATH_CONFIG")) != NULL) {
        filename=p;
      }
    }

    strncpy(path_config, filename, CA_MAXPATHLEN+1);
    /* Who knows */
    path_config[CA_MAXPATHLEN+1] = '\0';

    if ((fp = fopen(path_config,"r")) == NULL) {
        serrno = SENOCONFIG;
        return (NULL);
    }

    for (;;) {
        p = fgets(buffer, bufsiz-1, fp);
        if (p == NULL) {
            break;
        }

        if ( (cp = strtok(p," \t")) == NULL) continue; /* empty line */
        if (*cp == '#') continue; /* comment */
        if (strcmp(cp,category) == 0) { /* A category match */
            if ( (cp = strtok(NULL," \t")) == NULL ) continue;
            if (*cp == '#') continue; /* comment, invalid */
            if ( strcmp(cp,name) == 0) { /* A match */
                if (flags != 0) {
                    /* Don't tokenize next arg */
                    separator = "#\n";
                } else {
                    separator = "#\t \n";
                }

                if ( (cp = strtok(NULL, separator)) == NULL ) continue;
                if (*cp == '#') continue;
                found++;
                break;
            }
            else {
                continue;
            }
        } else {
            continue;
        }
    }
    if (fclose(fp)) return(NULL);
    if (found == 0) return(NULL);
    else return(cp);
}

static int value_key = -1;

char *getconfent(const char *category,
                 const char *name,
                 int flags)
{
    char *value = NULL;

    Cglobals_get(&value_key,(void **) &value,BUFSIZ+1);
    if ( value == NULL ) {
        return(NULL);
    }

    return(getconfent_r(NULL,category,name,flags,value,BUFSIZ+1));
}

char *getconfent_fromfile(const char *filename,
                          const char *category,
                          const char *name,
                          int flags)
{
    char *value = NULL;

    Cglobals_get(&value_key,(void **) &value,BUFSIZ+1);
    if ( value == NULL ) {
        return(NULL);
    }

    return(getconfent_r(filename,category,name,flags,value,BUFSIZ+1));
}

