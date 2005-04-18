/*
 * Copyright (C) 1991-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "@(#)$RCSfile: getconfent.c,v $ $Revision: 1.12 $ $Date: 2005/04/18 10:59:34 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

#include <stdio.h>
#include <string.h>
#include "Cglobals.h"
#include "serrno.h"
#include "getconfent.h"

#ifndef PATH_CONFIG
#if defined(_WIN32)
#define PATH_CONFIG "%SystemRoot%\\system32\\drivers\\etc\\shift.conf"
#else
#define PATH_CONFIG "/etc/shift.conf"
#endif
#endif /* PATH_CONFIG */

#if ((defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32))
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */


static char *getconfent_r _PROTO((char *, char *, char *, int, char *, int));

static char *getconfent_r(filename,category, name, flags, buffer, bufsiz)
     char *filename;
     char *category;
     char *name;
     int flags;
     char *buffer;
     int bufsiz;
{
    FILE    *fp;
    char    *p, *cp;
    char    *getenv();
    int     found = 0;
    char    path_config[256];
    char    *separator;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

    if (filename == NULL) {
      /* Use default config file is not in the parameters */
      filename = PATH_CONFIG;
      /* But give precedence to $PATH_CONFIG environment variable */
      if ((p = getenv("PATH_CONFIG")) != NULL) {
        filename=p;
      }
    }

#if defined(_WIN32)
    if (strncmp (filename, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot"))) {
        sprintf (path_config, "%s\\%s", p, strchr (filename, '\\'));
    } else {
#endif
        strcpy (path_config, filename);
#if defined(_WIN32)
    }
#endif

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

char DLL_DECL *getconfent(category, name, flags)
     char *category;
     char *name;
     int flags;
{
    char *value = NULL;

    Cglobals_get(&value_key,(void **) &value,BUFSIZ+1);
    if ( value == NULL ) {
        return(NULL);
    }

    return(getconfent_r(NULL,category,name,flags,value,BUFSIZ+1));
}

char DLL_DECL *getconfent_fromfile(filename,category, name, flags)
     char *filename;
     char *category;
     char *name;
     int flags;
{
    char *value = NULL;

    Cglobals_get(&value_key,(void **) &value,BUFSIZ+1);
    if ( value == NULL ) {
        return(NULL);
    }

    return(getconfent_r(filename,category,name,flags,value,BUFSIZ+1));
}
