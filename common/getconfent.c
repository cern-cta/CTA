/*
 * $Id: getconfent.c,v 1.7 1999/07/27 09:25:30 obarring Exp $
 * $Log: getconfent.c,v $
 * Revision 1.7  1999/07/27 09:25:30  obarring
 * Add cvsID string constant
 *
 * Revision 1.6  1999/07/22 15:29:53  obarring
 * Go back to use strtok_r instead of strpbrk
 *
 * Revision 1.5  1999/07/21 12:42:20  jdurand
 * HP-UX's cc [without options] don't like function prototypes. Changed it
 * to old C style.
 *
 * Revision 1.4  1999/07/20 15:11:39  obarring
 * Add CVS log directive
 *
 */

/*
 * Copyright (C) 1991-1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "$Id: getconfent.c,v 1.7 1999/07/27 09:25:30 obarring Exp $";
#endif /* not lint */

#include <stdio.h>
#include <string.h>
#include <Cglobals.h>
#include <serrno.h>

#ifndef PATH_CONFIG
#if defined(_WIN32)
#define PATH_CONFIG "%SystemRoot%\\system32\\drivers\\etc\\shift.conf"
#else
#define PATH_CONFIG "/etc/shift.conf"
#endif
#endif /* PATH_CONFIG */

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */


char *getconfent_r(category, name, flags, buffer, bufsiz)
     char *category;
     char *name;
     int flags;
     char *buffer;
     int bufsiz;
{
    char    *filename=PATH_CONFIG;
    FILE    *fp;
    char    *p, *cp;
    char    *getenv();
    int     found = 0;
    char    path_config[256];
    char    *separator;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

    if ((p = getenv("PATH_CONFIG")) != NULL) {
        filename=p;
    }
#if defined(_WIN32)
    if (strncmp (filename, "%SystemRoot%\\", 13) == 0 &&   
        (p = getenv ("SystemRoot")))
        sprintf (path_config, "%s\\%s", p, strchr (filename, '\\'));
    else
#endif
        strcpy (path_config, filename);

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

char *getconfent(category, name, flags)
     char *category;
     char *name;
     int flags;
{
    char *value = NULL;

    Cglobals_get(&value_key,(void **) &value,BUFSIZ+1);
    if ( value == NULL ) {
        return(NULL);
    }

    return(getconfent_r(category,name,flags,value,BUFSIZ+1));
}
