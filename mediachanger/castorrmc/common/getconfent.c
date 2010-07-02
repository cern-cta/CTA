/*
 * Copyright (C) 1991-1999 by CERN/IT/PDP/DM
 * All rights reserved
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

#if (defined(_REENTRANT) || defined(_THREAD_SAFE))
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */


static char *getconfent_r _PROTO((const char *, const char *, const char *, int, char *, int));

static char *getconfent_r(filename,category, name, flags, buffer, bufsiz)
     const char *filename;
     const char *category;
     const char *name;
     int flags;
     char *buffer;
     int bufsiz;
{
    FILE    *fp;
    char    *p, *cp;
    char    *getenv();
    int     found = 0;
    char    path_config[CA_MAXPATHLEN+1];
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

    strncpy (path_config, filename, CA_MAXPATHLEN);
    /* Who knows */
    path_config[CA_MAXPATHLEN] = '\0';

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
     const char *category;
     const char *name;
     int flags;
{
    char *value = NULL;

    Cglobals_get(&value_key,(void **) &value,BUFSIZ+1);
    if ( value == NULL ) {
        return(NULL);
    }

    return(getconfent_r(NULL,category,name,flags,value,BUFSIZ+1));
}

char *getconfent_fromfile(filename,category, name, flags)
     const char *filename;
     const char *category;
     const char *name;
     int flags;
{
    char *value = NULL;

    Cglobals_get(&value_key,(void **) &value,BUFSIZ+1);
    if ( value == NULL ) {
        return(NULL);
    }

    return(getconfent_r(filename,category,name,flags,value,BUFSIZ+1));
}

int getconfent_parser(conf_val, result, count)
     char **conf_val;
     char ***result;
     int *count;
{
  char *p,*q,*last;
  int i=0;

  /* Counting the number of strings for the array */
  if ((p = strdup(*conf_val)) == NULL) { return -1; }
  for (q = strtok(p," \t"); q != NULL; q = strtok(NULL," \t")) i++;
  free(p);

  /* Saving the index information to pass on later */
  *count = i;
  
  /* Allocating the necessary space and parsing the string */
  if ((p = strdup(*conf_val)) == NULL) { return -1; }
  (*result) = (char **)calloc((i+1), sizeof(char *));
  if (result == NULL) { return -1; }
 
  i = 0 ;
  for (q = strtok(p," \t");q != NULL; q = strtok(NULL," \t")) { (*result)[i++] = strdup(q); }
  free(p);

  return 0;
}

int getconfent_multi_fromfile(filename, category, name, flags, result, count)
     const char *filename;
     const char *category;
     const char *name;
     int flags;
     char ***result;
     int *count;
{
  char *conf_val;

  if((conf_val = getconfent_fromfile(filename,category,name,flags)) == NULL){ 
    *result = NULL;
    *count = 0;
    return 0; 
  }
 
  if ( getconfent_parser(&conf_val, result, count) == -1 ) {return -1;}

  return 0;
}



int getconfent_multi(category, name, flags, result, count)
     const char *category;
     const char *name;
     int flags;
     char ***result;
     int *count;
{
  char *conf_val;
  
  if((conf_val = getconfent(category,name,flags)) == NULL) { 
    *result = NULL;
    *count = 0;
    return 0; 
  }
  
  if( getconfent_parser(&conf_val, result, count) == -1 ) {return -1;}

  return 0;
}
