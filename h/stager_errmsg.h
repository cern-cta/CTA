/*
 * $Id*
 */

#ifndef __stager_errmsg_h
#define __stager_errmsg_h

#include "osdep.h"

#ifdef __cplusplus
 extern "C" {
#endif 

   int stager_seterrbuf (char *, int);
   int stager_setoutbuf (char *, int);
   int stager_geterrbuf (char **, int *);
   int stager_getoutbuf (char **, int *);
   int stager_setlog (void (*) (int, char *));
   int stager_getlog (void (**) (int, char *));
   int stager_errmsg (const char *, const char *, ...);
   int stager_outmsg (const char *, const char *, ...);

#ifdef __cplusplus
 }
#endif

#endif /* __stager_errmsg_h */
