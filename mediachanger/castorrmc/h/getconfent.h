/*
 * $Id: getconfent.h,v 1.1 2005/04/18 10:59:02 jdurand Exp $
 */

#ifndef __getconfent_h
#define __getconfent_h

#include "osdep.h"

EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));
EXTERN_C char DLL_DECL *getconfent_fromfile _PROTO((char *, char *, char *, int));


#endif /* __getconfent_h */
