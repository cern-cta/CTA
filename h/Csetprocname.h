/*
 * $Id: Csetprocname.h,v 1.1 2001/11/30 10:42:31 jdurand Exp $
 */

#ifndef __CSETPROCNAME_H
#define __CSETPROCNAME_H

#include <osdep.h>

EXTERN_C int DLL_DECL Cinitsetprocname _PROTO((int, char **, char **));
EXTERN_C int DLL_DECL Csetprocname _PROTO((CONST char *, ...));

#endif /* __CSETPROCNAME_H */
