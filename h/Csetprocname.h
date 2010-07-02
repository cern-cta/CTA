/*
 * $Id: Csetprocname.h,v 1.2 2003/11/04 13:20:25 jdurand Exp $
 */

#ifndef __CSETPROCNAME_H
#define __CSETPROCNAME_H

#include <osdep.h>

EXTERN_C int Cinitsetprocname _PROTO((int, char **, char **));
EXTERN_C int Csetprocname _PROTO((char *, ...));

#endif /* __CSETPROCNAME_H */
