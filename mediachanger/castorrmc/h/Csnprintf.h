/*
 * $Id: Csnprintf.h,v 1.1 2003/11/04 13:02:56 jdurand Exp $
 */

#ifndef __Csnprintf_h
#define __Csnprintf_h

#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include "osdep.h"

EXTERN_C int DLL_DECL Csnprintf _PROTO((char *, size_t, char *, ...));
EXTERN_C int DLL_DECL Cvsnprintf _PROTO((char *, size_t, char *, va_list));

#endif /* __Csnprintf_h */
