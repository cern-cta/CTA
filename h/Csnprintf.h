/*
 * $Id: Csnprintf.h,v 1.2 2007/12/07 11:40:53 sponcec3 Exp $
 */

#ifndef __Csnprintf_h
#define __Csnprintf_h

#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include "osdep.h"

EXTERN_C int Csnprintf (char *, size_t, const char *, ...);
EXTERN_C int Cvsnprintf (char *, size_t, const char *, va_list);

#endif /* __Csnprintf_h */
