/*
 * Copyright (C) 1993-1999 by CERN/CN/PDP/DH
 * All rights reserved
 */

#ifndef __stgtimeout_h
#define __stgtimeout_h

#if _WIN32
#include <types.h>
#else
#include <sys/types.h>
#endif

#if defined(__STDC__)

extern ssize_t  netread_timeout(int, void *, size_t, int);
extern ssize_t  netwrite_timeout(int, void *, size_t, int);

#else /* __STDC__ */

extern ssize_t  netread_timeout();
extern ssize_t  netwrite_timeout();

#endif /* __STDC__ */

#endif /* __stgtimeout_h */
