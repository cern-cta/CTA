/*
 * $Id: socket_timeout.h,v 1.3 1999/10/14 12:06:11 jdurand Exp $
 *
 * $Log: socket_timeout.h,v $
 * Revision 1.3  1999/10/14 12:06:11  jdurand
 * *** empty log message ***
 *
 * Revision 1.2  1999-07-20 10:51:14+02  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Added missing Id and Log CVS's directives
 *
 */

#ifndef __stgtimeout_h
#define __stgtimeout_h

#if _WIN32
#include <windows.h>
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
