/*
 * $Id: socket_timeout.h,v 1.5 2000/05/31 10:35:17 obarring Exp $
 *
 * $Log: socket_timeout.h,v $
 * Revision 1.5  2000/05/31 10:35:17  obarring
 * Add DLL_DECL to prototypes
 *
 * Revision 1.4  1999/10/20 19:11:53  jdurand
 * Introduced a typdef size_t ssize_t so that Windows is happy
 *
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

#include <osdep.h>
#include <net.h>

#if _WIN32
#include <windows.h>
typedef size_t ssize_t;
#else
#include <sys/types.h>
#endif

EXTERN_C ssize_t DLL_DECL  netread_timeout _PROTO((SOCKET, void *, size_t, int));
EXTERN_C ssize_t DLL_DECL  netwrite_timeout _PROTO((SOCKET, void *, size_t, int));

#endif /* __stgtimeout_h */
