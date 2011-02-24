/*
 * $Id: socket_timeout.h,v 1.8 2007/12/07 13:54:32 sponcec3 Exp $
 *
 * $Log: socket_timeout.h,v $
 * Revision 1.8  2007/12/07 13:54:32  sponcec3
 * Fixed warnings
 *
 * Revision 1.7  2007/12/07 13:26:07  sponcec3
 * Fixed warnings
 *
 * Revision 1.6  2002/09/04 08:06:51  jdurand
 * nbytes argument changed from int for ssize_t
 *
 * Revision 1.5  2000/05/31 10:35:17  obarring
 * Add to prototypes
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

#include "osdep_trunk_r21843.h"
#include "net_trunk_r21843.h"
#include <sys/socket.h>

#include <sys/types.h>

EXTERN_C ssize_t  netread_timeout (SOCKET, void *, ssize_t, int);
EXTERN_C ssize_t  netwrite_timeout (SOCKET, void *, ssize_t, int);
EXTERN_C int netconnect_timeout (SOCKET, struct sockaddr *, size_t, int);

#endif /* __stgtimeout_h */
