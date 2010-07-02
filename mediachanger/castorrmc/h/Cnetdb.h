/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: Cnetdb.h,v $ $Revision: 1.11 $ $Date: 2007/07/11 14:27:12 $ CERN IT-PDP/DM Olof Barring
 */


#ifndef _CNETDB_H
#define _CNETDB_H

#include <osdep.h>
#include <netdb.h>

EXTERN_C struct hostent *Cgethostbyname (CONST char *);
EXTERN_C struct hostent *Cgethostbyaddr (CONST void *, size_t, int);
EXTERN_C struct servent *Cgetservbyname (CONST char *, CONST char *);

#define CLOSE(x)        ::close(x)        /* Actual close system call     */
#define IOCTL(x,y,z)    ::ioctl(x,y,z)    /* Actual ioctl system call     */

#endif /* _CNETDB_H */
